# engine.py — farthest-first + campus bundling + backhaul + long-haul SD policy
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, date, timedelta, time
from typing import List, Tuple, Dict, Set, Optional
import math
import re
from collections import defaultdict

from models import connect, fetch_office, fetch_tech, list_month_jobs, MonthJob
from rules import (
    START_TIME, LUNCH_WINDOW, LUNCH_MINUTES,
    is_workday, is_in_phase, is_last_thursday,
    CITY_SPEED_MPH, HWY_SPEED_MPH, HWY_THRESHOLD_MILES
)

# ----------------- tunables -----------------
DEFAULT_DURATION_HOURS = 1.5       # hours if missing
MAX_SKIP_NOTES = 25
BACKTRACK_TOL = 0.01               # miles; monotone “closer to office” tolerance
NON_MONO_ALLOW = 2.0               # miles; allow small non-monotone step if needed
LAMBDA_PROGRESS = 0.8              # weight for “closer to office” in scoring
CAMPUS_DIST_MI = 0.90              # distance-only campus radius (pure proximity)

# ----- long-haul policy (e.g., Torrance -> San Diego) -----
LONG_HAUL_THRESHOLD_MI   = 80.0   # first-leg distance from office to count as long-haul
LONG_HAUL_MIN_PMS        = 2      # must schedule at least this many PMs on long-haul days
LONG_HAUL_MAX_PMS        = 3      # soft cap (still bounded by daily cap)
LONG_HAUL_OT_BUFFER_MIN  = 90     # allow up to this much overtime on return for long-haul days
LONG_HAUL_CLUSTER_RADIUS = 20.0   # miles; keep additional long-haul stops near the anchor metro
# --------------------------------------------

def summarize_skips(skip_msgs, max_items: int = MAX_SKIP_NOTES) -> str:
    if not skip_msgs:
        return ""
    by_reason = defaultdict(int)
    for s in skip_msgs:
        i = s.find("("); j = s.rfind(")")
        reason = s[i+1:j].strip() if (i != -1 and j != -1 and j > i) else "other"
        by_reason[reason] += 1
    shown = skip_msgs[:max_items]
    extra = len(skip_msgs) - len(shown)
    parts = []
    if shown: parts.append(" | ".join(shown))
    if by_reason:
        parts.append(" ; ".join(f"{cnt} skipped ({reason})"
                                for reason, cnt in sorted(by_reason.items(), key=lambda x: -x[1])))
    if extra > 0: parts.append(f"... and {extra} more skipped.")
    return " | ".join(p for p in parts if p)

@dataclass
class Placed:
    job_id: int
    property: str
    customer: str
    type: str
    start: datetime
    end: datetime
    drive_min_from_prev: float
    reasoning: str

# ---------- distances ----------
def haversine_mi(lat1, lon1, lat2, lon2) -> float:
    R = 3958.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2*R*math.asin(math.sqrt(a))

def leg_minutes(miles: float) -> float:
    if miles >= HWY_THRESHOLD_MILES:
        return 60.0 * (miles / HWY_SPEED_MPH)
    return 60.0 * (miles / CITY_SPEED_MPH)

def safe_leg_distance(a_lat, a_lon, b_lat, b_lon) -> float:
    if any(v is None for v in (a_lat, a_lon, b_lat, b_lon)):
        return 1e9
    return haversine_mi(a_lat, a_lon, b_lat, b_lon)

def distance_to_office_mi(prop, office) -> float:
    if any(v is None for v in (prop.lat, prop.lon, office.lat, office.lon)):
        return -1.0
    return haversine_mi(prop.lat, prop.lon, office.lat, office.lon)

# ---------- PM detection ----------
def is_pm_like(s: str | None) -> bool:
    if not s:
        return False
    s = " ".join(s.lower().split())
    if any(x in s for x in ["repair", "service call", "emergency"]):
        return False
    pm_signals = [
        "pm","preventive","maintenance","monthly","bi month","bi-month","bimonth",
        "bi monthly","quarter","qtr","semiannual","semi-annual","biannual","annual","weekly"
    ]
    return any(sig in s for sig in pm_signals)

# ---------- calendar ----------
def month_days(yy: int, mm: int) -> List[date]:
    d = date(yy, mm, 1); out = []
    while d.month == mm:
        out.append(d); d += timedelta(days=1)
    return out

def first_n_workdays(yy: int, mm: int, holidays: Set[date], n: int) -> List[date]:
    out = []
    for d in month_days(yy, mm):
        if is_workday(d) and d not in holidays:
            out.append(d)
            if len(out) >= n: break
    return out

# ---------- campus bundling (distance-only) ----------
@dataclass
class Bundle:
    members: List[Tuple[MonthJob, object]]    # list of (mj, prop)
    total_hours: float
    type_label: str
    phase: Optional[str]
    time_window_start: Optional[str]
    time_window_end: Optional[str]
    fixed_date: Optional[str]
    must_be_last_thursday: int
    anchor_prop: object                       # representative stop (farthest from office)
    pm_count: int                             # how many PM-like jobs inside

def _to_minutes(hhmm: Optional[str]) -> Optional[int]:
    if not hhmm: return None
    try:
        h, m = map(int, hhmm.split(":")); return h*60 + m
    except Exception:
        return None

def _to_hhmm(m: Optional[int]) -> Optional[str]:
    if m is None: return None
    return f"{m//60:02d}:{m%60:02d}"

def can_bundle(members: List[Tuple[MonthJob, object]]) -> Optional[Bundle]:
    """Return composite constraints if members can be bundled; else None."""
    if not members:
        return None

    # Sum hours (use defaults when missing), count PMs
    total_h = 0.0
    pm_cnt = 0
    for (mj, _) in members:
        hrs = mj.duration_hours if (mj.duration_hours and mj.duration_hours > 0) else DEFAULT_DURATION_HOURS
        total_h += float(hrs)
        if is_pm_like(mj.type):
            pm_cnt += 1

    # fixed date: all must match (or None)
    fixeds = { (mj.fixed_date or "").strip() for (mj, _) in members }
    fixed_norm = { f for f in fixeds if f != "" }
    if len(fixed_norm) > 1:
        return None
    fixed_val = list(fixed_norm)[0] if fixed_norm else None

    # last Thursday: only bundle if all require it (conservative)
    if any((mj.must_be_last_thursday or 0) not in (0,1) for (mj,_) in members):
        return None
    last_thu_req = 1 if all(int(mj.must_be_last_thursday or 0) == 1 for (mj,_) in members) else 0

    # phase intersection: if all equal non-empty, keep; else None
    phases = { (mj.phase or "").strip().lower() for (mj,_) in members }
    phase_val = list(phases)[0] if len([p for p in phases if p != ""]) == 1 else None

    # windows: intersect [start,end]
    starts = [ _to_minutes(mj.time_window_start) for (mj,_) in members if mj.time_window_start ]
    ends   = [ _to_minutes(mj.time_window_end)   for (mj,_) in members if mj.time_window_end   ]
    start_int = max(starts) if starts else None
    end_int   = min(ends)   if ends   else None
    if start_int is not None and end_int is not None and start_int > end_int:
        return None
    tws = _to_hhmm(start_int)
    twe = _to_hhmm(end_int)

    # placeholder anchor (set in build_bundles)
    anchor = members[0][1]

    return Bundle(
        members=members,
        total_hours=total_h,
        type_label=f"PM Bundle ({len(members)})",
        phase=phase_val,
        time_window_start=tws,
        time_window_end=twe,
        fixed_date=fixed_val,
        must_be_last_thursday=last_thu_req,
        anchor_prop=anchor,
        pm_count=pm_cnt
    )

def build_bundles(jobs: List[Tuple[MonthJob, object]], office) -> List[Tuple[MonthJob, object] | Bundle]:
    """
    Group (mj, prop) into campus bundles using pure proximity:
    any jobs whose properties are connected within CAMPUS_DIST_MI become one bundle.
    Properties without coordinates are left as singles.
    """
    items = jobs[:]
    n = len(items)
    if n <= 1:
        return items

    # Build proximity graph over *all* properties with coords
    adj = [[] for _ in range(n)]
    for i in range(n):
        (mji, pi) = items[i]
        if any(v is None for v in (pi.lat, pi.lon)):
            continue
        for j in range(i+1, n):
            (mjj, pj) = items[j]
            if any(v is None for v in (pj.lat, pj.lon)):
                continue
            if haversine_mi(pi.lat, pi.lon, pj.lat, pj.lon) <= CAMPUS_DIST_MI:
                adj[i].append(j); adj[j].append(i)

    # Connected components => bundles (size>=2) or singles
    seen = [False]*n
    out: List[Tuple[MonthJob, object] | Bundle] = []
    for i in range(n):
        if seen[i]:
            continue
        (mji, pi) = items[i]
        # No coords => single
        if any(v is None for v in (pi.lat, pi.lon)):
            seen[i] = True
            out.append(items[i])
            continue

        # BFS component
        stack = [i]; seen[i] = True; comp_idx = [i]
        while stack:
            v = stack.pop()
            for w in adj[v]:
                if not seen[w]:
                    seen[w] = True
                    stack.append(w)
                    comp_idx.append(w)

        if len(comp_idx) == 1:
            out.append(items[i])
            continue

        members = [items[k] for k in comp_idx]
        b = can_bundle(members)
        if b:
            # choose anchor: farthest from office among members
            def dist_off(p):
                return distance_to_office_mi(p, office)
            far_member = max((t[1] for t in members), key=lambda p: dist_off(p) if dist_off(p) >= 0 else -1.0)
            b.anchor_prop = far_member
            out.append(b)
        else:
            out.extend(members)

    # Sort so farthest bundles/jobs are considered first for anchors
    def farthest_key(x):
        prop = x.anchor_prop if isinstance(x, Bundle) else x[1]
        d = distance_to_office_mi(prop, office)
        return d if d >= 0 else -1.0

    out.sort(key=farthest_key, reverse=True)
    return out

# ---------- long-haul helpers ----------
def is_long_haul(first_prop, office) -> bool:
    """True if first stop is far enough from office to trigger long-haul policy."""
    d = distance_to_office_mi(first_prop, office)
    return (d >= 0) and (d >= LONG_HAUL_THRESHOLD_MI)

def within_cluster(pivot_prop, cand_prop, radius_mi: float) -> bool:
    if any(v is None for v in (pivot_prop.lat, pivot_prop.lon, cand_prop.lat, cand_prop.lon)):
        return False
    return haversine_mi(pivot_prop.lat, pivot_prop.lon, cand_prop.lat, cand_prop.lon) <= radius_mi

# ---------- core ----------
def schedule_month(
    db_path: str,
    month: str,
    region: str,
    tech_name: str,
    pm_cap_default: int | None = None,
    holidays: Set[date] | None = None,
    keep_first_n_workdays_open: int | None = 5,
    pm_cap_overrides: Optional[Dict[date | str, int]] = None,
    **_
) -> Dict[date, List[Placed]]:
    # normalize overrides
    pm_cap_overrides = pm_cap_overrides or {}
    cap_by_day: Dict[date, int] = {}
    for k, v in pm_cap_overrides.items():
        if isinstance(k, date):
            cap_by_day[k] = int(v)
        else:
            try:
                cap_by_day[datetime.strptime(str(k), "%Y-%m-%d").date()] = int(v)
            except Exception:
                pass

    con = connect(db_path)
    office = fetch_office(con, region)
    tech = fetch_tech(con, tech_name)
    if office is None: raise RuntimeError(f"No office found for region {region}")
    if tech is None:   raise RuntimeError(f"No tech found named {tech_name}")

    jobs = list_month_jobs(con, month, region)

    def job_assigned_to(mj: MonthJob, tech_str: str) -> bool:
        if not getattr(mj, "assigned_tech", None): return True
        val = str(mj.assigned_tech).strip().lower()
        return (val == "") or (val == tech_str.strip().lower())

    jobs = [(mj, prop) for (mj, prop) in jobs if job_assigned_to(mj, tech_name)]

    # Build campus bundles (mixed list of (mj,prop) and Bundle)
    mixed = build_bundles(jobs, office)

    by_day: Dict[date, List[Placed]] = {}
    remaining = mixed[:]  # mixed list
    base_cap = pm_cap_default if (pm_cap_default is not None) else tech.max_pms_per_day

    yy, mm = map(int, month.split("-"))
    holidays = holidays or set()
    held_open: Set[date] = set()
    if keep_first_n_workdays_open and keep_first_n_workdays_open > 0:
        held_open = set(first_n_workdays(yy, mm, holidays, keep_first_n_workdays_open))

    for d in month_days(yy, mm):
        if not is_workday(d): continue
        if d in holidays:
            by_day[d] = [Placed(-1,"--","--","NOTE",
                          datetime.combine(d, time(0,0)), datetime.combine(d, time(0,0)),
                          0.0, "Holiday")]
            continue
        if d in held_open:
            by_day[d] = [Placed(-1,"--","--","NOTE",
                          datetime.combine(d, time(0,0)), datetime.combine(d, time(0,0)),
                          0.0, f"First {keep_first_n_workdays_open} working days held open")]
            continue

        pm_cap_for_day = cap_by_day.get(d, base_cap)

        # Eligibility for this date
        pool: List[Tuple[str, object]] = []  # ("bundle"|"single", item)
        for item in remaining:
            if isinstance(item, Bundle):
                b = item
                # day filters
                if b.fixed_date:
                    try: fd = datetime.strptime(b.fixed_date, "%Y-%m-%d").date()
                    except ValueError: fd = None
                    if fd and fd != d: continue
                if b.must_be_last_thursday and not is_last_thursday(d): continue
                if b.phase and (not is_in_phase(d, b.phase)): continue
                pool.append(("bundle", b))
            else:
                mj, prop = item
                if mj.fixed_date:
                    try: fd = datetime.strptime(mj.fixed_date, "%Y-%m-%d").date()
                    except ValueError: fd = None
                    if fd and fd != d: continue
                if mj.must_be_last_thursday and not is_last_thursday(d): continue
                if mj.phase and (not is_in_phase(d, mj.phase)): continue
                pool.append(("single", item))

        if not pool: continue

        # build plan
        day_plan: List[Placed] = []
        placed_prop_ids: Set[int] = set()
        current = datetime.combine(d, START_TIME)
        last_lat, last_lon = office.lat, office.lon
        lunch_taken = False
        prev_office_dist = None
        pm_count = 0

        # ---- Long-haul day controls (computed after first placement)
        long_haul_active = False
        long_haul_anchor_prop = None
        min_pms_required = 0
        max_pms_allowed  = 999999
        latest_return_dt_override = None  # becomes a datetime if long-haul triggers

        # ---------- placers ----------
        def place_single(mj, prop, from_lat, from_lon, note_tag: str, latest_return_dt_override=None) -> Optional[Placed]:
            """Tolerant of synthetic 'bundle arrival' mj (may lack id/duration/windows)."""
            # drive
            if any(v is None for v in (from_lat, from_lon, prop.lat, prop.lon)):
                miles = 0.0; drive_min = 0.0; drive_reason = "no coords → assumed 0 drive"
            else:
                miles = haversine_mi(from_lat, from_lon, prop.lat, prop.lon)
                drive_min = leg_minutes(miles); drive_reason = f"drive={drive_min:.1f} min"
            # earliest start
            first_appt_dt = datetime.combine(d, datetime.strptime(tech.first_appt, "%H:%M").time())
            earliest = max(current, first_appt_dt)
            # time window start
            tws = getattr(mj, "time_window_start", None)
            if tws:
                try:
                    _tws = datetime.combine(d, datetime.strptime(tws, "%H:%M").time())
                    earliest = max(earliest, _tws)
                except Exception:
                    pass
            arrive = earliest + timedelta(minutes=drive_min)
            # lunch
            L0, L1 = LUNCH_WINDOW
            nonlocal lunch_taken
            if not lunch_taken and datetime.combine(d, L0) <= arrive <= datetime.combine(d, L1):
                arrive += timedelta(minutes=LUNCH_MINUTES)
                lunch_taken = True
            # window end
            twe = getattr(mj, "time_window_end", None)
            if twe:
                try:
                    _twe = datetime.combine(d, datetime.strptime(twe, "%H:%M").time())
                    if arrive > _twe: return None
                except Exception:
                    pass
            # duration (default if missing)
            dur_val = getattr(mj, "duration_hours", None)
            duration_hours = (dur_val if (dur_val and dur_val > 0) else DEFAULT_DURATION_HOURS)
            end_service = arrive + timedelta(hours=duration_hours)
            # return bound (allow per-day override for long-haul OT)
            latest_return_dt = datetime.combine(d, datetime.strptime(tech.latest_return, "%H:%M").time())
            if latest_return_dt_override is not None:
                latest_return_dt = latest_return_dt_override
            # back to base bound
            if (prop.lat is None or prop.lon is None or office.lat is None or office.lon is None):
                back_drive = 0.0
            else:
                back_drive = leg_minutes(haversine_mi(prop.lat, prop.lon, office.lat, office.lon))
            if end_service + timedelta(minutes=back_drive) > latest_return_dt: return None

            return Placed(
                job_id=getattr(mj, "id", -1),
                property=prop.name,
                customer=getattr(prop, "customer", ""),
                type=(getattr(mj, "type", None) or "monthly"),
                start=arrive, end=end_service,
                drive_min_from_prev=drive_min,
                reasoning=f"{prop.name}: {note_tag}; {drive_reason}; windows/lunch/return honored."
            )

        def place_bundle(b: Bundle, from_lat, from_lon, note_tag: str, latest_return_dt_override=None) -> Optional[List[Placed]]:
            """Place a whole bundle (members sequentially) with correct first-leg drive from office/prev stop."""
            nonlocal lunch_taken, current
            # Synthetic bundle arrival to set the day's clock correctly
            class _M: pass
            M = _M()
            M.id = -1
            M.time_window_start = b.time_window_start
            M.time_window_end   = b.time_window_end
            M.duration_hours    = b.total_hours
            M.type = b.type_label
            M.fixed_date = b.fixed_date
            M.must_be_last_thursday = b.must_be_last_thursday
            M.phase = b.phase

            anchor = b.anchor_prop
            class _P: pass
            P = _P(); P.name = "[Campus]"; P.customer = getattr(anchor, "customer", "")
            P.lat = anchor.lat; P.lon = anchor.lon

            # Arrival drive to campus anchor (this is the true first-leg drive)
            first = place_single(M, P, from_lat, from_lon, f"{note_tag} (bundle arrival)", latest_return_dt_override)
            if first is None:
                return None

            # Now expand inside-campus sequence: nearest-next among members
            seq: List[Placed] = []
            curr_lat, curr_lon = (anchor.lat, anchor.lon) if (anchor.lat is not None and anchor.lon is not None) else (from_lat, from_lon)
            curr_time = first.start  # arrival time to campus

            inner = b.members[:]
            def d_from_anchor(t):
                pj = t[1]
                return safe_leg_distance(anchor.lat, anchor.lon, pj.lat, pj.lon)
            inner.sort(key=d_from_anchor)

            for idx, (mj, prop) in enumerate(inner):
                saved_current = current
                current = curr_time
                pl = place_single(mj, prop, curr_lat, curr_lon, "in-campus hop", latest_return_dt_override)
                current = saved_current
                if pl is None:
                    return None

                # Ensure the FIRST member shows drive from office/prev stop (not ~0)
                if idx == 0:
                    pl.drive_min_from_prev = round(first.drive_min_from_prev, 1)
                    pl.reasoning = re.sub(
                        r"drive=\s*[\d.]+\s*min",
                        f"drive={pl.drive_min_from_prev:.1f} min",
                        pl.reasoning
                    )

                seq.append(pl)
                # march within campus
                curr_lat, curr_lon = (prop.lat if prop.lat is not None else curr_lat,
                                      prop.lon if prop.lon is not None else curr_lon)
                curr_time = pl.end

            # after finishing the bundle, advance the day clock
            current = seq[-1].end
            return seq

        # choose anchor: farthest feasible first (prefer bundles)
        def item_anchor_prop(x):
            return x.anchor_prop if isinstance(x, Bundle) else x[1]

        def dist_off_prop(p):
            return distance_to_office_mi(p, office)

        placed_something = False
        pool_sorted = sorted(
            pool,
            key=lambda pr: (
                0 if pr[0] == "bundle" else 1,  # bundles before singles
                -(dist_off_prop(item_anchor_prop(pr[1])) if dist_off_prop(item_anchor_prop(pr[1])) >= 0 else -1.0)
            )
        )

        for kind, item in pool_sorted:
            needed_pm = item.pm_count if isinstance(item, Bundle) else (1 if is_pm_like(item[0].type) else 0)
            if pm_count + needed_pm > pm_cap_for_day:
                continue

            if kind == "bundle":
                seq = place_bundle(item, last_lat, last_lon, "farthest-first (bundle)", latest_return_dt_override)
                if seq is None:
                    continue
                for pl in seq:
                    day_plan.append(pl)
                pm_count += item.pm_count
                last_member = item.members[-1][1]
                if last_member.lat is not None and last_member.lon is not None:
                    last_lat, last_lon = last_member.lat, last_member.lon
                remaining.remove(item)
                dop = dist_off_prop(last_member)
                prev_office_dist = dop if (dop is not None and dop >= 0) else prev_office_dist
                placed_something = True

                # ---- Long-haul activation (after first real stop placed: first member in bundle)
                if not long_haul_active:
                    first_real_prop = item.members[0][1] if item.members else item.anchor_prop
                    if first_real_prop is not None and is_long_haul(first_real_prop, office):
                        long_haul_active = True
                        long_haul_anchor_prop = first_real_prop
                        min_pms_required = LONG_HAUL_MIN_PMS
                        max_pms_allowed  = LONG_HAUL_MAX_PMS
                        base_latest_return = datetime.combine(d, datetime.strptime(tech.latest_return, "%H:%M").time())
                        latest_return_dt_override = base_latest_return + timedelta(minutes=LONG_HAUL_OT_BUFFER_MIN)

                break
            else:
                mj, prop = item
                pl = place_single(mj, prop, last_lat, last_lon, "farthest-first (single)", latest_return_dt_override)
                if pl is None:
                    continue
                day_plan.append(pl)
                if is_pm_like(mj.type): pm_count += 1
                current = pl.end
                if prop.lat is not None and prop.lon is not None:
                    last_lat, last_lon = prop.lat, prop.lon
                remaining.remove(item)
                dop = dist_off_prop(prop)
                prev_office_dist = dop if (dop is not None and dop >= 0) else prev_office_dist
                placed_something = True

                # ---- Long-haul activation (after first real stop placed)
                if not long_haul_active:
                    first_real_prop = prop
                    if first_real_prop is not None and is_long_haul(first_real_prop, office):
                        long_haul_active = True
                        long_haul_anchor_prop = first_real_prop
                        min_pms_required = LONG_HAUL_MIN_PMS
                        max_pms_allowed  = LONG_HAUL_MAX_PMS
                        base_latest_return = datetime.combine(d, datetime.strptime(tech.latest_return, "%H:%M").time())
                        latest_return_dt_override = base_latest_return + timedelta(minutes=LONG_HAUL_OT_BUFFER_MIN)

                break

        if not placed_something:
            by_day[d] = [Placed(-1,"--","--","NOTE",
                          datetime.combine(d, time(0,0)), datetime.combine(d, time(0,0)),
                          0.0, "No eligible job fit constraints for first stop.")]
            continue

        # 2) Work back toward office with scoring + tiny non-monotone allowance
        while True:
            pool = []
            for item in remaining:
                if isinstance(item, Bundle):
                    b = item
                    if b.fixed_date:
                        try: fd = datetime.strptime(b.fixed_date, "%Y-%m-%d").date()
                        except ValueError: fd = None
                        if fd and fd != d: continue
                    if b.must_be_last_thursday and not is_last_thursday(d): continue
                    if b.phase and (not is_in_phase(d, b.phase)): continue
                    pool.append(("bundle", b))
                else:
                    mj, prop = item
                    if mj.fixed_date:
                        try: fd = datetime.strptime(mj.fixed_date, "%Y-%m-%d").date()
                        except ValueError: fd = None
                        if fd and fd != d: continue
                    if mj.must_be_last_thursday and not is_last_thursday(d): continue
                    if mj.phase and (not is_in_phase(d, mj.phase)): continue
                    pool.append(("single", item))
            if not pool:
                break

            # If long-haul and we haven't met the minimum, constrain pool to the anchor cluster
            constrained_pool = pool
            if long_haul_active and (pm_count < min_pms_required):
                filtered = []
                for kind, item in pool:
                    prop = item.anchor_prop if isinstance(item, Bundle) else item[1]
                    if within_cluster(long_haul_anchor_prop, prop, LONG_HAUL_CLUSTER_RADIUS):
                        filtered.append((kind, item))
                if filtered:
                    constrained_pool = filtered  # keep SD-local candidates until min PMs satisfied

            closer: List[Tuple[str, object, float, float]] = []
            soft:   List[Tuple[str, object, float, float]] = []

            def score_for(prop):
                drv = safe_leg_distance(last_lat, last_lon, prop.lat, prop.lon)
                drv_min = leg_minutes(drv) if drv < 1e8 else 0.0
                d_off = dist_off_prop(prop)
                prog = (d_off if d_off >= 0 else 0.0)
                return (drv_min + LAMBDA_PROGRESS * prog, d_off)

            for kind, item in constrained_pool:
                need_pm = item.pm_count if isinstance(item, Bundle) else (1 if is_pm_like(item[0].type) else 0)

                # Long-haul soft max (still obeys pm_cap_for_day)
                if long_haul_active and (pm_count + need_pm > max_pms_allowed):
                    continue

                if pm_count + need_pm > pm_cap_for_day:
                    continue

                prop = item.anchor_prop if isinstance(item, Bundle) else item[1]
                sc, d_off = score_for(prop)
                if prev_office_dist is None or d_off < 0:
                    soft.append((kind, item, d_off, sc))
                else:
                    if d_off <= prev_office_dist - BACKTRACK_TOL:
                        closer.append((kind, item, d_off, sc))
                    elif d_off <= prev_office_dist + NON_MONO_ALLOW:
                        soft.append((kind, item, d_off, sc))

            picked = False
            for cand_set, tag in ((sorted(closer, key=lambda t: t[3]), "back-toward-office"),
                                  (sorted(soft,   key=lambda t: t[3]), "soft-backhaul")):
                if picked: break
                for (kind, item, d_off, _sc) in cand_set:
                    if kind == "bundle":
                        seq = place_bundle(item, last_lat, last_lon, tag, latest_return_dt_override)
                        if seq is None:
                            continue
                        for pl in seq:
                            day_plan.append(pl)
                        pm_count += item.pm_count
                        last_member = item.members[-1][1]
                        if last_member.lat is not None and last_member.lon is not None:
                            last_lat, last_lon = last_member.lat, last_member.lon
                        remaining.remove(item)
                        prev_office_dist = d_off if d_off >= 0 else prev_office_dist
                        picked = True
                        break
                    else:
                        mj, prop = item
                        pl = place_single(mj, prop, last_lat, last_lon, tag, latest_return_dt_override)
                        if pl is None:
                            continue
                        day_plan.append(pl)
                        if is_pm_like(mj.type): pm_count += 1
                        current = pl.end
                        if prop.lat is not None and prop.lon is not None:
                            last_lat, last_lon = prop.lat, prop.lon
                        remaining.remove(item)
                        prev_office_dist = d_off if d_off >= 0 else prev_office_dist
                        picked = True
                        break
            if not picked:
                break

        if day_plan:
            by_day[d] = day_plan

    return by_day


















