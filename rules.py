from __future__ import annotations
from datetime import datetime, date, timedelta, time

START_TIME = time(7,30)
LUNCH_WINDOW = (time(10,30), time(11,30))
LUNCH_MINUTES = 30
WORKDAYS = {0,1,2,3,4}
CITY_SPEED_MPH = 22.0
HWY_SPEED_MPH = 38.0
HWY_THRESHOLD_MILES = 18.0

def is_workday(d: date) -> bool:
    return d.weekday() in WORKDAYS

def is_in_phase(d: date, phase: str | None) -> bool:
    if not phase or phase in ("any","weekly"):
        return True
    day = d.day
    if phase == "early": return 1 <= day <= 10
    if phase == "mid":   return 11 <= day <= 20
    if phase == "late":  return day >= 21
    return True

def is_last_thursday(d: date) -> bool:
    first = d.replace(day=1)
    next_month = (first.replace(day=28) + timedelta(days=4)).replace(day=1)
    last_day = next_month - timedelta(days=1)
    offset = (last_day.weekday() - 3) % 7  # Thu=3
    return d == last_day - timedelta(days=offset)
