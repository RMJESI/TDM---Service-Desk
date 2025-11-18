from __future__ import annotations
import csv
from datetime import date
from typing import Dict, List
from engine import Placed

def write_csv(path: str, schedule: Dict[date, List[Placed]]):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(["Date","Property","Type","Start","End","DriveFromPrev(min)","Reasoning"])
        for d, items in sorted(schedule.items()):
            for p in items:
                if p.job_id == -1:
                    continue
                w.writerow([d.isoformat(), p.property, p.type, p.start.strftime('%H:%M'), p.end.strftime('%H:%M'),
                            f"{p.drive_min_from_prev:.1f}", p.reasoning])
