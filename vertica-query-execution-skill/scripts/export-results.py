#!/usr/bin/env python3
import csv
import json
from datetime import datetime
import os

def export_json(result, out_path):
    payload = {
        "meta": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "source": "vertica",
            "row_count": result.get("row_count"),
            "truncated": result.get("truncated"),
            "columns": result.get("columns")
        },
        "records": result.get("records") or []
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

def export_csv(result, out_path):
    cols = result.get("columns") or []
    rows = result.get("rows") or []
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        for row in rows:
            writer.writerow(["" if v is None else v for v in row])

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="inp", required=True)
    p.add_argument("--format", choices=["json","csv"], required=True)
    p.add_argument("--out", required=True)
    args = p.parse_args()

    with open(args.inp, "r", encoding="utf-8") as f:
        data = json.load(f)

    if args.format == "json":
        export_json(data, args.out)
    else:
        export_csv(data, args.out)
