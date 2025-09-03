import os, sys, time, json, base64, argparse, zipfile, csv
from datetime import datetime, timedelta
from pathlib import Path
import requests

ENV_BASIC   = os.getenv("RELIABLE_BASIC_AUTH", "").strip()
ENV_API_KEY = os.getenv("RELIABLE_API_KEY", "").strip()
ENV_COUNTRY = os.getenv("RELIABLE_COUNTRY", "US").strip()
ENV_BASE_STG  = os.getenv("RELIABLE_BASE_URL_STG","https://stgapi.reliableparts.net:8077/ws/rest/ReliablePartsBoomiAPI").rstrip("/")
ENV_BASE_PROD = os.getenv("RELIABLE_BASE_URL_PROD","").rstrip("/")

def now_ts(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
def yyyymmdd(dt=None): return (dt or datetime.now()).strftime("%Y%m%d")
def ensure_dir(p: Path): p.mkdir(parents=True, exist_ok=True)
def write_bytes(path: Path, data: bytes):
    ensure_dir(path.parent); open(path, "wb").write(data)
def write_text(path: Path, text: str):
    ensure_dir(path.parent); open(path, "w", encoding="utf-8").write(text)
def write_json(path: Path, obj):
    write_text(path, json.dumps(obj, indent=2, ensure_ascii=False))

def api_headers():
    return {
        "Authorization": ENV_BASIC,
        "x-api-key": ENV_API_KEY,
        "country": ENV_COUNTRY or "US",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def post_json(url: str, payload=None, timeout=60):
    try:
        r = requests.post(url, headers=api_headers(), json=(payload or {}), timeout=timeout)
        ctype = r.headers.get("Content-Type","").lower()
        js = r.json() if "application/json" in ctype else None
        return r.status_code, r.text, js
    except Exception as e:
        return -1, str(e), None

def unzip_all(zip_path: Path, out_dir: Path):
    ensure_dir(out_dir)
    names = []
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(out_dir)
        names = z.namelist()
    return [out_dir / n for n in names]

def load_csv_as_map(csv_path: Path, key_col: str):
    with open(csv_path, newline="", encoding="utf-8", errors="ignore") as f:
        rdr = csv.DictReader(f)
        if key_col not in rdr.fieldnames:
            raise SystemExit(f"[ERROR] Key column '{key_col}' not found in {csv_path}")
        rows = {}
        for row in rdr:
            k = (row.get(key_col) or "").strip()
            if k and k not in rows:
                rows[k] = row
        return rows, rdr.fieldnames

def csv_diff(old_csv: Path, new_csv: Path, out_prefix: Path, key_col: str, fields_to_compare=None):
    old_map, old_cols = load_csv_as_map(old_csv, key_col)
    new_map, new_cols = load_csv_as_map(new_csv, key_col)
    new_keys = set(new_map)-set(old_map); rem_keys = set(old_map)-set(new_map)
    shared = set(new_map) & set(old_map)
    if fields_to_compare:
        cmp_fields = [c for c in fields_to_compare if c in old_cols and c in new_cols]
    else:
        cmp_fields = [c for c in (set(old_cols)&set(new_cols)) if c != key_col]

    changed_rows = []
    for k in shared:
        diffs = {}; diff_any = False
        for c in cmp_fields:
            a = (old_map[k].get(c) or "").strip()
            b = (new_map[k].get(c) or "").strip()
            if a != b:
                diffs[c+"_old"] = a; diffs[c+"_new"] = b; diff_any = True
        if diff_any:
            diffs[key_col] = k; changed_rows.append(diffs)

    out_prefix.parent.mkdir(parents=True, exist_ok=True)
    new_path      = out_prefix.with_name(out_prefix.name + "new.csv")
    removed_path  = out_prefix.with_name(out_prefix.name + "removed.csv")
    changed_path  = out_prefix.with_name(out_prefix.name + "changed.csv")

    with open(new_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow([key_col]); [w.writerow([k]) for k in sorted(new_keys)]
    with open(removed_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow([key_col]); [w.writerow([k]) for k in sorted(rem_keys)]
    with open(changed_path, "w", newline="", encoding="utf-8") as f:
        if changed_rows:
            fields = [key_col] + [x for pair in ((c+"_old",c+"_new") for c in cmp_fields) for x in pair]
            w = csv.DictWriter(f, fieldnames=fields); w.writeheader()
            for r in changed_rows: row = {k:"" for k in fields}; row.update(r); w.writerow(row)
        else:
            csv.writer(f).writerow([key_col])

    return {"new":len(new_keys),"removed":len(rem_keys),"changed":len(changed_rows),
            "compare_fields":cmp_fields,
            "outputs":{"new":str(new_path),"removed":str(removed_path),"changed":str(changed_path)}}

def resolve_base_url(env: str) -> str:
    env = (env or "stg").lower()
    if env == "prod":
        if not ENV_BASE_PROD:
            raise SystemExit("[ERROR] PROD base URL not set (RELIABLE_BASE_URL_PROD).")
        return ENV_BASE_PROD
    return ENV_BASE_STG

def endpoints(base_url: str):
    base = base_url.rstrip("/") + "/partInventoryAndPriceFile/v1"
    return {"create": base + "/create", "download": base + "/download"}

def require_creds():
    missing = []
    if not ENV_BASIC:   missing.append("RELIABLE_BASIC_AUTH")
    if not ENV_API_KEY: missing.append("RELIABLE_API_KEY")
    if missing: raise SystemExit(f"[ERROR] Missing credentials: {', '.join(missing)}")

def try_download(url: str, gen_date: str, timeout=90):
    code, text, js = post_json(url, {"generatedDate": gen_date}, timeout=timeout)
    if code == 200 and isinstance(js, dict) and str(js.get("errorCode")) == "100" and js.get("fileContents"):
        return True, js
    return False, js if js is not None else {"http_status": code, "body": text}

def main():
    ap = argparse.ArgumentParser(description="Reliable Parts create→download→unzip (+optional diff)")
    ap.add_argument("--env", choices=["stg","prod"], default="stg")
    ap.add_argument("--base-url", default=None)
    ap.add_argument("--create", action="store_true")
    ap.add_argument("--date", default=None, help="YYYYMMDD (default: today; may try yesterday early a.m.)")
    ap.add_argument("--poll-mins", type=int, default=10)
    ap.add_argument("--timeout-mins", type=int, default=120)
    ap.add_argument("--outdir", default="parts_runs")
    ap.add_argument("--no-unzip", action="store_true")
    ap.add_argument("--diff-old", default=None)
    ap.add_argument("--key-col", default="partNumber")
    ap.add_argument("--diff-fields", nargs="*", default=None)
    args = ap.parse_args()

    require_creds()
    base_url = args.base_url or resolve_base_url(args.env)
    eps = endpoints(base_url)

    outdir = Path(args.outdir); ensure_dir(outdir)
    metadata = {"started_at":now_ts(),"env":args.env,"base_url":base_url,"create_called":bool(args.create),
                "target_date":args.date or "auto","poll_interval_minutes":args.poll_mins,"timeout_minutes":args.timeout_mins,
                "outputs":{},"result":"UNKNOWN","notes":[]}

    if args.create:
        print(f"[{now_ts()}] CREATE → {eps['create']}")
        code, text, js = post_json(eps["create"], None, timeout=60)
        metadata["create_http_status"] = code
        if code != 200:
            metadata["result"]="CREATE_HTTP_ERROR"
            write_json(outdir / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", metadata)
            sys.exit(f"[ERROR] CREATE failed: HTTP {code}\n{text}")
        print("[OK] CREATE accepted. Generation will take a while.")

    gen_date = args.date or yyyymmdd()
    tried_yesterday = False
    start_time = time.time(); attempts = 0; success = False; last_payload = None

    def time_left(): return args.timeout_mins*60 - (time.time()-start_time)

    while True:
        attempts += 1
        print(f"[{now_ts()}] DOWNLOAD attempt #{attempts} for date {gen_date} …")
        ok, js = try_download(eps["download"], gen_date, timeout=90)
        last_payload = js
        if ok: success = True; break

        ec = str(js.get("errorCode")) if isinstance(js, dict) else "N/A"
        em = js.get("errorMessage") if isinstance(js, dict) else str(js)
        print(f"  → Not ready (errorCode={ec}). {('Message: ' + str(em)) if em else ''}")

        if not args.create: break
        if not args.date and not tried_yesterday and (datetime.now().hour < 6):
            yest = yyyymmdd(datetime.now() - timedelta(days=1))
            print(f"  → Trying yesterday once: {yest}")
            gen_date = yest; tried_yesterday = True; continue

        left = time_left()
        if left <= 0: print("  → Timeout reached."); break
        sleep_s = max(60, args.poll_mins*60)
        print(f"  → Sleeping {sleep_s//60} min (time left ≈ {int(left//60)} min)…")
        time.sleep(sleep_s)

    metadata["download_attempts"]=attempts
    metadata["download_payload_last"]=last_payload
    metadata["generatedDate_final"]=gen_date

    if not success:
        metadata["result"]="DOWNLOAD_NOT_READY"
        write_json(outdir / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", metadata)
        sys.exit("[ERROR] File not ready. Try again later or verify the generatedDate.")

    file_name = last_payload["fileName"]; file_b64 = last_payload["fileContents"]
    zip_bytes = base64.b64decode(file_b64)
    zip_path  = outdir / f"{file_name}"
    write_bytes(zip_path, zip_bytes)
    print(f"[OK] Saved ZIP → {zip_path} ({len(zip_bytes):,} bytes)")
    metadata["outputs"]["zip"] = str(zip_path); metadata["result"]="DOWNLOADED"

    csv_path = None
    if not args.no_unzip:
        extract_dir = outdir / f"unzipped_{gen_date}"
        files = unzip_all(zip_path, extract_dir)
        print(f"[OK] Unzipped {len(files)} file(s) → {extract_dir}")
        for p in files:
            print("   -", p.name)
            if p.suffix.lower() == ".csv": csv_path = p
        metadata["outputs"]["unzipped_dir"]=str(extract_dir)
        metadata["outputs"]["unzipped_files"]=[str(p) for p in files]
        if csv_path: metadata["outputs"]["csv"]=str(csv_path)

    if args.diff_old and csv_path:
        old_csv = Path(args.diff_old)
        if old_csv.exists():
            print(f"[{now_ts()}] DIFF against {old_csv} (key={args.key_col}) …")
            delta_prefix = outdir / f"delta_{gen_date}_"
            diff_info = csv_diff(old_csv, csv_path, delta_prefix, key_col=args.key_col, fields_to_compare=args.diff_fields)
            print(f"   NEW={diff_info['new']}  REMOVED={diff_info['removed']}  CHANGED={diff_info['changed']}")
            metadata["diff"]=diff_info
        else:
            print(f"[WARN] --diff-old not found: {old_csv}")

    metadata["finished_at"]=now_ts()
    write_json(outdir / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", metadata)
    print("[DONE]")

if __name__ == "__main__":
    main()
<<<<<<< HEAD
=======
PY
>>>>>>> 3cd24fe (FTake bash out)
