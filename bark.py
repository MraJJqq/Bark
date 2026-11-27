#!/usr/bin/env python3
"""Single-authority Bark notifier for Vegas scan results."""
import json, os, sys, time, tempfile
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
import requests

CST = timezone(timedelta(hours=8))
API = "https://api.github.com"
STATE_FILE = "state.json"
SCANNER_REPO = os.environ.get("SCANNER_REPO", "MraJJqq/vegas")
SCANNER_TOKEN = os.environ["SCANNER_TOKEN"]
SCANNER_WORKFLOW = os.environ.get("SCANNER_WORKFLOW", "vegas-scan.yml")
SCANNER_BRANCH = os.environ.get("SCANNER_BRANCH", "main")
BARK_KEY = os.environ.get("BARK_KEY") or os.environ.get("BARK_DEVICE_KEY")
if not BARK_KEY:
    raise RuntimeError("BARK_KEY secret is missing (BARK_DEVICE_KEY is also accepted)")
BARK_SERVER = os.environ.get("BARK_SERVER", "https://api.day.app").rstrip("/")
RESCAN_TIMES, WAIT_SECONDS, POLL_SECONDS, KEEP_DAYS = 2, 900, 15, 7
SYMBOLS = [
    "MSFTUSDT","GOOGLUSDT","AMZNUSDT","NVDAUSDT","OPENAIUSDT","AMDUSDT",
    "TSMUSDT","AVGOUSDT","ORCLUSDT","ARMUSDT","MUUSDT","ASMLUSDT",
    "SNDKUSDT","SKHYUSDT","MRVLUSDT","GLWUSDT","HOODUSDT","SPCXUSDT",
    "BTCUSDT","HYPEUSDT","XAUUSDT","CLUSDT",
]
TITLE = "▸▸ 𝐕𝐞𝐠𝐚𝐬 𝐒𝐢𝐠𝐧𝐚𝐥 ◂◂"
NO_SIGNAL, SCAN_FAILED = "𝐍𝐨 𝐒𝐢𝐠𝐧𝐚𝐥", "𝐒𝐜𝐚𝐧 𝐅𝐚𝐢𝐥𝐞𝐝"
SEP = "\u00a0▰\u00a0"
BOLD = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
    "𝐀𝐁𝐂𝐃𝐄𝐅𝐆𝐇𝐈𝐉𝐊𝐋𝐌𝐍𝐎𝐏𝐐𝐑𝐒𝐓𝐔𝐕𝐖𝐗𝐘𝐙𝟎𝟏𝟐𝟑𝟒𝟓𝟔𝟕𝟖𝟗")
HEADERS = {"Authorization": f"Bearer {SCANNER_TOKEN}",
    "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
S = requests.Session()
S.headers.update(HEADERS)

class UncertainDelivery(RuntimeError): pass

def parse_time(value):
    if not isinstance(value, str): raise ValueError("generated_at missing")
    return datetime.strptime(value.replace(" (UTC+8)", ""), "%Y-%m-%d %H:%M:%S").replace(tzinfo=CST)

def load_state():
    if not os.path.exists(STATE_FILE):
        return {"initialized": False, "sent": {}, "uncertain": {},
                "no_signal_slots": [], "failure_slots": [], "uncertain_alerts": {},
                "daily_date": None, "daily_signals": {}}
    try:
        with open(STATE_FILE, encoding="utf-8") as f: x = json.load(f)
        if not isinstance(x, dict): raise ValueError("root is not object")
        for k, default in (("initialized", False), ("sent", {}), ("uncertain", {}),
                           ("no_signal_slots", []), ("failure_slots", []), ("uncertain_alerts", {}),
                           ("daily_date", None), ("daily_signals", {}), ("pending_signals", {}), ("last_pushed_generated_at", None)):
            x.setdefault(k, default)
        if not isinstance(x["sent"], dict) or not isinstance(x["uncertain"], dict):
            raise ValueError("signal state malformed")
        if not isinstance(x["no_signal_slots"], list) or not isinstance(x["failure_slots"], list):
            raise ValueError("slot state malformed")
        if not isinstance(x["uncertain_alerts"], dict):
            raise ValueError("uncertain alert state malformed")
        if x["daily_date"] is not None and not isinstance(x["daily_date"], str):
            raise ValueError("daily date state malformed")
        if not isinstance(x["daily_signals"], dict):
            raise ValueError("daily signal state malformed")
        if not isinstance(x["pending_signals"], dict):
            raise ValueError("pending signal state malformed")
        return x
    except Exception as e:
        raise RuntimeError(f"state.json damaged; refusing reset: {e}") from e

def save_state(x):
    fd, tmp = tempfile.mkstemp(prefix=".state-", dir=".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(x, f, ensure_ascii=False, indent=2); f.flush(); os.fsync(f.fileno())
        os.replace(tmp, STATE_FILE)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)

def prune(x):
    cutoff = int(time.time()) - KEEP_DAYS * 86400
    for field in ("sent", "uncertain"):
        x[field] = {k:v for k,v in x[field].items()
                    if isinstance(v, dict) and isinstance(v.get("ts"), int) and v["ts"] >= cutoff}
    keep = (datetime.now(CST).date() - timedelta(days=KEEP_DAYS)).isoformat()
    for field in ("no_signal_slots", "failure_slots"):
        x[field] = [v for v in x[field] if str(v)[:10] >= keep]
    x["uncertain_alerts"] = {
        k:v for k,v in x["uncertain_alerts"].items()
        if isinstance(v, dict) and isinstance(v.get("ts"), int) and v["ts"] >= cutoff
    }

def gh(url, method="GET", **kwargs):
    r = S.request(method, url, timeout=25, **kwargs)
    r.raise_for_status()
    return r.json() if r.content else {}

def workflow_url():
    return f"{API}/repos/{SCANNER_REPO}/actions/workflows/{quote(SCANNER_WORKFLOW, safe='')}"

def latest_run():
    x = gh(workflow_url()+"/runs", params={"branch":SCANNER_BRANCH,"per_page":1})
    runs = x.get("workflow_runs", [])
    return runs[0] if runs else None

def read_result():
    url = f"{API}/repos/{SCANNER_REPO}/contents/signals.json"
    r = S.get(url, params={"ref":SCANNER_BRANCH},
        headers={**HEADERS,"Accept":"application/vnd.github.raw+json"}, timeout=25)
    r.raise_for_status()
    return r.json()

def validate(x):
    if not isinstance(x, dict): raise ValueError("signals.json is not an object")
    scanned_symbols = x.get("scanned_symbols")
    if (not isinstance(scanned_symbols, list) or len(scanned_symbols) != len(SYMBOLS)
            or len(set(scanned_symbols)) != len(SYMBOLS) or set(scanned_symbols) != set(SYMBOLS)):
        raise ValueError("scanned_symbols does not exactly match the required 22 symbols")
    if x.get("scanned") != len(SYMBOLS) or x.get("succeeded") != len(SYMBOLS):
        raise ValueError(f"scan incomplete: scanned={x.get('scanned')} succeeded={x.get('succeeded')}")
    if x.get("errors") != []: raise ValueError(f"scan errors: {x.get('errors')!r}")
    if not isinstance(x.get("signals"), list): raise ValueError("signals must be a list")
    generated = parse_time(x.get("generated_at"))
    age = (datetime.now(CST)-generated).total_seconds()
    if age < -300 or age > 90*60: raise ValueError(f"result stale/future: {int(age)} seconds")
    signal_date = x.get("signal_date")
    if signal_date != generated.date().isoformat():
        raise ValueError(f"signal_date mismatch: {signal_date!r} vs generated date {generated.date()}")
    today = datetime.now(CST).date()
    if generated.date() != today or signal_date != today.isoformat():
        raise ValueError(f"refusing cross-day Vegas result: generated={generated.date()} today={today}")
    ids, allowed = set(), set(SYMBOLS)
    day_start = int(datetime.combine(generated.date(), datetime.min.time(), tzinfo=CST).timestamp() * 1000)
    day_end = day_start + 24 * 60 * 60 * 1000
    for item in x["signals"]:
        if not isinstance(item, dict): raise ValueError("malformed signal item")
        sid, symbol, stamp = item.get("id"), item.get("symbol"), item.get("ts")
        if not isinstance(sid, str) or not sid.strip() or sid in ids: raise ValueError("missing/duplicate signal ID")
        if symbol not in allowed: raise ValueError(f"unexpected symbol: {symbol}")
        if not isinstance(stamp, int) or not day_start <= stamp < day_end:
            raise ValueError(f"signal timestamp outside signal_date: {sid}")
        ids.add(sid)
    return generated

def rescan(previous_id, previous_generated):
    r = S.post(workflow_url()+"/dispatches", json={"ref":SCANNER_BRANCH,"inputs":{"bark_recovery":"true"}}, timeout=25)
    if r.status_code not in (201,204): raise RuntimeError(f"workflow_dispatch failed: HTTP {r.status_code}")
    deadline = time.monotonic()+WAIT_SECONDS
    run = None
    while time.monotonic() < deadline:
        latest = latest_run()
        if latest and int(latest["id"]) > int(previous_id or 0):
            run = latest; break
        time.sleep(POLL_SECONDS)
    if not run: raise TimeoutError("new Vegas scan run not found within 15 minutes")
    run_id = run["id"]
    while time.monotonic() < deadline:
        run = gh(f"{API}/repos/{SCANNER_REPO}/actions/runs/{run_id}")
        if run.get("status") == "completed":
            if run.get("conclusion") != "success":
                raise RuntimeError(f"Vegas scan run {run_id}: {run.get('conclusion')}")
            result = read_result()
            generated = validate(result)
            if previous_generated is not None and generated <= previous_generated:
                raise RuntimeError("signals.json did not refresh after rescan")
            return result
        time.sleep(POLL_SECONDS)
    raise TimeoutError(f"Vegas scan run {run_id} exceeded 15-minute wait")

def read_event():
    path = os.environ.get("GITHUB_EVENT_PATH")
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as handle:
            event = json.load(handle)
        payload = event.get("client_payload") or {}
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def acquire_result():
    event = read_event()
    event_status = event.get("scanner_job_status")
    event_run_id = event.get("scanner_run_id")
    recovery_event = event.get("scanner_recovery") is True
    original, error = None, None
    last = latest_run()

    try:
        if event_run_id:
            run = gh(f"{API}/repos/{SCANNER_REPO}/actions/runs/{int(event_run_id)}")
            if run.get("status") != "completed" or run.get("conclusion") != "success":
                raise ValueError(
                    f"Vegas event run {event_run_id} not successful: "
                    f"status={run.get('status')} conclusion={run.get('conclusion')}"
                )
            original = read_result()
            generated = validate(original)
            started = datetime.fromisoformat(
                run["created_at"].replace("Z", "+00:00")
            ).astimezone(CST)
            if generated < started - timedelta(minutes=2):
                raise ValueError("signals.json predates the Vegas run named by the event")
            return original

        original = read_result()
        generated = validate(original)
        if event_status == "failure":
            raise ValueError("Vegas workflow reported failure")
        if not last:
            raise ValueError("no Vegas workflow run found")
        if last.get("status") != "completed" or last.get("conclusion") != "success":
            raise ValueError(
                f"latest Vegas run is not successful: "
                f"status={last.get('status')} conclusion={last.get('conclusion')}"
            )
        if last.get("created_at"):
            started = datetime.fromisoformat(
                last["created_at"].replace("Z", "+00:00")
            ).astimezone(CST)
            if generated < started - timedelta(minutes=2):
                raise ValueError("signals.json predates the latest Vegas scan run")
        return original
    except Exception as exc:
        error = exc

    # A recovery-triggered Vegas run must never recursively start another
    # recovery chain. Its failure is reported once and then terminates.
    if recovery_event:
        raise RuntimeError(f"Vegas recovery run failed; automatic recovery stopped: {error}") from error

    previous_generated = None
    if original:
        try:
            previous_generated = parse_time(original.get("generated_at"))
        except Exception:
            pass
    previous_id = last.get("id", 0) if last else 0
    for attempt in range(1, RESCAN_TIMES + 1):
        try:
            print(f"Recovery scan {attempt}/{RESCAN_TIMES}")
            return rescan(previous_id, previous_generated)
        except Exception as exc:
            print(f"Recovery scan {attempt} failed: {exc}", file=sys.stderr)
            latest = latest_run()
            if latest:
                previous_id = latest.get("id", previous_id)
    raise RuntimeError(f"Vegas incomplete after 2 recovery scans; initial error: {error}")

def slot_key(generated):
    candidates = []
    for day_offset in (0,-1,1):
        d = generated.date()+timedelta(days=day_offset)
        for h in (8,10,16,20,22):
            t = datetime(d.year,d.month,d.day,h,5,tzinfo=CST)
            candidates.append(t)
    nearest = min(candidates, key=lambda t: abs((generated-t).total_seconds()))
    if abs((generated-nearest).total_seconds()) <= 2*3600:
        return nearest.strftime("%Y-%m-%d-%H")
    return generated.strftime("%Y-%m-%d-manual-%H%M")

def push(body):
    url = f"{BARK_SERVER}/{quote(BARK_KEY,safe='')}/{quote(TITLE,safe='')}/{quote(body,safe='')}"
    try:
        r = requests.get(url, params={"group":"Vegas Signal","level":"active"}, timeout=(10,25))
    except requests.RequestException as e: raise UncertainDelivery(str(e)) from e
    if r.status_code in (408, 429) or r.status_code >= 500:
        raise UncertainDelivery(f"Bark HTTP {r.status_code}")
    if r.status_code >= 400:
        raise RuntimeError(f"Bark rejected: HTTP {r.status_code} {r.text[:300]}")
    try: payload = r.json()
    except ValueError as e: raise UncertainDelivery("Bark returned a non-JSON response after request") from e
    if not isinstance(payload, dict) or payload.get("code") != 200:
        raise RuntimeError(f"Bark rejected: {payload!r}")
    return True

def failure_notice(state, slot):
    prior = state["uncertain_alerts"].get(slot)
    if slot in state["failure_slots"] or (prior and prior.get("body") == SCAN_FAILED): return
    try: push(SCAN_FAILED)
    except UncertainDelivery as e:
        state["uncertain_alerts"][slot] = {"ts":int(time.time()),"body":SCAN_FAILED}
        save_state(state); raise RuntimeError(f"failure notice delivery uncertain: {e}") from e
    state["failure_slots"].append(slot); save_state(state)

def main():
    state = load_state(); prune(state)
    try:
        result = acquire_result()
        generated = validate(result)
    except Exception as e:
        slot = datetime.now(CST).strftime("%Y-%m-%d-%H")
        print(f"SCAN FAILED: {e}", file=sys.stderr)
        failure_notice(state, slot); save_state(state); raise
    slot = slot_key(generated)
    signals = result["signals"]
    today = datetime.now(CST).date().isoformat()

    # Migrate the previous ID-based state once, preserving today's known signals.
    if state["daily_date"] is None:
        migrated = {}
        for sid, info in state["sent"].items():
            stamp = info.get("ts") if isinstance(info, dict) else None
            if isinstance(stamp, int) and datetime.fromtimestamp(stamp, CST).date().isoformat() == today:
                symbol = sid.rsplit("-", 1)[0]
                if symbol in SYMBOLS:
                    migrated[sid] = symbol
        state["daily_signals"] = migrated
        state["pending_signals"] = {}
        state["daily_date"] = today
    elif state["daily_date"] != today:
        # Beijing midnight starts a new daily set; never carry yesterday forward.
        state["daily_signals"] = {}
        state["pending_signals"] = {}
        state["daily_date"] = today

    if not state["initialized"]:
        # First run must deliver today's signals too; do not absorb them as a silent baseline.
        state["initialized"] = True
        state["baseline_generated_at"] = result["generated_at"]

    previous_ids = set(state["daily_signals"])
    for item in signals:
        state["daily_signals"][item["id"]] = item["symbol"]
    fresh = [item for item in signals if item["id"] not in previous_ids]

    # Deduplicate duplicate Bark invocations for the same Vegas scan, not repeated signal IDs.
    if state.get("last_pushed_generated_at") == result["generated_at"]:
        print(f"Vegas scan already notified: {result['generated_at']}")
        save_state(state)
        return

    if not state["daily_signals"]:
        if slot in state["no_signal_slots"]:
            print(f"No Signal already accepted for slot: {slot}"); save_state(state); return
        uncertain = state["uncertain_alerts"].get(slot)
        if uncertain and uncertain.get("body") == NO_SIGNAL:
            print(f"No-signal delivery already uncertain: {slot}"); save_state(state); return
        try: push(NO_SIGNAL)
        except UncertainDelivery as e:
            state["uncertain_alerts"][slot] = {"ts":int(time.time()),"body":NO_SIGNAL}
            save_state(state); raise RuntimeError(f"No Signal delivery uncertain: {e}") from e
        state["no_signal_slots"].append(slot)
        state["uncertain_alerts"].pop(slot, None)
        state["last_pushed_generated_at"] = result["generated_at"]
        save_state(state)
        print(f"No Signal accepted for {slot}"); return

    # Notify only the current Beijing day's cumulative signal set.
    all_symbols = set(state["daily_signals"].values())
    ordered = [s for s in SYMBOLS if s in all_symbols]
    body = SEP.join(s.removesuffix("USDT").translate(BOLD) for s in ordered)
    uncertain = state["uncertain_alerts"].get(slot)
    if uncertain and uncertain.get("body") == body:
        print(f"Signal delivery already uncertain for slot: {slot}")
        save_state(state)
        return
    try:
        push(body)
    except UncertainDelivery as e:
        state["uncertain_alerts"][slot] = {
            "ts": int(time.time()), "body": body,
            "signal_ids": [item["id"] for item in fresh]
        }
        save_state(state)
        raise RuntimeError(f"delivery uncertain; daily signal set retained and marked uncertain: {e}") from e

    state["last_pushed_generated_at"] = result["generated_at"]
    state["uncertain_alerts"].pop(slot, None)
    state["pending_signals"] = {}
    save_state(state)
    print(f"Bark accepted cumulative set: {len(state['daily_signals'])} daily IDs + pending queue / {len(ordered)} unique symbols")

if __name__ == "__main__": main()
