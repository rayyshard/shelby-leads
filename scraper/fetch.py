#!/usr/bin/env python3
"""
Shelby County, TN — Motivated Seller Lead Scraper
==================================================
Pulls recent recordings from the Shelby County Register/Clerk portal,
enriches owner records with property + mailing addresses from the
Shelby County Property Assessor (Melvin Burgess) bulk parcel DBF,
scores each lead, and writes records.json + a GHL-ready CSV.

Production notes
----------------
* Async Playwright drives the clerk portal (JS-heavy ASP.NET site).
* requests + BeautifulSoup handles the static assessor page and the
  __doPostBack file-download form.
* dbfread streams the bulk parcel DBF (no full load into memory).
* All network calls wrapped in retry(3) with exponential backoff.
* Every record parser is wrapped in try/except — bad rows are skipped,
  never fatal.
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import re
import sys
import time
import zipfile
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

try:
    from dbfread import DBF  # type: ignore
except Exception:  # pragma: no cover
    DBF = None  # handled at runtime

try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
except Exception:  # pragma: no cover
    async_playwright = None
    PWTimeout = Exception


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CLERK_PORTAL_URL = "https://www.shelbycountytn.gov/"
CLERK_SEARCH_URL = (
    "https://search.register.shelby.tn.us/search/"  # public search front-door
)
ASSESSOR_URL = "https://www.assessormelvinburgess.com/propertySearch"

LOOKBACK_DAYS = 7

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
DASH_DIR = ROOT / "dashboard"
CACHE_DIR = ROOT / ".cache"
for d in (DATA_DIR, DASH_DIR, CACHE_DIR):
    d.mkdir(parents=True, exist_ok=True)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# Lead type code -> human label
LEAD_TYPES: Dict[str, str] = {
    "LP":       "Lis Pendens",
    "NOFC":     "Notice of Foreclosure",
    "TAXDEED":  "Tax Deed",
    "JUD":      "Judgment",
    "CCJ":      "Certified Judgment",
    "DRJUD":    "Domestic Judgment",
    "LNCORPTX": "Corporate Tax Lien",
    "LNIRS":    "IRS Lien",
    "LNFED":    "Federal Lien",
    "LN":       "Lien",
    "LNMECH":   "Mechanic Lien",
    "LNHOA":    "HOA Lien",
    "MEDLN":    "Medicaid Lien",
    "PRO":      "Probate",
    "NOC":      "Notice of Commencement",
    "RELLP":    "Release of Lis Pendens",
}

# Loose keyword → code mapping for clerk-portal doc-type strings
DOC_TYPE_PATTERNS: List[Tuple[re.Pattern[str], str]] = [
    (re.compile(r"release.*lis\s*pendens", re.I), "RELLP"),
    (re.compile(r"lis\s*pendens",          re.I), "LP"),
    (re.compile(r"notice\s*of\s*forecl",   re.I), "NOFC"),
    (re.compile(r"substitute\s*trustee",   re.I), "NOFC"),
    (re.compile(r"tax\s*deed",             re.I), "TAXDEED"),
    (re.compile(r"certified\s*judg",       re.I), "CCJ"),
    (re.compile(r"domestic\s*judg",        re.I), "DRJUD"),
    (re.compile(r"judg",                   re.I), "JUD"),
    (re.compile(r"corp.*tax.*lien",        re.I), "LNCORPTX"),
    (re.compile(r"irs.*lien",              re.I), "LNIRS"),
    (re.compile(r"federal.*lien",          re.I), "LNFED"),
    (re.compile(r"mechanic.*lien",         re.I), "LNMECH"),
    (re.compile(r"hoa.*lien",              re.I), "LNHOA"),
    (re.compile(r"medicaid.*lien",         re.I), "MEDLN"),
    (re.compile(r"\blien\b",               re.I), "LN"),
    (re.compile(r"probate|estate|admin",   re.I), "PRO"),
    (re.compile(r"notice\s*of\s*commenc",  re.I), "NOC"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def retry(fn, *args, attempts: int = 3, base_delay: float = 1.5, **kwargs):
    """Synchronous retry with exponential backoff."""
    last_exc: Optional[BaseException] = None
    for i in range(attempts):
        try:
            return fn(*args, **kwargs)
        except Exception as e:  # noqa: BLE001
            last_exc = e
            sleep_for = base_delay * (2 ** i)
            print(f"[retry] {fn.__name__} attempt {i+1}/{attempts} failed: {e!r}; "
                  f"sleeping {sleep_for:.1f}s", file=sys.stderr)
            time.sleep(sleep_for)
    raise last_exc  # type: ignore[misc]


async def aretry(coro_fn, *args, attempts: int = 3, base_delay: float = 1.5, **kwargs):
    last_exc: Optional[BaseException] = None
    for i in range(attempts):
        try:
            return await coro_fn(*args, **kwargs)
        except Exception as e:  # noqa: BLE001
            last_exc = e
            sleep_for = base_delay * (2 ** i)
            print(f"[aretry] attempt {i+1}/{attempts} failed: {e!r}; "
                  f"sleeping {sleep_for:.1f}s", file=sys.stderr)
            await asyncio.sleep(sleep_for)
    raise last_exc  # type: ignore[misc]


def safe(d: Dict[str, Any], *keys: str, default: str = "") -> str:
    for k in keys:
        v = d.get(k)
        if v not in (None, "", " "):
            return str(v).strip()
    return default


def parse_money(text: str) -> float:
    if not text:
        return 0.0
    try:
        return float(re.sub(r"[^0-9.\-]", "", str(text)) or 0)
    except Exception:
        return 0.0


def parse_date(text: str) -> Optional[datetime]:
    if not text:
        return None
    text = str(text).strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    return None


def classify_doc_type(label: str) -> Tuple[str, str]:
    """Return (code, human_label) for a clerk-portal doc-type string."""
    for pat, code in DOC_TYPE_PATTERNS:
        if pat.search(label or ""):
            return code, LEAD_TYPES[code]
    return "", ""


def split_owner_name(name: str) -> Tuple[str, str]:
    """Best-effort first/last split for GHL export."""
    if not name:
        return "", ""
    n = re.sub(r"\s+", " ", name).strip().strip(",")
    # "LAST, FIRST MIDDLE"
    if "," in n:
        last, _, rest = n.partition(",")
        return rest.strip(), last.strip()
    parts = n.split(" ")
    if len(parts) == 1:
        return "", parts[0]
    return parts[0], " ".join(parts[1:])


def is_business(name: str) -> bool:
    return bool(re.search(
        r"\b(LLC|L\.L\.C|INC|CORP|CO\b|COMPANY|TRUST|LP\b|LLP|BANK|ESTATE|"
        r"PARTNERS|HOLDINGS|GROUP|ASSOCIATION|ASSN)\b",
        name or "", re.I,
    ))


# ---------------------------------------------------------------------------
# Property Appraiser — bulk DBF download + owner index
# ---------------------------------------------------------------------------

@dataclass
class ParcelRow:
    owner: str = ""
    site_addr: str = ""
    site_city: str = ""
    site_state: str = "TN"
    site_zip: str = ""
    mail_addr: str = ""
    mail_city: str = ""
    mail_state: str = ""
    mail_zip: str = ""


def _assessor_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*"})
    return s


def fetch_parcel_dbf() -> Optional[Path]:
    """
    Hit the assessor 'propertySearch' page, find the bulk-data download
    form (an ASP.NET __doPostBack), and POST to retrieve the DBF/ZIP.
    Cached on disk for 24h.
    """
    cache_file = CACHE_DIR / "parcels.dbf"
    if cache_file.exists() and (time.time() - cache_file.stat().st_mtime) < 86400:
        print(f"[assessor] using cached DBF: {cache_file}")
        return cache_file

    sess = _assessor_session()

    def _get():
        r = sess.get(ASSESSOR_URL, timeout=60)
        r.raise_for_status()
        return r

    try:
        page = retry(_get)
    except Exception as e:
        print(f"[assessor] could not load page: {e}", file=sys.stderr)
        return None

    soup = BeautifulSoup(page.text, "lxml")

    def hidden(name: str) -> str:
        tag = soup.find("input", {"name": name})
        return tag["value"] if tag and tag.has_attr("value") else ""

    form_data = {
        "__VIEWSTATE":          hidden("__VIEWSTATE"),
        "__VIEWSTATEGENERATOR": hidden("__VIEWSTATEGENERATOR"),
        "__EVENTVALIDATION":    hidden("__EVENTVALIDATION"),
        "__EVENTTARGET":        "",
        "__EVENTARGUMENT":      "",
    }

    # Find any postback link that looks like a bulk download
    target = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = (a.get_text() or "").lower()
        if "doPostBack" in href and ("bulk" in text or "dbf" in text or "download" in text):
            m = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href)
            if m:
                target = m.group(1)
                form_data["__EVENTARGUMENT"] = m.group(2)
                break
    if target:
        form_data["__EVENTTARGET"] = target

    def _post():
        r = sess.post(ASSESSOR_URL, data=form_data, timeout=300, stream=True)
        r.raise_for_status()
        return r

    try:
        resp = retry(_post)
    except Exception as e:
        print(f"[assessor] postback failed: {e}", file=sys.stderr)
        return None

    content = resp.content
    # Could be a zip containing the DBF, or the DBF directly
    if content[:2] == b"PK":
        try:
            z = zipfile.ZipFile(io.BytesIO(content))
            dbf_name = next((n for n in z.namelist() if n.lower().endswith(".dbf")), None)
            if not dbf_name:
                print("[assessor] zip had no .dbf inside", file=sys.stderr)
                return None
            with z.open(dbf_name) as src, open(cache_file, "wb") as dst:
                dst.write(src.read())
        except Exception as e:
            print(f"[assessor] zip extract failed: {e}", file=sys.stderr)
            return None
    else:
        cache_file.write_bytes(content)

    return cache_file if cache_file.exists() and cache_file.stat().st_size else None


def build_owner_index(dbf_path: Optional[Path]) -> Dict[str, ParcelRow]:
    """Map normalized owner-name variants → ParcelRow."""
    index: Dict[str, ParcelRow] = {}
    if not dbf_path or not dbf_path.exists():
        print("[index] no DBF available; owner index empty")
        return index
    if DBF is None:
        print("[index] dbfread not installed; owner index empty", file=sys.stderr)
        return index

    try:
        table = DBF(str(dbf_path), load=False, ignore_missing_memofile=True, encoding="latin-1")
    except Exception as e:
        print(f"[index] cannot open DBF: {e}", file=sys.stderr)
        return index

    count = 0
    for rec in table:
        try:
            row = ParcelRow(
                owner=safe(rec, "OWNER", "OWN1", "OWN_NAME"),
                site_addr=safe(rec, "SITE_ADDR", "SITEADDR", "SITUS"),
                site_city=safe(rec, "SITE_CITY", "SITUS_CITY"),
                site_zip=safe(rec, "SITE_ZIP", "SITUS_ZIP"),
                mail_addr=safe(rec, "ADDR_1", "MAILADR1", "MAIL_ADDR"),
                mail_city=safe(rec, "CITY", "MAILCITY", "MAIL_CITY"),
                mail_state=safe(rec, "STATE", "MAILSTATE", "MAIL_STATE", default="TN"),
                mail_zip=safe(rec, "ZIP", "MAILZIP", "MAIL_ZIP"),
            )
            if not row.owner:
                continue
            for key in owner_key_variants(row.owner):
                index.setdefault(key, row)
            count += 1
        except Exception:
            continue
    print(f"[index] indexed {count} parcels, {len(index)} owner-name keys")
    return index


def owner_key_variants(name: str) -> List[str]:
    """Generate FIRST LAST, LAST FIRST, LAST, FIRST normalized keys."""
    if not name:
        return []
    n = re.sub(r"[^A-Z0-9, ]", " ", name.upper())
    n = re.sub(r"\s+", " ", n).strip()
    out = {n}
    if "," in n:
        last, _, first = n.partition(",")
        last, first = last.strip(), first.strip()
        if last and first:
            out.add(f"{first} {last}")
            out.add(f"{last} {first}")
            out.add(f"{last}, {first}")
    else:
        parts = n.split(" ")
        if len(parts) >= 2:
            first, last = parts[0], parts[-1]
            out.add(f"{first} {last}")
            out.add(f"{last} {first}")
            out.add(f"{last}, {first}")
    return list(out)


def lookup_parcel(owner: str, index: Dict[str, ParcelRow]) -> Optional[ParcelRow]:
    if not owner or not index:
        return None
    for key in owner_key_variants(owner):
        hit = index.get(key)
        if hit:
            return hit
    return None


# ---------------------------------------------------------------------------
# Clerk Portal — Playwright async crawler
# ---------------------------------------------------------------------------

async def fetch_clerk_records(lookback_days: int) -> List[Dict[str, Any]]:
    if async_playwright is None:
        print("[clerk] playwright not installed", file=sys.stderr)
        return []

    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    print(f"[clerk] window {start:%m/%d/%Y} – {end:%m/%d/%Y}")

    results: List[Dict[str, Any]] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(user_agent=USER_AGENT)
        page = await ctx.new_page()

        async def _open():
            await page.goto(CLERK_SEARCH_URL, wait_until="domcontentloaded", timeout=60_000)

        try:
            await aretry(_open)
        except Exception as e:
            print(f"[clerk] could not load portal: {e}", file=sys.stderr)
            await browser.close()
            return results

        # Run one search per lead-type code. The clerk portal accepts a
        # doc-type filter and a date range. We try a generic field-fill
        # strategy and skip gracefully if a control isn't present.
        for code, label in LEAD_TYPES.items():
            try:
                await _run_one_search(page, code, label, start, end, results)
            except Exception as e:
                print(f"[clerk] search {code} failed: {e}", file=sys.stderr)
                continue

        await browser.close()

    print(f"[clerk] collected {len(results)} raw records")
    return results


async def _run_one_search(page, code: str, label: str,
                          start: datetime, end: datetime,
                          results: List[Dict[str, Any]]) -> None:
    """One pass of the clerk-portal search form for a given doc-type."""
    # Reset to search page
    try:
        await page.goto(CLERK_SEARCH_URL, wait_until="domcontentloaded", timeout=60_000)
    except PWTimeout:
        return

    # Date inputs (best-effort selectors)
    for sel in ['input[name*="From"]', 'input[id*="From"]', 'input[placeholder*="From"]']:
        try:
            await page.fill(sel, start.strftime("%m/%d/%Y"), timeout=2_000)
            break
        except Exception:
            continue
    for sel in ['input[name*="To"]', 'input[id*="To"]', 'input[placeholder*="To"]']:
        try:
            await page.fill(sel, end.strftime("%m/%d/%Y"), timeout=2_000)
            break
        except Exception:
            continue

    # Document type
    for sel in ['select[name*="DocType"]', 'select[id*="DocType"]', 'select[name*="Type"]']:
        try:
            await page.select_option(sel, label=label, timeout=2_000)
            break
        except Exception:
            try:
                await page.select_option(sel, value=code, timeout=2_000)
                break
            except Exception:
                continue

    # Submit
    for sel in ['input[type="submit"]', 'button[type="submit"]', 'button:has-text("Search")']:
        try:
            await page.click(sel, timeout=2_000)
            break
        except Exception:
            continue

    try:
        await page.wait_for_load_state("networkidle", timeout=30_000)
    except Exception:
        pass

    # Scrape result rows
    try:
        rows = await page.query_selector_all("table tr")
    except Exception:
        rows = []

    for row in rows:
        try:
            cells = await row.query_selector_all("td")
            if len(cells) < 4:
                continue
            texts = [(await c.inner_text()).strip() for c in cells]
            doc_num = next((t for t in texts if re.match(r"^[A-Z0-9-]{6,}$", t)), texts[0])
            filed = next((t for t in texts if re.match(r"\d{1,2}/\d{1,2}/\d{4}", t)), "")
            doc_label = next((t for t in texts if any(p.search(t) for p, _ in DOC_TYPE_PATTERNS)),
                             label)
            grantor = texts[2] if len(texts) > 2 else ""
            grantee = texts[3] if len(texts) > 3 else ""
            link_tag = await row.query_selector("a[href]")
            href = await link_tag.get_attribute("href") if link_tag else ""
            url = href if href and href.startswith("http") else (
                f"{CLERK_SEARCH_URL.rstrip('/')}/{href.lstrip('/')}" if href else CLERK_SEARCH_URL
            )
            results.append({
                "doc_num": doc_num,
                "doc_type_raw": doc_label,
                "filed": filed,
                "grantor": grantor,
                "grantee": grantee,
                "legal": " | ".join(texts[4:6]) if len(texts) > 5 else "",
                "amount_raw": next((t for t in texts if "$" in t), ""),
                "clerk_url": url,
                "_hint_code": code,
            })
        except Exception:
            continue


# ---------------------------------------------------------------------------
# Scoring + flag generation
# ---------------------------------------------------------------------------

def derive_flags(rec: Dict[str, Any], cat: str, has_addr: bool, week_old: bool) -> List[str]:
    flags: List[str] = []
    if cat == "LP":
        flags.append("Lis pendens")
    if cat in ("NOFC", "TAXDEED"):
        flags.append("Pre-foreclosure")
    if cat in ("JUD", "CCJ", "DRJUD"):
        flags.append("Judgment lien")
    if cat in ("LNCORPTX", "LNIRS", "LNFED"):
        flags.append("Tax lien")
    if cat == "LNMECH":
        flags.append("Mechanic lien")
    if cat == "PRO":
        flags.append("Probate / estate")
    if is_business(rec.get("owner", "")):
        flags.append("LLC / corp owner")
    if week_old:
        flags.append("New this week")
    # de-dupe, preserve order
    seen = set()
    return [f for f in flags if not (f in seen or seen.add(f))]


def score_record(rec: Dict[str, Any], cat: str, flags: List[str],
                 amount: float, has_addr: bool, week_old: bool) -> int:
    score = 30
    score += 10 * len(flags)
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20
    if amount > 100_000:
        score += 15
    elif amount > 50_000:
        score += 10
    if week_old:
        score += 5
    if has_addr:
        score += 5
    return max(0, min(100, score))


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def enrich_and_score(raw: List[Dict[str, Any]],
                     owner_index: Dict[str, ParcelRow]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    week_ago = datetime.now() - timedelta(days=7)

    for rec in raw:
        try:
            cat, cat_label = classify_doc_type(rec.get("doc_type_raw", ""))
            if not cat:
                cat = rec.get("_hint_code", "") or ""
                cat_label = LEAD_TYPES.get(cat, "")
            if cat and cat not in LEAD_TYPES:
                continue

            owner = rec.get("grantor", "") or ""
            parcel = lookup_parcel(owner, owner_index)

            filed_dt = parse_date(rec.get("filed", ""))
            week_old = bool(filed_dt and filed_dt >= week_ago)

            amount = parse_money(rec.get("amount_raw", ""))

            if parcel:
                prop_address = parcel.site_addr
                prop_city = parcel.site_city
                prop_zip = parcel.site_zip
                mail_address = parcel.mail_addr
                mail_city = parcel.mail_city
                mail_state = parcel.mail_state
                mail_zip = parcel.mail_zip
            else:
                prop_address = prop_city = prop_zip = ""
                mail_address = mail_city = mail_state = mail_zip = ""

            has_addr = bool(prop_address or mail_address)
            flags = derive_flags(rec, cat, has_addr, week_old)
            score = score_record(rec, cat, flags, amount, has_addr, week_old)

            out.append({
                "doc_num":     rec.get("doc_num", ""),
                "doc_type":    rec.get("doc_type_raw", ""),
                "filed":       rec.get("filed", ""),
                "cat":         cat,
                "cat_label":   cat_label,
                "owner":       owner,
                "grantee":     rec.get("grantee", ""),
                "amount":      amount,
                "legal":       rec.get("legal", ""),
                "prop_address": prop_address,
                "prop_city":    prop_city,
                "prop_state":   "TN",
                "prop_zip":     prop_zip,
                "mail_address": mail_address,
                "mail_city":    mail_city,
                "mail_state":   mail_state or "TN",
                "mail_zip":     mail_zip,
                "clerk_url":    rec.get("clerk_url", CLERK_PORTAL_URL),
                "flags":        flags,
                "score":        score,
            })
        except Exception as e:
            print(f"[enrich] skipping bad record: {e!r}", file=sys.stderr)
            continue

    out.sort(key=lambda r: r.get("score", 0), reverse=True)
    return out


def write_outputs(records: List[Dict[str, Any]]) -> None:
    fetched_at = datetime.now(timezone.utc).isoformat()
    end = datetime.now()
    start = end - timedelta(days=LOOKBACK_DAYS)
    payload = {
        "fetched_at":  fetched_at,
        "source":      "Shelby County Register + Assessor (Melvin Burgess)",
        "date_range":  f"{start:%Y-%m-%d} to {end:%Y-%m-%d}",
        "total":       len(records),
        "with_address": sum(1 for r in records if r.get("prop_address") or r.get("mail_address")),
        "records":     records,
    }
    for path in (DASH_DIR / "records.json", DATA_DIR / "records.json"):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, default=str))
        print(f"[write] {path} ({path.stat().st_size:,} bytes)")
    export_ghl_csv(records, DATA_DIR / "ghl_export.csv")


def export_ghl_csv(records: List[Dict[str, Any]], path: Path) -> None:
    cols = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        for r in records:
            first, last = split_owner_name(r.get("owner", ""))
            w.writerow([
                first, last,
                r.get("mail_address", ""), r.get("mail_city", ""),
                r.get("mail_state", ""), r.get("mail_zip", ""),
                r.get("prop_address", ""), r.get("prop_city", ""),
                r.get("prop_state", ""), r.get("prop_zip", ""),
                r.get("cat_label", ""), r.get("doc_type", ""),
                r.get("filed", ""), r.get("doc_num", ""),
                r.get("amount", 0), r.get("score", 0),
                "; ".join(r.get("flags", []) or []),
                "Shelby County, TN",
                r.get("clerk_url", ""),
            ])
    print(f"[write] {path} ({path.stat().st_size:,} bytes)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main_async() -> int:
    print("=" * 60)
    print(f"Shelby County Lead Pull — {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 60)

    # 1) Property assessor parcel index (best-effort)
    try:
        dbf_path = fetch_parcel_dbf()
    except Exception as e:
        print(f"[main] assessor fetch errored: {e}", file=sys.stderr)
        dbf_path = None
    owner_index = build_owner_index(dbf_path)

    # 2) Clerk portal scrape
    try:
        raw = await fetch_clerk_records(LOOKBACK_DAYS)
    except Exception as e:
        print(f"[main] clerk fetch errored: {e}", file=sys.stderr)
        raw = []

    # 3) Enrich, score, persist
    records = enrich_and_score(raw, owner_index)
    write_outputs(records)

    print(f"[done] {len(records)} records written")
    return 0


def main() -> int:
    try:
        return asyncio.run(main_async())
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        print(f"[fatal] {e!r}", file=sys.stderr)
        # Always emit an empty payload so downstream commits don't fail
        try:
            write_outputs([])
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    sys.exit(main())
