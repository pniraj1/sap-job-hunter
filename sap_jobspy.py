"""
SAP Job Hunter — Python Scraper
================================
Sources: LinkedIn (li_at cookie), Indeed Germany, Google Jobs, XING
Output:  jobs_latest.json  (dashboard-ready schema)

Run locally:  python sap_jobspy.py
Run on Colab: upload this file, then !python sap_jobspy.py
GitHub Actions runs this automatically every day at 8am Berlin time.

SETUP:
  pip install jobspy playwright pandas
  playwright install chromium

  Set environment variable (or GitHub Secret):
    LI_AT=<your linkedin li_at cookie value>

  How to get li_at cookie:
    1. Log into linkedin.com in Chrome
    2. Open DevTools → Application → Cookies → linkedin.com
    3. Find the cookie named "li_at" — copy its Value
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta

# ── Try importing dependencies ──────────────────────────────────────
try:
    import pandas as pd
    from jobspy import scrape_jobs
    JOBSPY_OK = True
except ImportError:
    JOBSPY_OK = False
    print("WARNING: jobspy not installed. Run: pip install jobspy pandas")

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_OK = True
except ImportError:
    PLAYWRIGHT_OK = False
    print("WARNING: playwright not installed. XING scraping disabled.")
    print("         Run: pip install playwright && playwright install chromium")


# ── Config ───────────────────────────────────────────────────────────
SEARCH_TERM    = "SAP MM Consultant"
LOCATION       = "Germany"
HOURS_OLD      = 72          # only jobs posted in last 72 hours
RESULTS_WANTED = 40          # per source
LI_AT          = os.environ.get("LI_AT", "")   # LinkedIn cookie
OUTPUT_FILE    = "jobs_latest.json"


# ── Schema helper ────────────────────────────────────────────────────
def make_job(title, company, location, job_url, source,
             description="", raw_date=None, salary=None,
             remote_type="Onsite", key_skills=None):
    return {
        "title":        str(title or "").strip(),
        "company":      str(company or "").strip(),
        "location":     str(location or "").strip(),
        "jobUrl":       str(job_url or "").strip(),
        "source":       str(source or "").strip(),
        "fullJD":       str(description or "").strip(),
        "rawDate":      str(raw_date) if raw_date else None,
        "salary":       str(salary) if salary else None,
        "remoteType":   remote_type,
        "keySkills":    key_skills or [],
        "fitScore":     0,      # dashboard calculates this
        "germanRequired": False, # dashboard detects from fullJD
        "langStatus":   "unverified",
    }


# ── JOBSPY scraper ───────────────────────────────────────────────────
def run_jobspy():
    if not JOBSPY_OK:
        return []

    site_names = ["indeed", "google"]
    proxies    = None

    # Add LinkedIn if cookie is available
    if LI_AT:
        site_names.append("linkedin")
        print(f"  LinkedIn: using li_at cookie ({LI_AT[:8]}...)")
    else:
        print("  LinkedIn: no LI_AT cookie found — skipping")
        print("  Set LI_AT environment variable to enable LinkedIn scraping")

    print(f"  Scraping {site_names} via JobSpy...")

    try:
        df = scrape_jobs(
            site_name=site_names,
            search_term=SEARCH_TERM,
            location=LOCATION,
            country_indeed="Germany",
            hours_old=HOURS_OLD,
            results_wanted=RESULTS_WANTED,
            linkedin_fetch_description=True,
            linkedin_company_ids=None,
            proxies=proxies,
            # Pass li_at cookie for LinkedIn auth
            **({"linkedin_cookies": [{"name": "li_at", "value": LI_AT,
                "domain": ".linkedin.com", "path": "/"}]}
               if LI_AT else {})
        )
    except Exception as e:
        print(f"  JobSpy error: {e}")
        return []

    jobs = []
    for _, row in df.iterrows():
        # Build salary string
        salary = None
        if pd.notna(row.get("min_amount")) and pd.notna(row.get("max_amount")):
            curr = row.get("currency", "EUR")
            salary = f"{curr} {int(row['min_amount']):,}–{int(row['max_amount']):,}"
        elif pd.notna(row.get("min_amount")):
            curr = row.get("currency", "EUR")
            salary = f"from {curr} {int(row['min_amount']):,}"

        # Remote type
        is_remote = row.get("is_remote", False)
        remote_type = "Remote" if is_remote else "Onsite"

        # Posting date
        raw_date = None
        if pd.notna(row.get("date_posted")):
            raw_date = str(row["date_posted"])

        jobs.append(make_job(
            title       = row.get("title"),
            company     = row.get("company"),
            location    = row.get("location"),
            job_url     = row.get("job_url"),
            source      = str(row.get("site", "jobspy")).capitalize(),
            description = row.get("description") or "",
            raw_date    = raw_date,
            salary      = salary,
            remote_type = remote_type,
        ))

    print(f"  JobSpy: {len(jobs)} jobs scraped")
    return jobs


# ── XING scraper ─────────────────────────────────────────────────────
def run_xing():
    if not PLAYWRIGHT_OK:
        return []

    print("  Scraping XING via Playwright...")
    jobs = []

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()

            # Intercept XING's internal GraphQL job search API
            captured = []

            def handle_response(response):
                if "xing.com" in response.url and response.status == 200:
                    ct = response.headers.get("content-type", "")
                    if "json" in ct:
                        try:
                            data = response.json()
                            if isinstance(data, dict):
                                # XING returns jobs in various nested paths
                                items = (
                                    data.get("jobs", {}).get("collection", []) or
                                    data.get("data", {}).get("jobSearchByQuery", {})
                                        .get("collection", []) or
                                    data.get("collection", [])
                                )
                                if items:
                                    captured.extend(items)
                        except Exception:
                            pass

            page.on("response", handle_response)

            # Build XING search URL
            query = SEARCH_TERM.replace(" ", "%20")
            url   = (f"https://www.xing.com/jobs/search?"
                     f"keywords={query}&location=Deutschland"
                     f"&radius=0&sort=date")

            page.goto(url, wait_until="networkidle", timeout=30000)
            time.sleep(3)

            # Also try scraping visible job cards from DOM
            cards = page.query_selector_all('[data-testid="job-posting-item"]')
            for card in cards:
                try:
                    title   = card.query_selector('a[data-testid="job-title"]')
                    company = card.query_selector('[data-testid="company-name"]')
                    loc     = card.query_selector('[data-testid="job-location"]')
                    date_el = card.query_selector("time")
                    link    = card.query_selector("a[href]")

                    jobs.append(make_job(
                        title    = title.inner_text() if title else "",
                        company  = company.inner_text() if company else "",
                        location = loc.inner_text() if loc else "Germany",
                        job_url  = ("https://www.xing.com" + link.get_attribute("href")
                                   if link else ""),
                        source   = "XING",
                        raw_date = (date_el.get_attribute("datetime")
                                   if date_el else None),
                    ))
                except Exception:
                    continue

            # Parse captured GraphQL responses
            for item in captured:
                try:
                    jd = item.get("jobPosting") or item
                    salary = None
                    sal    = jd.get("salary", {}) or {}
                    if sal.get("minimum") and sal.get("maximum"):
                        salary = f"EUR {sal['minimum']:,}–{sal['maximum']:,}"

                    jobs.append(make_job(
                        title    = jd.get("title") or jd.get("name", ""),
                        company  = (jd.get("company") or {}).get("name", ""),
                        location = (jd.get("location") or {}).get("city", "Germany"),
                        job_url  = jd.get("url") or jd.get("href") or "",
                        source   = "XING",
                        raw_date = jd.get("activatedAt") or jd.get("publishedAt"),
                        salary   = salary,
                        remote_type = ("Remote"
                                      if jd.get("isRemote") or
                                         jd.get("workingModel") == "REMOTE"
                                      else "Onsite"),
                    ))
                except Exception:
                    continue

            browser.close()

    except Exception as e:
        print(f"  XING Playwright error: {e}")
        return []

    # Deduplicate XING results by URL
    seen_urls = set()
    unique = []
    for j in jobs:
        if j["jobUrl"] and j["jobUrl"] not in seen_urls:
            seen_urls.add(j["jobUrl"])
            unique.append(j)

    print(f"  XING: {len(unique)} jobs scraped")
    return unique


# ── Language detection (client-side equivalent) ──────────────────────
GERMAN_PATTERNS = re.compile(
    r"deutsch(kenntnisse|e?s?|sprachig)?[\s\w]{0,20}"
    r"(erforderlich|vorausgesetzt|zwingend|fließend|b2|c1|niveau)"
    r"|fließend[e\s]+deutsch"
    r"|sprachkenntnisse[\s\w]{0,10}deutsch"
    r"|german[\s\w]{0,10}(b2|c1|mandatory|required|fluent)",
    re.IGNORECASE
)

def detect_german_required(text):
    return bool(GERMAN_PATTERNS.search(text or ""))


# ── Deduplication ────────────────────────────────────────────────────
def normalise(s):
    return re.sub(r"\s+", " ",
           re.sub(r"[^a-z0-9]", " ", (s or "").lower())).strip()

def fuzzy_match(a, b):
    wa = set(w for w in normalise(a).split() if len(w) > 3)
    wb = set(w for w in normalise(b).split() if len(w) > 3)
    if not wa or not wb:
        return False
    overlap = len(wa & wb)
    return overlap / min(len(wa), len(wb)) >= 0.65

def deduplicate(jobs):
    seen, seen_urls = [], set()
    for j in jobs:
        url = (j.get("jobUrl") or "").strip().lower().rstrip("/")
        if url and url in seen_urls:
            continue
        if any(fuzzy_match(s["company"], j["company"]) and
               fuzzy_match(s["title"], j["title"]) for s in seen):
            continue
        if url:
            seen_urls.add(url)
        seen.append(j)
    return seen


# ── Main ─────────────────────────────────────────────────────────────
def main():
    print("\n╔══════════════════════════════════════╗")
    print("║  SAP Job Hunter — Python Scraper     ║")
    print(f"║  {datetime.now().strftime('%Y-%m-%d %H:%M')} Berlin time          ║")
    print("╚══════════════════════════════════════╝\n")

    all_jobs = []

    # 1. JobSpy (Indeed + Google + LinkedIn)
    print("[1/2] JobSpy scraper")
    jobspy_jobs = run_jobspy()
    all_jobs.extend(jobspy_jobs)

    # 2. XING
    print("\n[2/2] XING scraper")
    xing_jobs = run_xing()
    all_jobs.extend(xing_jobs)

    # Detect German requirements from full JD text
    for j in all_jobs:
        j["germanRequired"] = detect_german_required(j.get("fullJD", ""))
        j["langStatus"]     = "unverified"  # dashboard re-checks

    # Deduplicate
    before = len(all_jobs)
    all_jobs = deduplicate(all_jobs)
    print(f"\n✓ Deduplication: {before} → {len(all_jobs)} unique jobs")

    # Sort by date (freshest first), nulls last
    def sort_key(j):
        rd = j.get("rawDate")
        if not rd or rd == "None":
            return datetime.min.replace(tzinfo=timezone.utc)
        for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
                    "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z"]:
            try:
                dt = datetime.strptime(rd[:len(fmt)], fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                continue
        return datetime.min.replace(tzinfo=timezone.utc)

    all_jobs.sort(key=sort_key, reverse=True)

    # Write output
    out = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "total":     len(all_jobs),
        "sources":   list({j["source"] for j in all_jobs}),
        "jobs":      all_jobs,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Saved {len(all_jobs)} jobs → {OUTPUT_FILE}")
    print(f"   Sources: {', '.join(out['sources'])}")

    # Print freshness breakdown
    now = datetime.now(timezone.utc)
    buckets = {"today":0, "yesterday":0, "2-3d":0, "older":0, "unknown":0}
    for j in all_jobs:
        dt = sort_key(j)
        if dt == datetime.min.replace(tzinfo=timezone.utc):
            buckets["unknown"] += 1
        else:
            age = (now - dt).days
            if age == 0:   buckets["today"]     += 1
            elif age == 1: buckets["yesterday"] += 1
            elif age <= 3: buckets["2-3d"]      += 1
            else:          buckets["older"]      += 1

    print(f"\n   Freshness breakdown:")
    print(f"   Today:     {buckets['today']}")
    print(f"   Yesterday: {buckets['yesterday']}")
    print(f"   2–3 days:  {buckets['2-3d']}")
    print(f"   Older:     {buckets['older']}")
    print(f"   Unknown:   {buckets['unknown']}\n")


if __name__ == "__main__":
    main()
