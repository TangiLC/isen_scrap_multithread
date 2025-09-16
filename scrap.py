import argparse
import os
import random
import time
import json
import sqlite3
import requests
from bs4 import BeautifulSoup
from typing import Any, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

URL = "https://realpython.github.io/fake-jobs/"


def fetch_html(url: str, th_name: Optional[str]) -> str:
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    if resp.status_code == 200:
        thrd = th_name.split("_", 1)[1] if th_name and "_" in th_name else ""
        bg_code = 100 + ((int(thrd) if thrd else 0) % 7)
        m_th = f"\033[{bg_code}mThread:{thrd}>\033[0m" if thrd != "" else ""
        # code = random.randint(90, 96)
        # mssg = f"\033[{code}m{url[19:]} \033[0m"
        print(f"{m_th} Succès ! Page {url[19:]} récupérée.")
        return resp.text
    else:
        return ""


def save_json(data: dict, filepath: str) -> None:
    dirpath = os.path.dirname(filepath)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def save_sqlite(data: Dict[int, Dict[str, Any]], db_path: str) -> None:
    """
    Crée/ouvre la base, crée trois tables :
      - titles(id, title)
      - regions(id, region)
      - jobs(id, title_id, company, ville, region_id, date, url, content)
    Ajoute les valeurs manquantes dans titles/regions et insère les jobs.
    """
    dirpath = os.path.dirname(db_path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS titles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT UNIQUE
            )
        """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS regions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region TEXT UNIQUE
            )
        """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY,
                title_id INTEGER,
                company TEXT,
                ville TEXT,
                region_id INTEGER,
                date TEXT,
                url TEXT,
                content TEXT,
                FOREIGN KEY(title_id) REFERENCES titles(id),
                FOREIGN KEY(region_id) REFERENCES regions(id)
            )
        """
        )

        for idx, item in data.items():
            title = item.get("title", "").strip()
            company = item.get("company", "")
            ville = (item.get("location") or {}).get("ville", "")
            region = (item.get("location") or {}).get("region", "")
            date = item.get("date", "")
            url = item.get("url", "")
            content = item.get("content", "")

            # --- gestion du titre ---
            cur.execute("SELECT id FROM titles WHERE title = ?", (title,))
            row = cur.fetchone()
            if row:
                title_id = row[0]
            else:
                cur.execute("INSERT INTO titles (title) VALUES (?)", (title,))
                title_id = cur.lastrowid

            # --- gestion du region ---
            region_id = None
            if region:
                cur.execute("SELECT id FROM regions WHERE region = ?", (region,))
                row = cur.fetchone()
                if row:
                    region_id = row[0]
                else:
                    cur.execute("INSERT INTO regions (region) VALUES (?)", (region,))
                    region_id = cur.lastrowid

            # --- insertion du job ---
            cur.execute(
                """
                INSERT OR REPLACE INTO jobs
                (id, title_id, company, ville, region_id, date, url, content)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (idx, title_id, company, ville, region_id, date, url, content),
            )

        conn.commit()
        print(f"{len(data)} jobs écrits dans {db_path}.")
    finally:
        conn.close()


def parse_cards(html: str) -> Dict[int, Tuple[str, BeautifulSoup]]:
    """
    Récupère tous les blocs <div class="card-content">
    Retourne {index: (title, bloc)}
    """
    soup = BeautifulSoup(html, "html.parser")
    cards: Dict[int, Tuple[str, BeautifulSoup]] = {}

    for idx, card in enumerate(soup.select("div.card-content")):
        title_el = card.select_one("h2.title")
        title = title_el.get_text(strip=True) if title_el else f"Job {idx}"
        cards[idx] = (title, card)

    return cards


def extract_info(
    idx: int, title: str, card: BeautifulSoup
) -> Tuple[int, Dict[str, str]]:
    """
    Récupère company, location, date, url et content (2ème page)
    """
    thread_name = threading.current_thread().name
    time.sleep(0.1)
    company_elem = card.select_one("h3.company")
    location_elem = card.select_one("p.location")
    time_elem = card.select_one("time")
    links = card.select("a.card-footer-item")
    apply_href = links[1]["href"] if len(links) > 1 else None

    company = company_elem.get_text(strip=True) if company_elem else "NC"
    str_loc = location_elem.get_text(strip=True) if location_elem else "NC,--"

    ville = str_loc.split(",", 1)[0]
    region = str_loc.split(",", 1)[1]
    location = {"ville": ville, "region": region}

    date = (
        time_elem.get("datetime")
        if time_elem and time_elem.get("datetime")
        else (time_elem.get_text(strip=True) if time_elem else "NC")
    )

    url = apply_href if apply_href else "NC"
    content = ""

    if url != "NC":
        html_detail = fetch_html(url, thread_name)
        if html_detail:
            sub_soup = BeautifulSoup(html_detail, "html.parser")
            p_elem = sub_soup.select_one("div.content p")
            if p_elem:
                content = p_elem.get_text(strip=True)

    return idx, {
        "title": title,
        "company": company,
        "location": location,
        "date": date,
        "url": url,
        "content": content,
    }


def scrape_jobs(workers: int) -> Dict[int, Dict[str, str]]:
    html = fetch_html(URL, "")
    if not html:
        print("Impossible de récupérer la page.")
        return {}

    cards = parse_cards(html)

    results: Dict[int, Dict[str, str]] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(extract_info, idx, title, container)
            for idx, (title, container) in cards.items()
        ]

        for future in as_completed(futures):
            idx, data = future.result()
            results[idx] = data

    return results


def main() -> None:
    start = time.time()
    parser = argparse.ArgumentParser(description="Scraper d'offres avec multithreading")
    parser.add_argument(
        "--workers", "-w", type=int, default=5, help="Nombre de workers (défaut: 5)"
    )
    args = parser.parse_args()

    print(
        f"Démarrage du scraping avec {args.workers} worker"
        f"{'s' if args.workers > 1 else ''} ..."
    )

    data = scrape_jobs(workers=args.workers)
    print(f"{len(data)} offres récupérées.")

    save_json(data, "scrap/fake-jobs.json")
    save_sqlite(data, "scrap/fake-jobs.db")

    print("Durée totale :", time.time() - start)


if __name__ == "__main__":
    main()
