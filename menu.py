import pandas as pd
import requests
from wakepy import keep

from request_data import headers, cookies, engine
from fetcher import scrape_areas
from threading import Thread
import time


def menu(minimum: int, maximum: int, lang: str):
    for d in scrape_areas(minimum, maximum):
        with keep.presenting():
            response = requests.get(
                "https://www.talabat.com/_next/data/manifests/menu.json?aid={}&countrySlug={}&vertical=restaurant&branchId={}&branchSlug={}&lang={}"
                .format(d["area_id"], d["country_name"], d["current_number"], "", lang), headers=headers,
                cookies=cookies)
            try:
                response = response.json()
            except:
                print(f"failed {d['current_number']}  {lang}")
                continue
            if response.__contains__("pageProps") and response["pageProps"].__contains__("initialMenuState"):
                df = pd.DataFrame(response["pageProps"]["initialMenuState"]["menuData"]["items"])
                df.to_sql(name=f"menu_{lang}", con=engine, if_exists="append", index=False)
                time.sleep(1)


Thread(name="Talabat Menu Scraper EN 1", daemon=False, args=(1, 3000, "en"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 2", daemon=False, args=(3001, 5000, "en"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 3", daemon=False, args=(5001, 7000, "en"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 4", daemon=False, args=(7001, 9000, "en"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 5", daemon=False, args=(9001, 10000, "en"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 6", daemon=False, args=(10001, 15000, "en"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 7", daemon=False, args=(15001, 20000, "en"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 8", daemon=False, args=(20001, 21000, "en"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 9", daemon=False, args=(21001, 22000, "en"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 10", daemon=False, args=(22001, 23000, "en"), target=menu).start()

Thread(name="Talabat Menu Scraper EN 1", daemon=False, args=(1, 3000, "ar"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 2", daemon=False, args=(3001, 5000, "ar"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 3", daemon=False, args=(5001, 7000, "ar"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 4", daemon=False, args=(7001, 9000, "ar"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 5", daemon=False, args=(9001, 10000, "ar"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 6", daemon=False, args=(10001, 15000, "ar"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 7", daemon=False, args=(15001, 20000, "ar"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 8", daemon=False, args=(20001, 21000, "ar"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 9", daemon=False, args=(21001, 22000, "ar"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 10", daemon=False, args=(22000, 23000, "ar"), target=menu).start()
