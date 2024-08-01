import pandas as pd
import requests
from wakepy import keep

from request_data import headers, cookies, engine
from fetcher import scrape_areas
from threading import Thread
import time

"""
the main function to run on multiple threads 
this function will fetch the data page by page 
minimum -- the first page to scrape 
maximum -- the last page to scrape 
lang -- the language of the data 
"""
def menu(minimum: int, maximum: int, lang: str):
    for d in scrape_areas(minimum, maximum):
        
        with keep.presenting(): # if the device is in sleep mode the the bot will stop working so this line will keep the device awake
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
                items:list = response["pageProps"]["initialMenuState"]["menuData"]["items"]
                list_items = []
                for item in items:
                    item.update({"area_id":d["area_id"]})
                    list_items.append(item)
              
                df = pd.DataFrame(list_items)
                df.to_sql(name=f"menu_{lang}", con=engine, if_exists="append", index=False)
                time.sleep(1)

"""
the threads that will run in parallel to the main function 
Note: Feel free to edit or remove any of the threads
"""
Thread(name="Talabat Menu Scraper EN 1", daemon=False, args=(1, 3000, "en"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 2", daemon=False, args=(3001, 5000, "en"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 3", daemon=False, args=(5001, 7000, "en"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 1", daemon=False, args=(1, 3000, "ar"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 2", daemon=False, args=(3001, 5000, "ar"), target=menu).start()
Thread(name="Talabat Menu Scraper EN 3", daemon=False, args=(5001, 7000, "ar"), target=menu).start()

