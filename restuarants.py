import json
from threading import Thread
import time
import requests

from fetcher import scrape_areas
import pandas as pd
from wakepy import keep
from request_data import cookies, headers,engine




def grocery(start, end, lang):
    for x in scrape_areas(start, end):
        page_number = 1
        while True:
            with keep.presenting():
                headers["User-Agent"] = str(x)
                try:
                    res = requests.get(
                        "https://www.talabat.com/_next/data/manifests/listing.json?countrySlug={}&areaId={}&areaSlug={}&page={}&lang={}".format(
                            x["country_name"], x["area_id"], x["area_name"], page_number, lang), cookies=cookies,
                        headers=headers).json()
                except:
                    print(
                        "unable to parse json. make sure you changed the cookies and headers in request_data.py "
                        "file")
                    return

            if "pageProps" in res and "data" in res["pageProps"] and len(res["pageProps"]["data"]) > 0:
                print("adding")

                df = pd.DataFrame(res["pageProps"]["data"]["vendors"],
                                  columns=["id", "createdAt", "name", "rate", "logo", "heroImage",
                                           "totalRatings", "deliveryFee", "avgDeliveryTime", "deliveryTime",
                                           "minimumOrderAmount",
                                           "isTalabatGO",
                                           "branchId",
                                           "branchSlug",
                                           "branchName",
                                           "cuisineString",
                                           "menuUrl",
                                           "view", "promotionText", "discountText", "isNew",
                                           "acceptCreditCard", "acceptDebitCard", "acceptCash",
                                           "totalReviews", "status",
                                           "statusCode",
                                           "isCateringAvailable",
                                           "deliveryChargesType",
                                           "contactlessDelivery",
                                           "isDarkstore",
                                           "grlRequired",
                                           "isCokeRestaurant",
                                           "IsMigratedToDh",
                                           "IsProvideTracking",
                                           "shopType",
                                           "shopPosition",
                                           "isProvideOrderStatus",
                                           "shopArea",
                                           "shopCity",
                                           "isShopSponcered",
                                           "verticalType"
                                           ])
                df.to_sql("rest_{}".format(lang), con=engine, if_exists="append", index=False)
                page_number += 1
                time.sleep(1)


            else:
                break
            time.sleep(1)
            print(page_number)


Thread(name="Talabat Restaurants Scraper EN 1", daemon=False, args=(1, 3000, "en"), target=grocery).start()
Thread(name="Talabat Restaurants Scraper EN 2", daemon=False, args=(3001, 5000, "en"), target=grocery).start()
Thread(name="Talabat Restaurants Scraper EN 3", daemon=False, args=(5001, 7000, "en"), target=grocery).start()
Thread(name="Talabat Restaurants Scraper EN 4", daemon=False, args=(7001, 9000, "en"), target=grocery).start()
Thread(name="Talabat Restaurants Scraper EN 5", daemon=False, args=(9001, 10000, "en"), target=grocery).start()
Thread(name="Talabat Restaurants Scraper AR 1", daemon=False, args=(1, 3000, "ar"), target=grocery).start()
Thread(name="Talabat Restaurants Scraper AR 2", daemon=False, args=(3001, 5000, "ar"), target=grocery).start()
Thread(name="Talabat Restaurants Scraper AR 3", daemon=False, args=(5001, 7000, "ar"), target=grocery).start()
Thread(name="Talabat Restaurants Scraper AR 4", daemon=False, args=(7001, 9000, "ar"), target=grocery).start()
Thread(name="Talabat Restaurants Scraper AR 5", daemon=False, args=(9001, 10000, "ar"), target=grocery).start()
