import json
from threading import Thread
import time
import requests
from sqlalchemy import create_engine
from fetcher import scrap_areas
import pandas as pd
from wakepy import keep
from request_data import cookies,headers
engine = create_engine("mysql+mysqldb://root@localhost/test1")
def grocery(start,end,lang):
    try:
      for x in scrap_areas(start,end):
        page_number = 1
        while True:
            with keep.presenting():
                headers["User-Agent"] = str(x)
                res = requests.get("https://www.talabat.com/_next/data/ec09ba93-d0ec-4885-8040-1fceffc350ea/vertical/vertical-area.json?countrySlug={}&vertical=groceries&areaId={}&areaSlug={}&page={}&lang={}".format(x["country_name"],x["area_id"],x["area_name"],page_number,lang),cookies=cookies,headers=headers)
               
                res = json.loads(res.text)
                    
                if  "pageProps" in res  and "vendors" in res["pageProps"] and len(res["pageProps"]["vendors"]) > 0:
                    print("adding")
                        
                    df = pd.DataFrame(res["pageProps"]["vendors"],columns=["id","createdAt","name","rate","logo","heroImage","totalRatings","deliveryFee","avgDeliveryTime","deliveryTime","minimumOrderAmount"
                                                                                            ,"isTalabatGO","branchId","branchSlug","branchName","cuisineString","menuUrl",
                                                                                            "view","promotionText","discountText","isNew",
                                                                                            "acceptCreditCard","acceptDebitCard","acceptCash",
                                                                                            "totalReviews","status",
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
                    df.to_sql("grocery_{}".format(lang),con=engine,if_exists="append",index=False)
                    page_number += 1
                    time.sleep(1)
                  
                
                else:
                    break
                time.sleep(1)
                print(page_number)
    except requests.exceptions.ConnectTimeout:
       print("internet connection is down")
       grocery(start,end,lang)
# grocery("en")          
Thread(name="Talabat Grocery Scraper EN 1",daemon=False,args=(1,3000,"en"),target=grocery).start()
Thread(name="Talabat Grocery Scraper EN 2",daemon=False,args=(3001,5000,"en"),target=grocery).start()
Thread(name="Talabat Grocery Scraper EN 3",daemon=False,args=(5001,7000,"en"),target=grocery).start()
Thread(name="Talabat Grocery Scraper EN 4",daemon=False,args=(7001,9000,"en"),target=grocery).start()
Thread(name="Talabat Grocery Scraper EN 5",daemon=False,args=(9001,10000,"en"),target=grocery).start()
Thread(name="Talabat Grocery Scraper AR 1",daemon=False,args=(1,3000,"ar"),target=grocery).start()
Thread(name="Talabat Grocery Scraper AR 2",daemon=False,args=(3001,5000,"ar"),target=grocery).start()
Thread(name="Talabat Grocery Scraper AR 3",daemon=False,args=(5001,7000,"ar"),target=grocery).start()
Thread(name="Talabat Grocery Scraper AR 4",daemon=False,args=(7001,9000,"ar"),target=grocery).start()
Thread(name="Talabat Grocery Scraper AR 5",daemon=False,args=(9001,10000,"ar"),target=grocery).start()
