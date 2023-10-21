
import time

from wakepy import keep
import json

import pandas as pd
from threading import Thread

from fetcher import scrap_areas,engine,get


def format(parsed_json,engine,lang):
        df = pd.DataFrame(parsed_json["pageProps"]["data"]["vendors"],columns=["id","createdAt","name","rate","logo","heroImage","totalRatings","deliveryFee","avgDeliveryTime","deliveryTime","minimumOrderAmount"
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
   
        zone = pd.DataFrame(parsed_json["pageProps"]["data"]["area"],index=[0])
        
        zone.to_sql("areas_{}".format(lang),engine,if_exists="append",index=False)
    
      
        df.to_sql("vendors_{}".format(lang),engine,if_exists="append",index=False)
        del df
        del zone
     
def vendors(min:int,max:int,lang:str):
   

    for area in scrap_areas(min,max):
        page_number = 1
        with keep.presenting():
          
            while True: 
                restaurants = get("https://www.talabat.com/_next/data/13792ef3-65fb-4264-8910-d31837343b84/listing.json?lang=en&countrySlug={}&areaId={}&areaSlug={}&page={}"
                                  .format(area["country_name"],area["area_id"],area["area_name"],page_number),area)   
                
                restaurants = json.loads(restaurants.text)
                
                if "pageProps" in restaurants and "data" in restaurants["pageProps"]:
                    vendors = restaurants["pageProps"]["data"]["vendors"]
                    for vendor in vendors:
                      
                        _,country_name,_,branch_id,branch_slug = vendor["menuUrl"].split("/")
                        menu = get("https://www.talabat.com/_next/data/13792ef3-65fb-4264-8910-d31837343b84/menu.json?aid={}&lang={}&countrySlug={}&vertical=restaurant&branchId={}&branchSlug={}"
                                   .format(area["area_id"],lang,country_name,branch_id,branch_slug),area)
                      
                        menu = json.loads(menu.text)
                        if "pageProps" in  menu and "initialMenuState" in menu["pageProps"]:
                            print("adding")
                            items= menu["pageProps"]["initialMenuState"]["menuData"]["items"]
                            df = [{
                                 "rest_id":vendor["id"],
                                 "rest_name":vendor["name"],
                                 **item
                            } for item in items ]
                            df =pd.DataFrame(df,index=range(0,len(df)))
                         
                            df.to_sql("menu_{}".format(lang),engine,if_exists="append")
                            time.sleep(1)
                    format(restaurants,engine,lang)
                    print(page_number)
                    page_number += 1
                else:
                     break
                time.sleep(1)

                
        
 

Thread(name="Talabat Vendors Scraper EN 1",daemon=False,args=(1,3000,"en"),target=vendors).start()
Thread(name="Talabat Vendors Scraper EN 2",daemon=False,args=(3001,5000,"en"),target=vendors).start()
Thread(name="Talabat Vendors Scraper EN 3",daemon=False,args=(5001,7000,"en"),target=vendors).start()
Thread(name="Talabat Vendors Scraper EN 4",daemon=False,args=(7001,9000,"en"),target=vendors).start()
Thread(name="Talabat Vendors Scraper EN 5",daemon=False,args=(9001,10000,"en"),target=vendors).start()
Thread(name="Talabat Vendors Scraper AR 1",daemon=False,args=(1,3000,"ar"),target=vendors).start()
Thread(name="Talabat Vendors Scraper AR 2",daemon=False,args=(3001,5000,"ar"),target=vendors).start()
Thread(name="Talabat Vendors Scraper AR 3",daemon=False,args=(5001,7000,"ar"),target=vendors).start()
Thread(name="Talabat Vendors Scraper AR 4",daemon=False,args=(7001,9000,"ar"),target=vendors).start()
Thread(name="Talabat Vendors Scraper AR 5",daemon=False,args=(9001,10000,"ar"),target=vendors).start()
