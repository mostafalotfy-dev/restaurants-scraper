#! /bin/env python

import requests
from sqlalchemy import create_engine


from request_data import headers,cookies
engine = create_engine("mysql+mysqldb://root@localhost/test")
def scrap_areas(min,max):
   
    for x in range(min,max):
          
            headers["User-Agent"] = str(x)
            areas = requests.get("https://www.talabat.com/oman/restaurants/{}/halban".format(x),cookies=cookies, headers=headers)
            if len(areas.url.split("/")) != 7:
                  continue
            _ , _, domain,country_name,rest,area_id,area_name  = areas.url.split("/")
            del areas
            yield {
                    "country_name":country_name,
                    "area_name":area_name,
                    "domain":domain,
                    "area_id":area_id,
                    "type":rest,
                    "current_number":x
                }
            


def get(url:str,x:dict[str,str]):
      headers["User-Agent"] = str(x)
      return requests.get(url,cookies=cookies,headers=headers)
     


