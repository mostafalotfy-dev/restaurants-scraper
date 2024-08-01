# talabat scraper 

* this project made to scrape restuarant ("talabat" to be specific) and insert it to mysql database
* the database default name is "test" but you can change it from the "request_data.py" file
## Project Setup 
1. Create a database called "test" 
2. run one of "restuarants.py" using the following command to scrape the restuarants:
```
python restuarants.py
OR
python menu.py
```
* use restuarants.py to fetch restuarants or menu.py to fetch the menu (one command per run)
### Why i cannot run the two files at the same time 

the "cloudflare" will prevent the bot from sending more than 6 requests per second since the project using multithreading to fetch data (3 threads for arabic version and 3 threads for english version).

* read the code and hit the star button if you like the project.

* if you have any question ,need help or if you found a bug make an issue


