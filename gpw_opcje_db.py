#!/usr/bin/env python3

from urllib.request import urlopen
from bs4 import BeautifulSoup
from re import compile
from datetime import datetime
from datetime import date
import sqlite3


def main():

    adres_opcje = 'https://www.bankier.pl/gielda/notowania/opcje'
    db_path = '/home/listonosz/gpw/database/gpw_opcje.db'  
    opcje_call = ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L')
    opcje_put = ('M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X')
  
    strona = urlopen(adres_opcje).read()
    walory = BeautifulSoup(strona, 'lxml').findAll('tr')
  
    opcje = {}
  
    for walor in walory:
  
        pole_nazwa = walor.find('td', {'class':'colWalor textNowrap'})
    
        if pole_nazwa is None:

            continue
    
        nazwa = pole_nazwa.contents[0].strip().upper()
        opcje[nazwa] = {}
      
        if nazwa[4] in opcje_call:
      
            opcje[nazwa]['type'] = 'call'
        
        elif nazwa[4] in opcje_put:
      
            opcje[nazwa]['type'] = 'put'

        else:

            opcje[nazwa]['type'] = 'undefined'
        
        opcje[nazwa]['wigp'] = float(nazwa[-4:])
        opcje[nazwa]['exchange'] = float(walor.find('td', {'class': compile('colKurs*')}).contents[0].strip().replace(',', '.'))
        opcje[nazwa]['change'] = float(walor.find('td', {'class': compile('colZmiana*')}).contents[0].strip().replace(',', '.'))
        opcje[nazwa]['pchange'] = float(walor.find('td', {'class': compile('colZmianaProcentowa*')}).contents[0].strip().replace(',', '.').replace('%', ''))
        opcje[nazwa]['open'] = float(walor.find('td', {'class':'colOtwarcie'}).contents[0].strip().replace(',', '.'))
        opcje[nazwa]['max'] = float(walor.find('td', {'class':'calMaxi'}).contents[0].strip().replace(',', '.'))
        opcje[nazwa]['min'] = float(walor.find('td', {'class':'calMini'}).contents[0].strip().replace(',', '.'))
        date_str = str(date.today().year) + '.' + walor.find('td', {'class':'colAktualizacja'}).contents[0].strip()
        opcje[nazwa]['date'] = datetime.strptime(date_str, "%Y.%d.%m %H:%M").isoformat()
        date_str = walor.find('td', {'class':'colTermin'}).contents[0].strip()
        opcje[nazwa]['term'] = datetime.strptime(date_str, "%Y-%m-%d").isoformat()
        opcje[nazwa]['tstamp'] = datetime.now().isoformat()
  
    db = sqlite3.connect(db_path)
    cur = db.cursor()

    for key, value in opcje.items():
  
        cols = value.keys()
        vals = value.values()
        query = "CREATE TABLE IF NOT EXISTS {0} (type TEXT, wigp REAL, exchange REAL, change REAL, pchange REAL, open REAL, max REAL, min REAL, date TEXT, term TEXT, tstamp TEXT PRIMARY KEY)".format(key)
        cur.execute(query)
        query = "INSERT INTO {0} ({1}) VALUES ({2})".format(key,', '.join(cols),', '.join(['?'] * len(value)))
        cur.execute(query, list(vals))
        
    db.commit()
    db.close()
      
if __name__ == "__main__":
    main()
