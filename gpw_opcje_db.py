#!/usr/bin/env python3

from json import loads
from urllib.request import urlopen
from bs4 import BeautifulSoup
from re import compile
from datetime import datetime
from datetime import date
import sqlite3
import os



path = '/'.join(os.path.abspath(__file__).split('/')[:-1])

cfg = loads(open(os.path.join(path, 'gpw_opcje_db.conf')).read())


def GetOptions(web_page):
  
    opcje = {}
  
    for walor in BeautifulSoup(web_page.read(), 'lxml').findAll('tr'):
        
        pole_nazwa = walor.find('td', {'class':'colWalor textNowrap'})
    
        if pole_nazwa is None:
            
            continue
    
        nazwa = pole_nazwa.contents[0].strip().upper()
        opcje[nazwa] = {}
      
        if nazwa[4] in cfg['opcje_call']:
      
            opcje[nazwa]['type'] = 'call'
        
        elif nazwa[4] in cfg['opcje_put']:
      
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

    return opcje



def main():
    
    log_path = os.path.join(path, 'log.txt')

    if not os.path.isfile(log_path):
        
        plik = open(log_path, 'w')
        plik.write(datetime.now().isoformat() + ' - File "log.txt" does not exist - creating log.txt file.\n')
        plik.close()
        
    if not os.path.isdir(os.path.join(path, 'database')):

        os.makedirs(os.path.join(path, 'database'), mode=0o755)
        plik = open(log_path, 'a')
        plik.write(datetime.now().isoformat() + ' - Folder "database" does not exist - creating "database" folder.\n')
        plik.close()

    try:
        
        strona = urlopen(cfg['adres_opcje'])
        
    except:
        
        plik = open(log_path, 'a')
        plik.write(datetime.now().isoformat() + ' - Unable to connect with webpage - exiting.\n')
        plik.close()
        raise SystemExit()
    
    try:
        
        db = sqlite3.connect(os.path.join(path, 'database', cfg['database']))
        
    except:
        
        plik = open(log_path, 'a')
        plik.write(datetime.now().isoformat() + ' - Unable to connect with database - exiting.\n')
        plik.close()
        raise SystemExit()        
        
    cur = db.cursor()

    for key, value in GetOptions(strona).items():
  
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
