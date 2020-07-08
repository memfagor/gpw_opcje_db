#!/usr/bin/env python3

import logging
import sqlite3
import os
from json import load
from urllib.request import urlopen
from bs4 import BeautifulSoup
from re import compile
from datetime import date, datetime


def GetScriptPath():
    return '/'.join(os.path.abspath(__file__).split('/')[:-1])


def GetConfig(path, fname):
    with open(os.path.join(path, fname), 'r') as config_file:
        config = load(config_file)
    return config


def GetOptions(web_page):


    def InputToFloat(inpt):
        inpt = inpt.strip()
        inpt = inpt.replace(' ', '')
        inpt = inpt.replace(u'\xa0', u'')
        inpt = inpt.replace('%', '')
        inpt = inpt.replace(',', '.')
        return float(inpt)


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
        opcje[nazwa]['exchange'] = InputToFloat(walor.find('td', {'class': compile('colKurs*')}).contents[0])
        opcje[nazwa]['change'] = InputToFloat(walor.find('td', {'class': compile('colZmiana*')}).contents[0])
        opcje[nazwa]['pchange'] = InputToFloat(walor.find('td', {'class': compile('colZmianaProcentowa*')}).contents[0])
        opcje[nazwa]['open'] = InputToFloat(walor.find('td', {'class':'colOtwarcie'}).contents[0])
        opcje[nazwa]['max'] = InputToFloat(walor.find('td', {'class':'calMaxi'}).contents[0])
        opcje[nazwa]['min'] = InputToFloat(walor.find('td', {'class':'calMini'}).contents[0])
        date_str = str(date.today().year) + '.' + walor.find('td', {'class':'colAktualizacja'}).contents[0].strip()
        opcje[nazwa]['date'] = datetime.strptime(
                date_str, "%Y.%d.%m %H:%M").isoformat()
        date_str = walor.find('td', {'class':'colTermin'}).contents[0].strip()
        opcje[nazwa]['term'] = datetime.strptime(
                date_str, "%Y-%m-%d").isoformat()
        opcje[nazwa]['tstamp'] = datetime.now().isoformat()
    return opcje


def main():
    path = GetScriptPath()
    cfg = GetConfig(path, 'gpw_opcje_db.conf')
    logging.basicConfig(
            format='%(asctime)s %(levelname)s:%(message)s',
            filename=os.path.join(path, cfg['logfile']),
            level=logging.DEBUG)
    if not os.path.isdir(os.path.join(path, 'database')):
        logging.warning('Folder "database" does not exist - creating one.')
        os.makedirs(os.path.join(path, 'database'), mode=0o755)
    try:
        strona = urlopen(cfg['adres_opcje'])
    except Exception as e:
        logging.error(e)
        raise SystemExit()
    try:
        db = sqlite3.connect(os.path.join(path, 'database', cfg['database']))
    except Exception as e:
        logging.error(e)
        raise SystemExit()
    cur = db.cursor()
    for key, value in GetOptions(strona).items():
        cols = value.keys()
        vals = value.values()
        query = "CREATE TABLE IF NOT EXISTS {0} (type TEXT, wigp REAL," \
                " exchange REAL, change REAL, pchange REAL, open REAL," \
                "max REAL, min REAL, date TEXT, term TEXT," \
                "tstamp TEXT PRIMARY KEY)".format(key)
        cur.execute(query)
        query = "INSERT INTO {0} ({1}) VALUES ({2})".format(
                key,', '.join(cols),', '.join(['?'] * len(value)))
        cur.execute(query, list(vals))
    db.commit()
    db.close()


if __name__ == "__main__":
    main()
