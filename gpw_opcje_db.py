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


def GetOptions(web_page, config):


    def InputToFloat(inpt):
        inpt = inpt.replace(' ', '')
        inpt = inpt.replace(u'\xa0', u'')
        inpt = inpt.replace('%', '')
        inpt = inpt.replace(',', '.')
        return float(inpt)


    def GetOptionType(opt_name, config):
        if opt_name[4] in config['opcje_call']:
            opt_type = 'call'
        elif opt_name[4] in config['opcje_put']:
            opt_type = 'put'
        else:
            opt_type = 'undefined'
        return opt_type


    def GetTDContent(obj, pattern):
        return obj.find('td', {'class': compile(pattern)}).contents[0].strip()

    def ISODate(date_str, format_str):
        return datetime.strptime(date_str, format_str).isoformat()


    opcje = {}
    for walor in BeautifulSoup(web_page.read(), 'lxml').findAll('tr'):
        pole_nazwa = walor.find('td', {'class': compile('colWalor*')})
        if pole_nazwa is None:
            continue
        nazwa = pole_nazwa.contents[0].strip().upper()
        opcje[nazwa] = {}
        opcje[nazwa]['type'] = GetOptionType(nazwa, config)
        opcje[nazwa]['wigp'] = float(nazwa[-4:])
        opcje[nazwa]['exchange'] = InputToFloat(GetTDContent(walor, 'colKurs*'))
        opcje[nazwa]['change'] = InputToFloat(GetTDContent(walor, 'colZmiana*'))
        opcje[nazwa]['pchange'] = InputToFloat(GetTDContent(walor, 'colZmianaProcentowa*'))
        opcje[nazwa]['open'] = InputToFloat(GetTDContent(walor, 'colOtwarcie*'))
        opcje[nazwa]['max'] = InputToFloat(GetTDContent(walor, 'calMaxi*'))
        opcje[nazwa]['min'] = InputToFloat(GetTDContent(walor, 'calMini*'))
        date_str = str(date.today().year) + '.' + GetTDContent(walor, 'colAktualizacja*')
        opcje[nazwa]['date'] = ISODate(date_str, "%Y.%d.%m %H:%M")
        date_str = GetTDContent(walor, 'colTermin*')
        opcje[nazwa]['term'] = ISODate(date_str, "%Y-%m-%d")
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
    for key, value in GetOptions(strona, cfg).items():
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
