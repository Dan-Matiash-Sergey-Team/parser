import requests
import json
import codecs
import pymongo
import time
import random

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
session = requests.Session()
retry = Retry(connect=5, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

def getDTPData(region_id, region_name, district_id, district_name, months, year):
    # if "Тюмень" in district_name:
    #     print("debug")
    cards_dict = {"data": {"date": ["MONTHS:1.2017"], "ParReg": "71100", "order": {"type": "1", "fieldName": "dat"},
                           "reg": "71118", "ind": "1", "st": "1", "en": "16"}}
    cards_dict["data"]["ParReg"] = region_id
    cards_dict["data"]["reg"] = district_id
    months_list = []
    json_data = None
    for month in months:
        months_list.append("MONTHS:" + str(month) + "." + str(year))
    cards_dict["data"]["date"] = months_list
    # постраничный перебор карточек
    start = 1
    increment = 500  # можно 100, не стоит 1000, т.к. можно словить таймаут запроса
    i = 0

    while True:
        cards_dict["data"]["st"] = str(start)
        cards_dict["data"]["en"] = str(start + increment - 1)
        # генерируем компактную запись json, без пробелов. иначе сайт не воспринимает данные
        cards_dict_json = {}

        cards_dict_json["data"] = json.dumps(cards_dict["data"], separators=(',', ':')).encode('utf8').decode(
            'unicode-escape')
        # cookie = {'_ga': 'GA1.2.478506347.1519754452', "_gid":"GA1.2.2037539788.1525170819", "JSESSIONID": "1B0BD20D95BB9D6462347C3D48EF8B13",
        #           "sputnik_session":"1525213652519|0"}
        #
        # r = requests.post("http://stat.gibdd.ru/map/getDTPCardData", json=cards_dict_json, cookies = cookie)
        proxies = ['94.154.191.35:45067',
                   '88.218.64.187:60884',
                   '194.32.222.221:48818',
                   '91.188.212.42:62378',
                   '45.89.70.254:54656',
                   '45.139.126.125:49601',
                   '91.188.245.129:62070',
                   '176.53.173.51:47468',
                   '92.249.15.18:50880',
                   '194.156.106.162:58508',
                   '45.138.144.63:64161',
                   '45.150.61.40:52846',
                   '45.142.73.34:60837',
                   '91.188.231.226:52360',
                   '45.147.13.187:50457',
                   '194.93.1.10:59648',
                   '45.145.168.30:49839',
                   '176.103.93.119:55343',
                   '45.95.31.158:47642',
                   ]
        r = session.post("http://stat.gibdd.ru/map/getDTPCardData", json=cards_dict_json, proxies={'https':f'https://{random.choice(proxies)}@AY7z1p2r:xeRM6ARH'})
        i += 1
        print(i)
        if r.status_code == 200:
            try:
                cards = json.loads(json.loads(r.content)["data"])["tab"]
            except:
                log_text = u"Отсутствуют данные для {0} ({1}) за {2}-{3}.{4}". \
                    format(region_name, district_name, months[0], months[len(months) - 1], year)
                break
                # log_text = u"Не удалось получить данные для {} ({}) за {}-{}.{}, диапазон номеров карточек {}-{}". \
                #     format(region_name, district_name, months[0], months[len(months) - 1], year, start, start + increment - 1)
                # print(log_text)
                # write_log(log_text)
                # start += increment
                # continue

            if len(cards) > 0:
                if json_data is None:
                    json_data = cards
                else:
                    json_data = json_data + cards
            if len(cards) == increment:
                start += increment
            else:
                break
        else:
            if "Unexpected character (',' (code 44))" in r.text:  # карточки закончились
                break
            # if "No content to map due to end-of-input" in r.text: # или ошибка JS - для этого района нет данных
            else:
                log_text = u"Отсутствуют данные для {0} ({1}) за {2}-{3}.{4}". \
                    format(region_name, district_name, months[0], months[len(months) - 1], year)

                break

    return json_data


filename = "regions.json"
with codecs.open(filename, "r", "utf-8") as f:
    regions = json.loads(json.loads(json.dumps(f.read())))

region_id = "45"

username = 'universai'
password = 'cumzone'
cluster = '195.133.147.101:1488'
client = pymongo.MongoClient(f"mongodb://{username}:{password}@{cluster}")
db = client.dtp.dtp
for year in range(2020, 2021):
    for month in range(1, 13):
        for region in regions:
            # была запрошена статистика по одному из регионов, а не по РФ
            if region_id != "0" and region["id"] != region_id:
                continue

            # муниципальные образования в регионе
            districts = json.loads(region["districts"])
            for district in districts:
                # получение карточек ДТП
                cards = getDTPData(region["id"], region["name"], district["id"], district["name"], [month], year)
                if cards is None:
                    continue
                for card in cards:
                    data = {}
                    data['id'] = card['KartId']
                    data['data'] = card
                    data['year'] = year
                    data['district_id'] = district['id']
                    data['district_name'] = district['name']
                    data['month'] = month
                    if len(list(db.find({'id': data['id']}))) == 0:
                        db.insert_one(data)
            if region["id"] == region_id:
                break
