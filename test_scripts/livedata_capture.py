# Testing connection
# The library would warn you for updates whenever they are.
from truedata_ws.websocket.TD import TD
from copy import deepcopy
import pandas as pd
import time
import json
import datetime
from influxdb import InfluxDBClient
import logging
logging.basicConfig(level=logging.INFO)

usname = 'wssand001'
psword = 'john001'
td_app = TD(usname, psword)

host = 'localhost'
port = 8086
user = ''
password = ''
dbname = 'LiveData'

conn=InfluxDBClient(host,port,user,password,dbname)
ret = conn.create_database(dbname)
logging.info("create database "+str(ret))

#symbols = ['RELIANCE', 'ITC','20MICRONS', '21STCENMGM', '3IINFOTECH', '3MINDIA', '3PLAND']

symbols=['AXISBANK','UPL','GRASIM','KOTAKBANK']

# symbols =['20MICRONS', '21STCENMGM', '3IINFOTECH', '3MINDIA', '3PLAND', '5PAISA', '63MOONS', '8KMILES', 'A2ZINFRA', 'AARTIDRUGS', 'AARTIIND', 'AARVEEDEN', 'AAVAS', 'ABAN', 'ABB', 'ABBOTINDIA', 'ABCAPITAL', 'ABFRL', 'ABMINTLTD', 'ABSLBANETF', 'ABSLNN50ET', 'ACC', 'ACCELYA', 'ACE', 'ADANIENT', 'ADANIGAS', 'ADANIGREEN', 'ADANIPORTS', 'ADANIPOWER', 'ADANITRANS', 'ADFFOODS', 'ADHUNIKIND', 'ADLABS', 'ADORWELD', 'ADROITINFO', 'ADSL', 'ADVANIHOTR', 'ADVENZYMES', 'AEGISCHEM', 'AFFLE', 'AGARIND', 'AGCNET', 'AGRITECH', 'AGROPHOS', 'AHLEAST', 'AHLUCONT', 'AHLWEST', 'AIAENG', 'AIONJSW', 'AIRAN', 'AJANTPHARM', 'AJMERA', 'AKASH', 'AKSHARCHEM', 'AKSHOPTFBR', 'AKZOINDIA', 'ALANKIT', 'ALBERTDAVD', 'ALBK', 'ALCHEM', 'ALEMBICLTD', 'ALICON', 'ALKALI', 'ALKEM', 'ALKYLAMINE', 'ALLCARGO', 'ALLSEC', 'ALMONDZ', 'ALOKINDS', 'ALPA', 'ALPHAGEO', 'ALPSINDUS', 'AMARAJABAT', 'AMBER', 'AMBIKCO', 'AMBUJACEM', 'AMDIND', 'AMJLAND', 'AMRUTANJAN', 'ANANTRAJ', 'ANDHRABANK', 'ANDHRACEMT', 'ANDHRSUGAR', 'ANDPAPER', 'ANIKINDS', 'ANKITMETAL', 'ANSALAPI', 'ANSALHSG', 'ANTGRAPHIC', 'ANUP', 'APARINDS', 'APCL', 'APCOTEXIND', 'APEX', 'APLAPOLLO', 'APLLTD', 'APOLLO', 'APOLLOHOSP', 'APOLLOPIPE', 'APOLLOTYRE', 'APOLSINHOT', 'APTECHT', 'ARCHIDPLY', 'ARCHIES', 'ARCOTECH', 'ARENTERP', 'ARIES', 'ARIHANT', 'ARIHANTSUP', 'ARMANFIN', 'AROGRANITE', 'ARROWGREEN', 'ARSHIYA', 'ARSSINFRA', 'ARTEMISMED', 'ARVIND', 'ARVINDFASN', 'ARVSMART', 'ASAHIINDIA', 'ASAHISONG', 'ASAL', 'ASALCBR', 'ASHAPURMIN', 'ASHIANA', 'ASHIMASYN', 'ASHOKA', 'ASHOKLEY', 'ASIANHOTNR', 'ASIANPAINT', 'ASIANTILES', 'ASPINWALL', 'ASTEC', 'ASTERDM', 'ASTRAL', 'ASTRAMICRO', 'ASTRAZEN', 'ASTRON', 'ATFL', 'ATLANTA', 'ATLASCYCLE', 'ATNINTER', 'ATUL', 'ATULAUTO', 'AUBANK', 'AURIONPRO', 'AUROPHARMA', 'AUSOMENT', 'AUTOAXLES', 'AUTOIND', 'AUTOLITIND', 'AVADHSUGAR', 'AVANTIFEED', 'AVTNPL', 'AXISBANK', 'AXISCADES', 'AXISGOLD', 'AXISNIFTY', 'AYMSYNTEX', 'BAGFILMS', 'BAJAJ-AUTO', 'BAJAJCON', 'BAJAJELEC', 'BAJAJFINSV', 'BAJAJHIND', 'BAJAJHLDNG', 'BAJFINANCE', 'BALAJITELE', 'BALAMINES', 'BALAXI', 'BALKRISHNA', 'BALKRISIND', 'BALLARPUR', 'BALMLAWRIE', 'BALPHARMA', 'BALRAMCHIN', 'BANARBEADS', 'BANARISUG', 'BANCOINDIA', 'BANDHANBNK', 'BANG', 'BANKBARODA', 'BANKBEES', 'BANKINDIA', 'BANSWRAS', 'BARTRONICS', 'BASF', 'BASML', 'BATAINDIA', 'BAYERCROP', 'BBL', 'BBTC', 'BCG', 'BCP', 'BDL', 'BEARDSELL', 'BEDMUTHA', 'BEL', 'BEML', 'BEPL', 'BERGEPAINT', 'BFINVEST', 'BFUTILITIE', 'BGLOBAL', 'BGRENERGY', 'BHAGERIA', 'BHAGYANGR', 'BHAGYAPROP', 'BHANDARI', 'BHARATFORG', 'BHARATGEAR', 'BHARATRAS', 'BHARATWIRE', 'BHARTIARTL', 'BHEL', 'BIGBLOC', 'BIL', 'BILENERGY', 'BINANIIND', 'BINDALAGRO', 'BIOCON', 'BIOFILCHEM', 'BIRLACABLE', 'BIRLACORPN', 'BIRLAMONEY', 'BIRLATYRE', 'BLBLIMITED', 'BLISSGVS', 'BLKASHYAP', 'BLS', 'BLUEBLENDS', 'BLUECHIP', 'BLUECOAST', 'BLUEDART', 'BLUESTARCO', 'BODALCHEM', 'BOMDYEING', 'BORORENEW', 'BOSCHLTD', 'BPCL', 'BPL', 'BRFL', 'BRIGADE', 'BRITANNIA', 'BRNL', 'BROOKS', 'BSE', 'BSL', 'BSLGOLDETF', 'BSLNIFTY', 'BSOFT', 'BURNPUR', 'BUTTERFLY', 'BVCL', 'BYKE', 'CADILAHC', 'CALSOFT', 'CAMLINFINE', 'CANBK', 'CANDC', 'CANFINHOME', 'CANTABIL', 'CAPACITE', 'CAPLIPOINT', 'CAPTRUST', 'CARBORUNIV', 'CAREERP', 'CARERATING', 'CASTEXTECH', 'CASTROLIND', 'CCHHL', 'CCL', 'CDSL', 'CEATLTD', 'CEBBCO', 'CELEBRITY', 'CELESTIAL', 'CENTENKA', 'CENTEXT', 'CENTRALBK', 'CENTRUM', 'CENTUM', 'CENTURYPLY', 'CENTURYTEX', 'CERA', 'CEREBRAINT', 'CESC', 'CESCVENT', 'CGCL', 'CGPOWER', 'CHALET', 'CHAMBLFERT', 'CHEMBOND', 'CHEMFAB', 'CHENNPETRO', 'CHOLAFIN', 'CHOLAHLDNG', 'CIGNITITEC', 'CIMMCO', 'CINELINE', 'CINEVISTA', 'CIPLA', 'CKFSL', 'CLEDUCATE', 'CLNINDIA', 'CMICABLES', 'CNOVAPETRO', 'COALINDIA', 'COCHINSHIP', 'COFFEEDAY', 'COLPAL', 'COMPINFO', 'COMPUSOFT', 'CONCOR', 'CONFIPET', 'CONSOFINVT', 'CONTROLPR', 'CORALFINAC', 'CORDSCABLE', 'COROMANDEL', 'CORPBANK', 'COSMOFILMS', 'COUNCODOS', 'COX&KINGS', 'CPSEETF', 'CREATIVE', 'CREDITACC', 'CREST', 'CRISIL', 'CROMPTON', 'CSBBANK', 'CTE', 'CUB', 'CUBEXTUB', 'CUMMINSIND', 'CUPID', 'CURATECH', 'CYBERMEDIA', 'CYBERTECH', 'CYIENT', 'DAAWAT', 'DABUR', 'DALBHARAT', 'DALMIASUG', 'DAMODARIND', 'DATAMATICS', 'DBCORP', 'DBL', 'DBREALTY', 'DBSTOCKBRO', 'DCAL', 'DCBBANK', 'DCM', 'DCMNVL', 'DCMSHRIRAM', 'DCW', 'DECCANCE', 'DEEPAKFERT', 'DEEPAKNTR', 'DEEPIND', 'DELTACORP', 'DELTAMAGNT', 'DEN', 'DENORA', 'DFMFOODS', 'DGCONTENT', 'DHAMPURSUG', 'DHANBANK', 'DHANUKA', 'DHARSUGAR', 'DHFL', 'DHUNINV', 'DIAMONDYD', 'DIAPOWER', 'DICIND', 'DIGISPICE', 'DIGJAMLTD', 'DISHTV', 'DIVISLAB', 'DIXON', 'DLF', 'DLINKINDIA', 'DMART', 'DNAMEDIA', 'DOLAT', 'DOLLAR', 'DOLPHINOFF', 'DONEAR', 'DPSCLTD', 'DPWIRES', 'DQE', 'DREDGECORP', 'DRREDDY', 'DSSL', 'DTIL', 'DUCON', 'DVL', 'DWARKESH', 'DYNAMATECH', 'DYNPRO', 'EASTSILK', 'EASUNREYRL', 'EBANK', 'EBBETF0423', 'EBBETF0430', 'ECLERX', 'EDELWEISS', 'EDL', 'EDUCOMP', 'EICHERMOT']



def write_quotes(t):
    for key,value in t.items():
        if value is None:
            t[key]=''

    json_body=[{"measurement":"Trade","fields":{"symbol":t['symbol'],"symbol_id":t['symbol_id'],"day_high":t['day_high'],"day_low":t['day_low'],"day_low":t['day_low'],"prev_day_close":t['prev_day_close'],"prev_day_oi":t['prev_day_oi'],"oi":t['oi'],"change":t['change'],"volume":t['volume'],"ltp":t['ltp'],"atp":t['atp'],"ttq":t['ttq'],"tick_seq":t['tick_seq'],"best_bid_price":t['best_bid_price'],"best_bid_qty":t['best_bid_qty'],"best_ask_price":t['best_ask_price'],"best_ask_qty":t['best_ask_qty']}}]
    ret = conn.write_points(json_body)
    return ret

req_ids = td_app.start_live_data(symbols)

live_data_objs = {}
time.sleep(1)
for req_id in req_ids:
    #print(req_id)
    live_data_objs[req_id] = deepcopy(td_app.live_data[req_id])
    #print(live_data_objs)
    #print(live_data_objs[req_id])
    #print(f'touchlinedata -> {td_app.touchline_data[req_id]}')
while True:
    for req_id in req_ids:
        if not td_app.live_data[req_id] == live_data_objs[req_id]:
            #print(td_app.live_data[req_id].__dict__)
            ret = write_quotes(td_app.live_data[req_id].__dict__)
            logging.info(ret)
            live_data_objs[req_id] = deepcopy(td_app.live_data[req_id])
