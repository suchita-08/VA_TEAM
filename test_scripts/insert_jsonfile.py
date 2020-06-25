import sys
import time
import logging
import json
import pandas as pd
from pandas.io.json import json_normalize
from ifxdb_wrapper import ifxdb
from datetime import datetime


ifx = ifxdb()

 
host = 'localhost'
port = 8086
user = ''
password = ''
dbname = 'TestDB'
c = ifx.connect(host, port, user, password, dbname)
print("--> "+str(c))

 

ret = c.create_database(dbname)
logging.info("create database "+str(ret))
 

 

def main(argv):
   
    ##reading the json value using pandas

 

    df=pd.read_json('C:/work/Backend/Data.json')

 

    ## splitting the nested json ohlc to different columns

 

    df['open']=[row['open'] for row in df['ohlc']]
    df['high']=[row['high'] for row in df['ohlc']]
    df['low']=[row['low'] for row in df['ohlc']]
    df['close']=[row['close'] for row in df['ohlc']]

 


    ## Now dropping the nested ohlc columns from the dataset

 

    df=df.drop('ohlc',axis=1)

 

 

    for row_index,row in df.iterrows():
      

 

        
        json_body=[{"measurement":"Trade","fields":{"tradable":row['tradable'],"mode":row['mode'],"instrument_token":row['instrument_token'],
                    "last_price":row['last_price'],"last_quantity":row['last_quantity'],"average_price":row['average_price'],"volume":row['volume'],
                    "buy_quantity":row['buy_quantity'],"sell_quantity":row['sell_quantity'],"change":row['change'],"open":row['open'],
                    "high":row['high'],"low":row['low'],"close":row['close']}}]

        print (json_body)

       ## writing the data to the influxdb 
        
        c.write_points(json_body)


if __name__ ==  '__main__':
    print(sys.argv[1:])
    main(sys.argv)