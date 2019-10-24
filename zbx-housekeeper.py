#!/usr/bin/python3

import mysql.connector
from mysql.connector import pooling
from mysql.connector import errorcode
import collections
import time
import datetime
from multiprocessing import Process, Manager
import config as cfg
import sys
import os


list_items = collections.OrderedDict()
list_hk = collections.OrderedDict()
items_all_count=0
limit_count=10000
try:
    del_limit=cfg.DEL_LIMIT
except NameError:
    del_limit=5000
try:
    del_part=cfg.DEL_PART
except NameError:
    del_part:10
try:
    DEBUG=cfg.DEBUG
except NameError:
    DEBUG=0

list_type={'history': '(0)', 'history_str': '(1)','history_text': '(4)','history_log': '(2)', 'history_uint': '(3)', 'trends': '(0)', 'trends_uint': '(0,3)'}

def get_count_items(mysql_pool,table):
    global items_all_count
    try:
        conn = mysql_pool.get_connection()
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(DISTINCT IFNULL(id.itemid,it.itemid)) FROM items as it LEFT JOIN item_discovery id ON id.parent_itemid=it.itemid WHERE value_type IN {};".format(list_type[table]))
            for (count,) in cursor:
                print("Items count is {}".format(count))
            items_all_count=count
            cursor.close()
            conn.close()
    except mysql.connector.Error as err:
        print("Failed get count items {}".format(err))
        exit(1)

def get_all_items(mysql_pool,table):
    global list_items
    j_count=0
    try:
        conn = mysql_pool.get_connection()
        if conn.is_connected():
            cursor = conn.cursor()
            while(j_count <= items_all_count):
                query="SELECT DISTINCT IFNULL(id.itemid,it.itemid) as itemid,it.{} FROM items as it LEFT JOIN item_discovery id ON id.parent_itemid=it.itemid WHERE value_type IN {} LIMIT {},{};".format(table.split('_',1)[0],list_type[table],j_count,limit_count)
                cursor.execute(query)
                for (itemid,history) in cursor:
                    if isinstance(history, (bytes, bytearray)):
                        history=history.decode()
                    if history == 0:
                        history=history+'d'
                    history=history.replace("w"," WEEK",1)
                    history=history.replace("d"," DAY",1)
                    history=history.replace("h"," HOUR",1)
                    list_items[str(itemid)]=history
                j_count +=limit_count
            cursor.close()
            conn.close()
    except mysql.connector.Error as err:
        print("Failed get items {}".format(err))
        exit(1)

def get_all_items_table_housekeeper(mysql_pool,table,list,flag):
    hcount=0
    j_count=0
    try:
        conn = mysql_pool.get_connection()
        if conn.is_connected():
            cursor = conn.cursor()
            query="SELECT COUNT(*) FROM housekeeper WHERE tablename='{}';".format(table,)
            cursor.execute(query)
            for (count,) in cursor:
                print("Items {} count in housekeeper is {}".format(table,count))
            hcount=count
            cursor.close()
            cursor = conn.cursor()
            while(j_count <= hcount):
                query="SELECT `value`,housekeeperid FROM housekeeper WHERE tablename='{}' LIMIT {},{};".format(table,j_count,limit_count)
                cursor.execute(query)
                for (itemid,hkid) in cursor:
                    if flag == 1:
                        list[str(itemid)]="7 DAY"
                    elif flag == 0:
                        list[str(itemid)]=hkid
                j_count +=limit_count
            cursor.close()
            conn.close()
    except mysql.connector.Error as err:
        print("Failed get items {}".format(err))
        exit(1)

def del_items_from_housekeeper(mysql_poll,list,table):
    try:
        conn = mysql_poll.get_connection()
        if  not conn.is_connected():
            conn = mysql_poll.get_connection()

        if conn.is_connected():
            cursor=conn.cursor()
            for itemid in list:
                query="SELECT COUNT(*) FROM {} WHERE `itemid`='{}'".format(table,itemid)
                cursor.execute(query)
                for (count,) in cursor:
                    if (count >0):
                        if DEBUG == 1:
                            print("{} #Items {} in {} is {}  hid({})".format(datetime.datetime.now(),itemid,table,count,list[itemid]))
                    elif (count == 0):
                        tic=time.time()
                        query="DELETE FROM housekeeper WHERE housekeeperid='{}' AND tablename='{}' AND `value`='{}';".format(list[itemid],table,itemid)
                        cursor.execute(query)
                        toc=time.time()
                        if DEBUG == 1:
                            print("{} #Delete Item {} of {} in housekeeper({}) is {} sec".format(datetime.datetime.now(),itemid,table,list[itemid],toc-tic))
            cursor.close()
            conn.close()
    except mysql.connector.Error as err:
        print("{} Failed get or rem count in {} item({}) {} ".format(datetime.datetime.now(),table,itemid,err))
        

def show_list_items():
    global list_items
    print("Len is {}".format(len(list_items)))

def get_rem_count_tables(conn,list,table):
    try:
        if conn.is_connected():
            it = 0
            for itemid,inter in list.items():
                cursor = conn.cursor()
                it+=1
                query="SELECT COUNT(itemid) FROM {} WHERE itemid={} AND FROM_UNIXTIME(clock) < (NOW() - INTERVAL {});"
                query=query.format(table,itemid,inter)
                tic=time.time()
                cursor.execute(query)
                toc=time.time()
                for (count,) in cursor:
                    if (count >0):
                        if (count <=del_limit):
                            c_del=count
                        elif (count // del_part < del_limit):
                            c_del=del_limit
                        else:
                            c_del=count // del_part
                        if DEBUG == 1:
                            print("{} {}#{}({}) Items {} get in {} count is {} is time {} sec".format(datetime.datetime.now(),os.getpid(),it,len(list),itemid,table,count,round(toc-tic,3)))
                        query_rem="DELETE FROM {} WHERE itemid={} AND FROM_UNIXTIME(clock) < (NOW() - INTERVAL {}) LIMIT {};"
                        query_rem=query_rem.format(table,itemid,inter,c_del)
                        tic=time.time()
                        cursor.execute(query_rem)
                        toc=time.time()
                        if DEBUG == 1:
                            print("{} {}#{}({}) Items {} delete in {} an {} count is time {} sec".format(datetime.datetime.now(),os.getpid(),it,len(list),itemid,table,c_del,round(toc-tic,3)))
                cursor.close()
        else:
            print("Failed get in {} items".format(table))
    except mysql.connector.Error as err:
        print("Failed get or rem count in {} item({}) {}".format(table,itemid,err,))

try:
  if (len(sys.argv)<2):
        print("Error: Not table in argv")
        exit(1)
  start = time.time()
  table = str(sys.argv[1])
  conn_pool = pooling.MySQLConnectionPool(pool_name="zbx_poll",pool_size=5,autocommit=True,connection_timeout=300,host=cfg.DBHOST,database=cfg.DBNAME,user=cfg.DBUSER,password=cfg.DBPASS)
  get_count_items(conn_pool,table)
  get_all_items(conn_pool,table)
  get_all_items_table_housekeeper(conn_pool,table,list_hk,0)
  del_items_from_housekeeper(conn_pool,list_hk,table)
  get_all_items_table_housekeeper(conn_pool,table,list_items,1)
  show_list_items()
  
  print("{} Start task delete from {} ".format(datetime.datetime.now(),table))
  conn = conn_pool.get_connection()
  cursor = conn.cursor()
  cursor.execute("SHOW CREATE TABLE {};".format(table))

  conn = conn_pool.get_connection()
  get_rem_count_tables(conn,list_items,table)

  end = time.time()
  print("{} End task delete from {} at {} sec".format(datetime.datetime.now(),table,end-start))

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database or table does not exist")
    else:
        print(err)
