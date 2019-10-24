#!/usr/bin/python3

import mysql.connector
from mysql.connector import pooling
from mysql.connector import errorcode
import collections
import time
import datetime
from multiprocessing import Process
import config as cfg
import sys
import re

def get_count_items(mysql_pool,table,part):
    try:
        conn = mysql_pool.get_connection()
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM {} PARTITION ({});".format(table,part))
            for (count,) in cursor:
                print("{} #Partition {} in {} table is count items {}".format(datetime.datetime.now(),part,table,count))
                if count==0:
                    drop_partition(mysql_pool,table,part)
            cursor.close()
            conn.close()
    except mysql.connector.Error as err:
        print("Failed get count items {}".format(err))

def drop_partition(mysql_pool,table,part):
    try:
        conn = mysql_pool.get_connection()
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("ALTER TABLE {} DROP PARTITION {};".format(table,part))
            print("{} #Partition {} in {} table is drop".format(datetime.datetime.now(),part,table))
            cursor.close()
            conn.close()
    except mysql.connector.Error as err:
        print("Failed drop partition {} in {} : {}".format(part,table,err))



try:
  if (len(sys.argv)<2):
        print("Error: Not table in argv")
        exit(1)
  start = time.time()
  conn_pool = pooling.MySQLConnectionPool(pool_name="zbx_poll",pool_size=10,autocommit=True,pool_reset_session=True,host=cfg.DBHOST,database=cfg.DBNAME,user=cfg.DBUSER,password=cfg.DBPASS)
  
  print("{} Start task drop partitions from {} ".format(datetime.datetime.now(),str(sys.argv[1])))
  conn = conn_pool.get_connection()
  cursor = conn.cursor()
  cursor.execute("SHOW CREATE TABLE {};".format(str(sys.argv[1])))
  for (table,ctable) in cursor:
        for part in re.findall('(p\d+)',ctable)[:len(re.findall('(p\d+)',ctable))-30]:            
            get_count_items(conn_pool,str(sys.argv[1]),part)

  end = time.time()
  print("{} End task drop partitions from {} at {} sec".format(datetime.datetime.now(),str(sys.argv[1]),end-start))

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database or table does not exist")
    else:
        print(err)
