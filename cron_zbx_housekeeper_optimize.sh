#!/bin/bash

date

config=$(dirname "$0")/config.py
source $config

list_tables=(
"history"
"history_log"
"history_str"
"history_text"
"history_uint"
"trends"
"trends_uint"
)

PATH=/sbin:/bin:/usr/sbin:/usr/bin

for TABLE in ${list_tables[*]}; do
    echo "Start optimize table ${TABLE} on ${SRV}"
    mysql -h${DBHOST} -u${DBUSER} -p${DBPASS} ${DBNAME} -e "OPTIMIZE TABLE ${TABLE}"
    echo "End optimize table ${TABLE} on ${SRV}"
done
