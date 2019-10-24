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

echo "Start partition tables on ${DBHOST}"
mysql -h${DBHOST} -u${DBUSER} -p${DBPASS} ${DBNAME} -e "CALL partition_maintenance_all('${DBNAME}');"
echo "End partition tables on ${DBHOST}"

for TABLE in ${list_tables[*]}; do
    echo `date +%Y-%m-%d-%H-%M-%S`" Proccess zbx-drop-emty-partitions.py started on ${TABLE}."
    /usr/bin/python3 `dirname "$0"`/zbx-drop-emty-partitions.py ${TABLE} >> /var/log/zbx_drop_emty_partitions_${TABLE}.log
done
