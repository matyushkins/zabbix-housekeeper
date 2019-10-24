#!/bin/bash

list_tables=("history"
"history_log"
"history_str"
"history_text"
"history_uint"
"trends"
"trends_uint"
)

PATH=/sbin:/bin:/usr/sbin:/usr/bin

for TABLE in ${list_tables[*]}; do

    if `ps aux | grep -v "grep" | grep "python3" | grep "zbx-housekeeper.py ${TABLE}$" > /dev/null` ; then
        echo "Proccess zbx_housekeeper.py on ${TABLE} already run."
        echo "Skip."
    else
        echo `date +%Y-%m-%d-%H-%M-%S`" Proccess zbx_housekeeper.py started on ${TABLE}."
        /usr/bin/python3 `dirname "$0"`/zbx-housekeeper.py ${TABLE} >> /var/log/zbx_housekeeper_${TABLE}.log & 
    fi
done
