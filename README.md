# Zabbix Housekeeper

1. Setup script

   - cp ./config.py.default ./config.py

2. Set Vars in config.py

   - DBHOST="mysql-zabbix"
   - DBNANE="zabbix"
   - DBUSER=""
   - DBPASS=""
   - DEL_LIMIT=15000 - defauts limit on delete one item
   - DEL_PART=10 - if many items to count items devide on var

3. Cron setup

   - #ZBX HOUSEKEEPER
   - */30 * * * 1-5 /opt/zabbix-housekeeper/cron_zbx_housekeeper.sh >> /var/log/zbx_housekeeper.log
   - 0 4 * * 6 /opt/zabbix-housekeeper/cron_zbx_housekeeper_optimize.sh >> /var/log/zbx_housekeeper_optimize.log
   - 30 5 * * 5 /opt/zabbix-housekeeper/cron_zbx_partition.sh >> /var/log/zbx_housekeeper_partition.log

4. Scritps
   - /opt/zabbix-housekeeper/cron_zbx_housekeeper.sh and zbx-housekeeper.py - Delete values of items
   - /opt/zabbix-housekeeper/cron_zbx_housekeeper_optimize.sh - Local Optimize tables of MySQL
   - /opt/zabbix-housekeeper/cron_zbx_partition.sh and zbx-drop-emty-partitions.py - New create and drop old empy partition
