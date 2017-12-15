#!/bin/sh
. /home/ora9i/.profile
CUSTID=A0000;export CUSTOMER
HOST=`hostname`;export HOSTNAME

OPTIME=`date '+%Y%m%d%H%M'`;export OPTIME
OPDATE=`date '+%Y%m%d'`;export OPDATE

$ORACLE_HOME/bin/sqlplus -s perfstat/perfstat @/home/ora9i/fred/top_sql_gets.sql $CUSTID $HOST $ORACLE_SID
