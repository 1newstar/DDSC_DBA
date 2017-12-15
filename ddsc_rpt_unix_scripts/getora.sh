#!/usr/bin/ksh

SCRIPT_HOME=/home/oracle/dba_report

##########PORFILE#############
. ${SCRIPT_HOME}/ora11g.env
##############################
CUST_ID=A0001
HOST=`hostname`;export HOSTNAME
OPTIME=`date '+%Y%m%d%H%M'`;export OPTIME
LOCKFILE=${SCRIPT_HOME}/${1}_getora.lock
PROGRAM_NAME=getora.sh


check_process()
{
   count=$(ps -ef|grep ${PROGRAM_NAME} | grep -v "grep" |wc -l|awk ' { print $1 } ')
   if [ ${count} -gt 1 ]; then      
    echo "THERE IS ANOTHER PROCESS ${PROGRAM_NAME} RUNNING !!!!"
      exit 1
   fi
}

get_dbinfo()
{
ORACLE_SID=$1
export ORACLE_SID
export DBNAME=$ORACLE_SID
sqlplus -S "/ as sysdba"<<EOF >/dev/null
set heading off
set feedback off
col instance_name for a10
col instance_number for 99 
col dbid format 999999999999999
set linesize 140
set echo off
spool ${1}_dbinfo.txt
select 'dbinfo '||v.instance_name,v.instance_number,(select dbid from v\$database) "DBID" from v\$instance v;
spool off;
EOF
>/dev/null

INST_NAME=`awk '/dbinfo/ {print $2}' ${1}_dbinfo.txt`;export INST_NAME
INST_NUM=`awk '/dbinfo/ {print $3}' ${1}_dbinfo.txt`;export INST_NUM
DBID=`awk '/dbinfo/ {print $4}' ${1}_dbinfo.txt`;export DBID
}

ALERT_LOG()
{
get_dbinfo $1
grep "ORA-" ${1}_dbinfo.txt >/dev/null
alert_rc=$?
if [[ ${alert_rc} -ne 0 ]]; then
ALERT_LOG_TEMP=${SCRIPT_HOME}/${INST_NAME}_${INST_NUM}_alert.tmp
ALERT_LOG=${SCRIPT_HOME}/`date +%m`_${INST_NAME}_${INST_NUM}_alert.his
ORACLE_SID=$1
export ORACLE_SID
export DBNAME=$ORACLE_SID
sqlplus -S "/ as sysdba"<<EOF >/dev/null
set heading off
set feedback off
col value format a90
col name format a40
set linesize 140
set serveroutput on
set echo off
spool ${INST_NAME}_${INST_NUM}_alertdir.txt
select 'get '||value from v\$parameter where name in ('background_dump_dest','instance_name') order by name;
spool off;
EOF
>/dev/null

rc=$?
if [[ ${rc} == 0 ]]; then
#Alert_log Location
grep  -v "SQL>" ${INST_NAME}_${INST_NUM}_alertdir.txt |grep "get" |awk 'NR==1 {printf $2 "/alert_"} NR==2 {print $2".log"}' >${INST_NAME}_${INST_NUM}_alertfile.log

export DB_ALERT=`cat ${INST_NAME}_${INST_NUM}_alertfile.log`

if [[ ! -f mon_${INST_NAME}_${INST_NUM}_alert.txt ]]; then
awk \
'BEGIN {  \
   first = 0;  \
}  \
{  \
   tim=substr($0, 1, 4);  \
   if ( tim == "Mon " || tim == "Tue " || tim == "Wed " || tim == "Thu " ||  \
        tim == "Fri " || tim == "Sat " || tim == "Sun " ) {  \
      new_time = $0;  \
      first = 0;  \
   }  \
   if ( $0 ~ /ORA-/ ) {  \
      if (new_time != "" && first == 0) {  \
         first = 1;  \
         printf("%s\n", new_time); \
      } \
      printf("%s\n", $0); \
   } \
}' \
$DB_ALERT  >mon_${INST_NAME}_${INST_NUM}_alert.txt


cp mon_${INST_NAME}_${INST_NUM}_alert.txt last_mon_${INST_NAME}_${INST_NUM}_alert.txt
#diff   mon_${INST_NAME}_${INST_NUM}_alert.txt last_mon_${INST_NAME}_${INST_NUM}_alert.txt  |sed '1d; s/< //g'  >${ALERT_LOG_TEMP}
#tail -10  mon_${INST_NAME}_${INST_NUM}_alert.txt > ${ALERT_LOG_TEMP}
grep_line=`egrep -n "Mon|Thu|Wed|Tue|Fri|Sat|Sun"  mon_${INST_NAME}_${INST_NUM}_alert.txt |tail -1 |awk -F":" '{print $1}' `
 
  if [[ -n ${grep_line} ]]; then
  sed -n "${grep_line},\$p" mon_${INST_NAME}_${INST_NUM}_alert.txt >${ALERT_LOG_TEMP}
  else
  touch ${ALERT_LOG_TEMP}
  fi

else

cp mon_${INST_NAME}_${INST_NUM}_alert.txt last_mon_${INST_NAME}_${INST_NUM}_alert.txt

awk \
'BEGIN {  \
   first = 0;  \
}  \
{  \
   tim=substr($0, 1, 4);  \
   if ( tim == "Mon " || tim == "Tue " || tim == "Wed " || tim == "Thu " ||  \
        tim == "Fri " || tim == "Sat " || tim == "Sun " ) {  \
      new_time = $0;  \
      first = 0;  \
   }  \
   if ( $0 ~ /ORA-/ ) {  \
      if (new_time != "" && first == 0) {  \
         first = 1;  \
         printf("%s\n", new_time); \
      } \
      printf("%s\n", $0); \
   } \
}' \
$DB_ALERT  > mon_${INST_NAME}_${INST_NUM}_alert.txt


    eval `du -sm mon_${INST_NAME}_${INST_NUM}_alert.txt last_mon_${INST_NAME}_${INST_NUM}_alert.txt |awk '{print "export size"NR"="$1}'`
   if [[ $size2 -gt $size1 ]]; then
    tail -10  mon_${INST_NAME}_${INST_NUM}_alert.txt > ${ALERT_LOG_TEMP}
    else
     diff   mon_${INST_NAME}_${INST_NUM}_alert.txt last_mon_${INST_NAME}_${INST_NUM}_alert.txt  |sed '1d; s/< //g' |grep -v ">"  >${ALERT_LOG_TEMP}
   fi
 fi
 fi
fi

awk ' \
BEGIN { \
   v_next = 0;printf("\n") \
} \
length($0) == 24 || length($0) == 30 { \
    if (v_next != 0) printf("\n"); \
    printf(CUST_ID "|" HOST "|" INST_NAME "|"INST_NUM"|"DBID"|" "%s |", $0); \
} \
length($0) != 24 && length($0) != 30 { \
   printf("%s ", $0); \
   v_next = 1; \
}' CUST_ID=${CUST_ID} HOST=$HOST INST_NAME=${INST_NAME} INST_NUM=${INST_NUM} DBID=$DBID  ${ALERT_LOG_TEMP} >>${ALERT_LOG}.tmp

awk '/./{ str1=$0; gsub(" TAIST", "",str1) ;print str1 }' ${ALERT_LOG}.tmp >${ALERT_LOG}
}

########### MAIN ###################

if [[ $# != 1 ]]; then
echo "
###########################################
# EXAMPLE:                                #
#                                         #
#         getora.sh sid                   #
#                                         #
###########################################
"
else
    
    if [ -f ${LOCKFILE} ]; then
    check_process
    fi
     
    echo $$ > ${LOCKFILE} 
    ALERT_LOG $1
    rm -rf ${LOCKFILE}
    trap 'rm ${LOCKFILE};exit ' 1 2 3 5 15
fi

