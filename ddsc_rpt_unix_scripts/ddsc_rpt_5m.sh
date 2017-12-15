#!/usr/bin/ksh

SCRIPT_HOME=/home/oracle/dba_report
##########PORFILE#############
. ${SCRIPT_HOME}/ora11g.env
##############################
CUSTID=A9999;export CUSTID
HOST=`hostname`;export HOST
OPTIME=`date '+%Y%m%d%H%M'`;export OPTIME
LOCKFILE=${SCRIPT_HOME}/run5m.lock
PROGRAM_NAME=ddsc_rpt_5m.sh

DICT=`date +%m`_ddsc_dict.his
LIB=`date +%m`_ddsc_lib.his
PGA=`date +%m`_ddsc_pga.his
SESS=`date +%m`_ddsc_sess.his
#SGACOM=`date +%m`_ddsc_sgacom.his
SHARE=`date +%m`_ddsc_share.his
SYSSTAT=`date +%m`_ddsc_sysstat.his
UNDO=`date +%m`_ddsc_undo.his
TEMP=`date +%m`_ddsc_temp.his

check_process()
{
   count=$(ps -ef|grep ${PROGRAM_NAME} | grep -v "grep" |wc -l|awk ' { print $1 } ')
   if [ ${count} -gt 1 ]; then    
    echo "THERE IS ANOTHER PROCESS ${PROGRAM_NAME} RUNNING !!!!"  
      exit 1
   fi
}

if [[ -f ${LOCKFILE} ]]; then
check_process
fi

echo $$ >${LOCKFILE}
trap 'rm ${LOCKFILE};exit ' 1 2 3 5 15

cd ${SCRIPT_HOME}

$ORACLE_HOME/bin/sqlplus -S '/ as sysdba' @${SCRIPT_HOME}/ddsc_rpt_5m.sql $CUSTID $HOST $ORACLE_SID $OPTIME


cat ddsc_db_dict.txt    >> ${DICT}
cat ddsc_db_lib.txt     >> ${LIB}
cat ddsc_db_pga.txt     >> ${PGA}
cat ddsc_db_sess.txt    >> ${SESS}
#cat ddsc_db_sgacom.txt  >> ${SGACOM}
cat ddsc_db_share.txt   >> ${SHARE}
cat ddsc_db_sysstat.txt >> ${SYSSTAT}
cat ddsc_db_undo.txt    >> ${UNDO}
cat ddsc_db_temp.txt    >> ${TEMP}

rm -rf ${LOCKFILE}