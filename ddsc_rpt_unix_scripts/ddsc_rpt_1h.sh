#!/usr/bin/ksh

SCRIPT_HOME=/home/oracle/dba_report

##########PORFILE#############
. ${SCRIPT_HOME}/ora11g.env
##############################
CUSTID=A9999;export CUSTID
HOST=`hostname`;export HOST
OPTIME=`date '+%Y%m%d%H%M'`;export OPTIME
LOCKFILE=${SCRIPT_HOME}/runlh.lock
PROGRAM_NAME=ddsc_rpt_1h.sh


TOPCPU=`date +%m`_ddsc_topcpu.his
TOPELAS=`date +%m`_ddsc_topelas.his
TOPEXEC=`date +%m`_ddsc_topexec.his
TOPPYRD=`date +%m`_ddsc_toppyrd.his
TOPGETS=`date +%m`_ddsc_topgets.his

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

$ORACLE_HOME/bin/sqlplus -S '/ as sysdba' @${SCRIPT_HOME}/top5_sql_cpu.sql  $CUSTID $HOST $ORACLE_SID
$ORACLE_HOME/bin/sqlplus -S '/ as sysdba' @${SCRIPT_HOME}/top5_sql_elas.sql $CUSTID $HOST $ORACLE_SID
$ORACLE_HOME/bin/sqlplus -S '/ as sysdba' @${SCRIPT_HOME}/top5_sql_exec.sql $CUSTID $HOST $ORACLE_SID
$ORACLE_HOME/bin/sqlplus -S '/ as sysdba' @${SCRIPT_HOME}/top5_sql_gets.sql $CUSTID $HOST $ORACLE_SID
$ORACLE_HOME/bin/sqlplus -S '/ as sysdba' @${SCRIPT_HOME}/top5_sql_read.sql $CUSTID $HOST $ORACLE_SID



cat top5_sql_cpu.txt    >> ${TOPCPU}
cat top5_sql_elas.txt   >> ${TOPELAS}
cat top5_sql_exec.txt   >> ${TOPEXEC}
cat top5_sql_gets.txt   >> ${TOPPYRD}
cat top5_sql_read.txt   >> ${TOPGETS}



rm -rf ${LOCKFILE}