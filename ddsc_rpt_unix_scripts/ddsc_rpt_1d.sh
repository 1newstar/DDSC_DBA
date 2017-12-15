#!/usr/bin/ksh

SCRIPT_HOME=/home/oracle/dba_report
##########PROFILE#############
. ${SCRIPT_HOME}/ora11g.env
##############################
CUSTID=A9999;export CUSTID
HOST=`hostname`;export HOST
OPTIME=`date '+%Y%m%d%H%M'`;export OPTIME
LOCKFILE=${SCRIPT_HOME}/runld.lock
PROGRAM_NAME=ddsc_rpt_1d.sh
RETEN_DAY=120

BAK=`date +%m`_ddsc_bak.his
DF=`date +%m`_ddsc_df.his
IDX=`date +%m`_ddsc_idx.his
INST=`date +%m`_ddsc_inst.his
LOGSW=`date +%m`_ddsc_logsw.his
OBJ=`date +%m`_ddsc_obj.his
PAR=`date +%m`_ddsc_par.his
SIZE=`date +%m`_ddsc_size.his
TBS=`date +%m`_ddsc_tbs.his
SEG=`date +%m`_ddsc_seg.his
SGARS=`date +%m`_ddsc_sgars.his

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

$ORACLE_HOME/bin/sqlplus -S '/ as sysdba' @${SCRIPT_HOME}/ddsc_rpt_1d.sql $CUSTID $HOST $ORACLE_SID $OPTIME



cat ddsc_db_bak.txt     >> ${BAK}
cat ddsc_db_df.txt      >> ${DF}
cat ddsc_db_idx.txt     >> ${IDX}
cat ddsc_db_inst.txt    >> ${INST}
cat ddsc_db_logsw.txt   >> ${LOGSW}
cat ddsc_db_obj.txt     >> ${OBJ}
cat ddsc_db_par.txt     >> ${PAR}
cat ddsc_db_size.txt    >> ${SIZE}
cat ddsc_db_tbs.txt     >> ${TBS}
cat ddsc_db_seg.txt     >> ${SEG}
cat ddsc_db_sgars.txt   >> ${SGARS}

##GET ORA-ERROR
${SCRIPT_HOME}/getora.sh $ORACLE_SID


rm -rf ${LOCKFILE}

find ${SCRIPT_HOME} -name "*.his" -type f -mtime +${RETEN_DAY} -exec rm -rf {} \;