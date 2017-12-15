SET HEADING OFF
SET ECHO OFF
SET FEEDBACK OFF
SET PAGES 0
SET LINESIZE 32766
SET LONG 1999999
SET TRIMOUT ON
SET TRIMSPOOL ON
SET NEWPAGE NONE
SET SQLBLANKLINES OFF
SET TRIMS ON
SET TIMING OFF
SET SERVEROUTPUT ON
SET VERIFY OFF
COLUMN SQL_TEXT FORMAT A32766 WORD WRAPPED

COL SNAP_ID NEW_VALUE V_BID
SELECT (SNAP_ID - 1) SNAP_ID
FROM DBA_HIST_SNAPSHOT
WHERE TO_CHAR(BEGIN_INTERVAL_TIME,'YYYY/MM/DD HH24') = TO_CHAR(SYSDATE-(60/1440),'YYYY/MM/DD HH24');

COL SNAP_ID NEW_VALUE V_EID
SELECT SNAP_ID
FROM DBA_HIST_SNAPSHOT
WHERE TO_CHAR(BEGIN_INTERVAL_TIME,'YYYY/MM/DD HH24') = TO_CHAR(SYSDATE-(60/1440),'YYYY/MM/DD HH24');

COL DBID NEW_VALUE V_DBID
SELECT DBID FROM V$DATABASE;

column "SNAP_TIME" NEW_VALUE END_SNAP_TIME
 SELECT to_char(END_INTERVAL_TIME,'yyyymmdd hh24mi') "SNAP_TIME"
    FROM DBA_HIST_SNAPSHOT B
   WHERE B.SNAP_ID = '&V_EID'
     AND B.DBID = '&V_DBID'
     AND B.INSTANCE_NUMBER = USERENV('INSTANCE');

spool DATA_DIR\top5_sql_elas.txt

select SAMPLE_TIME||'|'||ELAPSED_TIME||'|'||CPU_TIME||'|'||EXECUTIONS||'|'||ELAP_PER_EXEC||'|'||TOTAL_DB_TIME||'|'||SQL_ID||'|'||SQL_MODULE||'|'||DBMS_LOB.substr(SQL_TEXT,3000)||'|'
  from (select '&1|&2|&3|'||'&END_SNAP_TIME' SAMPLE_TIME,
               round(nvl((sqt.elap / 1000000), to_number(null)),2) ELAPSED_TIME,
               round(nvl((sqt.cput / 1000000), to_number(null)),2) CPU_TIME,
               sqt.exec EXECUTIONS,
               round(decode(sqt.exec,
                      0,
                      to_number(null),
                      (sqt.elap / sqt.exec / 1000000)),2) ELAP_PER_EXEC,
               round((100 *
               (sqt.elap / (SELECT (sum(e.value) - sum(b.value)) 
                             FROM DBA_HIST_SYS_TIME_MODEL e, DBA_HIST_SYS_TIME_MODEL b
                             WHERE e.SNAP_ID = '&V_EID'
                              AND e.DBID = '&V_DBID'
                             AND e.INSTANCE_NUMBER = USERENV('INSTANCE')
                             AND e.STAT_NAME = 'DB time'
                             and b.SNAP_ID = '&V_BID'
                             AND b.DBID = '&V_DBID'
                             AND b.INSTANCE_NUMBER =USERENV('INSTANCE')
                             AND b.STAT_NAME = 'DB time'))),1) TOTAL_DB_TIME,
               sqt.sql_id SQL_ID,
               to_clob(decode(sqt.module,
                              null,
                              null,
                              'Module: ' || sqt.module)) SQL_MODULE,
               nvl(st.sql_text, to_clob(' ** SQL Text Not Available ** ')) SQL_TEXT
          from (select sql_id,
                       max(module) module,
                       sum(elapsed_time_delta) elap,
                       sum(cpu_time_delta) cput,
                       sum(executions_delta) exec
                  from dba_hist_sqlstat
                 where dbid = '&V_DBID'
                   and instance_number = USERENV('INSTANCE')
                   and &V_BID < snap_id
                   and snap_id <= &V_EID
                 group by sql_id) sqt,
               dba_hist_sqltext st
         where st.sql_id(+) = sqt.sql_id
           and st.dbid(+) = '&V_DBID'
         order by nvl(sqt.elap, -1) desc, sqt.sql_id)
 where rownum <= 5
   and (rownum <=10 or TOTAL_DB_TIME > 1);
spool off
exit
