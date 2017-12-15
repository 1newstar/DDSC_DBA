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

spool DATA_DIR\top5_sql_read.txt

select SAMPLE_TIME||'|'||PHYSICAL_READS||'|'||EXECUTIONS||'|'||READS_PER_EXEC||'|'||ROUND(TOTAL,2)||'|'||CPU_TIME||'|'||ELAPSED_TIME||'|'||SQL_ID||'||'||SQL_MODULE||'|'||DBMS_LOB.substr(SQL_TEXT,3000)||'|'
     from (select '&1|&2|&3|'||'&END_SNAP_TIME' "SAMPLE_TIME",sqt.dskr PHYSICAL_READS,
                  sqt.exec EXECUTIONS,
                  round(decode(sqt.exec, 0, to_number(null), (sqt.dskr / sqt.exec)),1) READS_PER_EXEC,
                  round((100 * sqt.dskr) /
                  (SELECT sum(e.VALUE) - sum(b.value)
                     FROM DBA_HIST_SYSSTAT b, DBA_HIST_SYSSTAT e
                    WHERE B.SNAP_ID = '&V_BID'
                      AND E.SNAP_ID = '&V_EID'
                      AND B.DBID = '&V_DBID'
                      AND E.DBID = '&V_DBID'
                      AND B.INSTANCE_NUMBER = USERENV('INSTANCE')
                      AND E.INSTANCE_NUMBER = USERENV('INSTANCE')
                      and e.STAT_NAME = 'physical reads'
                      and b.stat_name = 'physical reads'),1) TOTAL,
                  round(nvl((sqt.cput / 1000000), to_number(null)),2) CPU_TIME,
                  round(nvl((sqt.elap / 1000000), to_number(null)),2) ELAPSED_TIME,
                  sqt.sql_id SQL_ID,
                  decode(sqt.module, null, null, 'Module: ' || sqt.module) SQL_MODULE,
                  nvl(st.sql_text, to_clob('** SQL Text Not Available **')) SQL_TEXT
             from (select sql_id,
                          max(module) module,
                          sum(disk_reads_delta) dskr,
                          sum(executions_delta) exec,
                          sum(cpu_time_delta) cput,
                          sum(elapsed_time_delta) elap
                     from dba_hist_sqlstat
                    where dbid = '&V_DBID'
                      and instance_number = USERENV('INSTANCE')
                      and '&V_BID' < snap_id
                      and snap_id <= '&V_EID'
                    group by sql_id) sqt,
                  dba_hist_sqltext st
            where st.sql_id(+) = sqt.sql_id
              and st.dbid(+) = '&V_DBID'
              and (SELECT sum(e.VALUE) - sum(b.value)
                     FROM DBA_HIST_SYSSTAT b, DBA_HIST_SYSSTAT e
                    WHERE B.SNAP_ID = '&V_BID'
                      AND E.SNAP_ID = '&V_EID'
                      AND B.DBID = '&V_DBID'
                      AND E.DBID = '&V_DBID'
                      AND B.INSTANCE_NUMBER = USERENV('INSTANCE')
                      AND E.INSTANCE_NUMBER = USERENV('INSTANCE')
                      and e.STAT_NAME = 'physical reads'
                      and b.stat_name = 'physical reads') > 0
            order by nvl(sqt.dskr, -1) desc, sqt.sql_id)
    where rownum <= 5
    and(rownum <= 10 or TOTAL > 1);
spool off
exit
