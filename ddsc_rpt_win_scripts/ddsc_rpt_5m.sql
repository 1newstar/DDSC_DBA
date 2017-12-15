SET pagesize9999
SET line 9999
SET head OFF
SET ECHO OFF
SET FEEDBACK OFF
SET VERIFY OFF
SET TIMING OFF
SET TRIMS ON
SET TRIM ON
SET NEWPAGE NONE


column na03 new_value vn03
column na09 new_value vn09
column na11 new_value vn11
column na13 new_value vn13
--column na14 new_value vn14
column na15 new_value vn15
column na16 new_value vn16
column na17 new_value vn17
column na18 new_value vn18
column na19 new_value vn19


-- Default Spool Path DATA_DIR\
select 'DATA_DIR\ddsc_db_undo.txt'    na03 from dual;
select 'DATA_DIR\ddsc_db_lib.txt'     na09 from dual;
select 'DATA_DIR\ddsc_db_pga.txt'     na11 from dual;
select 'DATA_DIR\ddsc_db_sess.txt'    na13 from dual;
--select 'DATA_DIR\ddsc_db_sgacom.txt'  na14 from dual;
select 'DATA_DIR\ddsc_db_sgars.txt'   na15 from dual;
select 'DATA_DIR\ddsc_db_share.txt'   na16 from dual;
select 'DATA_DIR\ddsc_db_sysstat.txt' na17 from dual;
select 'DATA_DIR\ddsc_db_dict.txt'    na18 from dual;
select 'DATA_DIR\ddsc_db_temp.txt'    na19 from dual;


spool '&vn03'
SELECT '&1|&2|&3|&4|'||S.SID||'|'||
       S.SERIAL#||'|'||
       S.USERNAME||'|'||
       S.PROGRAM||'|'||
       S.MACHINE||'|'||
       T.USED_UBLK||'|'||
       ROUND(T.USED_UBLK*8/1024,2)||'|'||
       T.USED_UREC
  FROM V$TRANSACTION T, V$SESSION S
 WHERE S.SADDR = T.SES_ADDR;
spool off

spool '&vn09'
select '&1|&2|&3|&4|'||sum(pins)||'|'||sum(pinhits) from v$librarycache;
spool off

spool '&vn11'
SELECT '&1|&2|&3|&4|'||OPTIMAL_COUNT||'|'||ROUND(OPTIMAL_COUNT*100/TOTAL, 2)||'|'||
       ONEPASS_COUNT||'|'|| ROUND(ONEPASS_COUNT*100/TOTAL, 2)||'|'||
       MULTIPASS_COUNT||'|'||ROUND(MULTIPASS_COUNT*100/TOTAL, 2)
FROM (SELECT DECODE(SUM(TOTAL_EXECUTIONS), 0 , 1, SUM(TOTAL_EXECUTIONS)) "TOTAL",
               SUM(OPTIMAL_EXECUTIONS) "OPTIMAL_COUNT",
               SUM(ONEPASS_EXECUTIONS) "ONEPASS_COUNT",
               SUM(MULTIPASSES_EXECUTIONS) "MULTIPASS_COUNT"
          FROM V$SQL_WORKAREA_HISTOGRAM
WHERE LOW_OPTIMAL_SIZE > 64*1024);
spool off



spool '&vn13'
SELECT '&1|&2|&3|&4|'||COUNT(CASE STATUS WHEN 'ACTIVE' THEN STATUS END)||'|'||
       COUNT(CASE STATUS WHEN 'INACTIVE' THEN STATUS END)||'|'||
       COUNT(*)
FROM V$SESSION;
spool off

/*
spool '&vn14'
SELECT '&1|&2|&3|&4|'||COMPONENT||'|'||CURRENT_SIZE||'|'||MIN_SIZE||'|'||MAX_SIZE||'|'||USER_SPECIFIED_SIZE||'|'||OPER_COUNT||'|'||LAST_OPER_TYPE||'|'||LAST_OPER_MODE||'|'||to_char(LAST_OPER_TIME,'yyyymmdd hh24miss')||'|'||GRANULE_SIZE FROM V$SGA_DYNAMIC_COMPONENTS;
spool off
*/

spool '&vn15'
SELECT '&1|&2|&3|&4|'||COMPONENT||'|'||OPER_TYPE||'|'||OPER_MODE||'|'||PARAMETER||'|'||INITIAL_SIZE||'|'||TARGET_SIZE||'|'||FINAL_SIZE||'|'||STATUS||'|'||to_char(START_TIME,'yyyymmdd hh24miss')||'|'||to_char(END_TIME,'yyyymmdd hh24miss') FROM V$SGA_RESIZE_OPS;
spool off

spool '&vn16'
select '&1|&2|&3|&4|'||a.total||'|'||b.unused||'|'||(a.total-b.unused)||'|'||(round((a.total-b.unused)/a.total,2))*100
 from (select pool, round(sum(bytes)/1024/1024,2) "TOTAL"
         from v$sgastat
        where pool = 'shared pool'
      group by pool) a,
     (select pool, round(bytes/1024/1024) "UNUSED"
       from v$sgastat
        where pool = 'shared pool'
         and name = 'free memory') b
where b.pool = a.pool;
spool off

spool '&vn17'
select ( '&1|&2|&3|&4|'||(SELECT PR.VALUE FROM V$SYSSTAT PR WHERE PR.NAME  ='physical reads')||'|'||
(SELECT PRD.VALUE FROM V$SYSSTAT PRD WHERE PRD.NAME ='physical reads direct') ||'|'||
(SELECT PRDL.VALUE FROM V$SYSSTAT PRDL WHERE PRDL.NAME='physical reads direct (lob)')||'|'||
(SELECT PRC.VALUE FROM V$SYSSTAT PRC  WHERE PRC.NAME ='physical reads cache')||'|'||
(SELECT DBG.VALUE FROM V$SYSSTAT DBG WHERE DBG.NAME ='db block gets')||'|'||
(SELECT DBGC.VALUE FROM V$SYSSTAT DBGC WHERE DBGC.NAME='db block gets from cache')||'|'||
(SELECT CG.VALUE FROM V$SYSSTAT CG WHERE CG.NAME  ='consistent gets')||'|'||
(SELECT CGC.VALUE FROM V$SYSSTAT CGC  WHERE CGC.NAME ='consistent gets from cache')||'|'||
(SELECT RS.VALUE FROM V$SYSSTAT RS  WHERE RS.NAME  ='redo size') ||'|'||
(SELECT RB.VALUE FROM  V$SYSSTAT RB WHERE RB.NAME  ='redo buffer allocation retries')||'|'||
(SELECT RSR.VALUE FROM V$SYSSTAT RSR WHERE RSR.NAME ='redo log space requests')||'|'||
(SELECT RSW.VALUE FROM V$SYSSTAT RSW WHERE RSW.NAME ='redo log space wait time')||'|'||
(SELECT RSI.VALUE FROM  V$SYSSTAT RSI   WHERE RSI.NAME ='redo log switch interrupts')) "BCT-DATA" from dual ;     

spool off

spool '&vn18'
SELECT '&1|&2|&3|&4|'||SUM(GETS)||'|'||SUM(GETMISSES) FROM V$ROWCACHE;
spool off


spool '&vn19'
SELECT '&1|&2|&3|&4|'||tsh.tablespace_name||'|'||
       (tsh.total_bytes/tsh.inst_count)/1024/1024||'|'||
       ((tsh.bytes_free/tsh.inst_count) +
       ( nvl (ss.free_blocks,0) * ts$.blocksize))/1024/1024 as "TBS_NAME,TOTAL_MB,FREE_MB"
  FROM ( SELECT tablespace_name,
               SUM (bytes_used + bytes_free) total_bytes,
               SUM (bytes_used) bytes_used,
               SUM (bytes_free) bytes_free,
               COUNT (DISTINCT inst_id) inst_count
          FROM gv$temp_space_header
         GROUP BY tablespace_name) tsh,
       ( SELECT tablespace_name, SUM (free_blocks) free_blocks
          FROM gv$sort_segment
         GROUP BY tablespace_name) ss,
       sys.ts$
  WHERE ts$.name = tsh.tablespace_name
   AND tsh.tablespace_name = ss.tablespace_name(+);
spool off

exit