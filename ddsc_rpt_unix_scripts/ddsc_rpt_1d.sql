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

column na01 new_value vn01
column na02 new_value vn02
column na04 new_value vn04
column na05 new_value vn05
column na06 new_value vn06
column na07 new_value vn07
column na08 new_value vn08
column na10 new_value vn10
column na12 new_value vn12
column na15 new_value vn15
column na19 new_value vn19


-- Default Spool Path SCRIPT_HOME/
select 'SCRIPT_HOME/ddsc_db_size.txt'    na01 from dual;
select 'SCRIPT_HOME/ddsc_db_tbs.txt'     na02 from dual;
select 'SCRIPT_HOME/ddsc_db_df.txt'      na04 from dual;
select 'SCRIPT_HOME/ddsc_db_inst.txt'    na05 from dual;
select 'SCRIPT_HOME/ddsc_db_par.txt'     na06 from dual;
select 'SCRIPT_HOME/ddsc_db_idx.txt'     na07 from dual;
select 'SCRIPT_HOME/ddsc_db_obj.txt'     na08 from dual;
select 'SCRIPT_HOME/ddsc_db_logsw.txt'   na10 from dual;
select 'SCRIPT_HOME/ddsc_db_seg.txt'     na12 from dual;
select 'SCRIPT_HOME/ddsc_db_sgars.txt'   na15 from dual;
select 'SCRIPT_HOME/ddsc_db_bak.txt'     na19 from dual;

spool '&vn01'
select '&1|&2|&3|&4|'||( 
  (select sum("MB") from (select sum(bytes/1024/1024) "MB" from dba_data_files
   union all
  select sum(bytes/1024/1024) "MB"  from dba_temp_files ))   ||'|'||
  (select sum(bytes/1024/1024) "FREE_MB" from dba_free_space)  ||'|'||
   ((select sum("MB") from (select sum(bytes/1024/1024) "MB" from dba_data_files
   union all
  select sum(bytes/1024/1024) "MB"  from dba_temp_files ))-(select sum(bytes/1024/1024) "FREE_MB" from dba_free_space)) 
) "DB_SIZE" from dual ; 
spool off

spool '&vn02'
select '&1|&2|&3|&4|'||tablespace_name||'|'||megs_alloc||'|'||megs_free||'|'||megs_used||'|'||max_mb||'|'||max_free_megs||'|'||"ALLOC_PCT_USED%"||'|'||"MAX_SIZE_PCT_USED%" from (
 SELECT a.tablespace_name,
       ROUND (a.bytes_alloc  / 1024 / 1024, 2 ) MEGS_ALLOC,
       ROUND (NVL (b.bytes_free, 0)  / 1024 / 1024 , 2) MEGS_FREE,
       ROUND ((a.bytes_alloc - NVL (b.bytes_free, 0)) / 1024 / 1024 , 2) MEGS_USED,
       ROUND (maxbytes / 1048576 , 2) MAX_MB,
       ( ROUND (maxbytes / 1048576 , 2) -
       ROUND ((a.bytes_alloc - NVL (b.bytes_free, 0)) / 1024 / 1024 , 2))  MAX_FREE_MEGS,
       100 - ROUND (( NVL(b.bytes_free, 0 ) / a.bytes_alloc) * 100 , 2) "ALLOC_PCT_USED%",
       ROUND ((100 * (a.bytes_alloc - NVL(b.bytes_free, 0 )) / 1024 / 1024 ) /
             (maxbytes / 1048576 ),
             2 ) "MAX_SIZE_PCT_USED%"
  FROM ( SELECT f.tablespace_name,
               SUM (f.BYTES) bytes_alloc,
               SUM (DECODE (f.autoextensible, 'YES', f.maxbytes, 'NO' , f.BYTES)) maxbytes
          FROM dba_data_files f
         GROUP BY tablespace_name) a,
       ( SELECT f.tablespace_name, SUM (f.BYTES) bytes_free
          FROM dba_free_space f
         GROUP BY tablespace_name) b
  WHERE a.tablespace_name = b.tablespace_name(+) order by "MAX_SIZE_PCT_USED%" desc
  ) ;
spool off

spool '&vn04'
SELECT '&1|&2|&3|&4|'||
     FILE_NAME||'|'||
     FILE_ID||'|'||
     TABLESPACE_NAME||'|'||
     BYTES/1024/1024||'|'||
     BLOCKS||'|'||
     STATUS||'|'||
     RELATIVE_FNO||'|'||
     AUTOEXTENSIBLE||'|'||
     MAXBYTES||'|'||
     MAXBLOCKS||'|'||
     INCREMENT_BY||'|'||
     USER_BYTES||'|'||
     USER_BLOCKS||'|'||
     ONLINE_STATUS
FROM DBA_DATA_FILES;
spool off

spool '&vn05'
SELECT '&1|&2|&3|&4|'||VERSION||'|'||to_char(STARTUP_TIME,'yyyymmdd hh24miss')||'|'||ROUND(SYSDATE-STARTUP_TIME,2) FROM V$INSTANCE;
spool off

spool '&vn06'
SELECT '&1|&2|&3|&4|'||NAME||'|'||VALUE||'|'||ISMODIFIED NOT_DEFAULT
FROM V$SYSTEM_PARAMETER 
WHERE VALUE IS NOT NULL AND ISDEFAULT ='FALSE';
spool off

spool '&vn07'
SELECT '&1|&2|&3|&4|'||OWNER||'|'||INDEX_NAME||'|'||STATUS
FROM DBA_INDEXES
WHERE STATUS = 'UNUSABLE';
spool off

spool '&vn08'
SELECT '&1|&2|&3|&4|'||OWNER||'|'||OBJECT_TYPE||'|'||OBJECT_NAME "INVALID OBJECT NAME"
FROM DBA_OBJECTS
WHERE STATUS='INVALID'
ORDER BY OWNER,OBJECT_TYPE,OBJECT_NAME;
spool off

spool '&vn10'
SELECT '&1|&2|&3|&4|'||TO_CHAR(TRUNC(COMPLETION_TIME),'yyyymmdd')||'|'||COUNT(*)||'|'||ROUND((SUM(BLOCKS*BLOCK_SIZE/1024/1024)),2) 
FROM V$ARCHIVED_LOG where COMPLETION_TIME between (trunc(sysdate)-1)  and (trunc(sysdate))
GROUP BY TRUNC(COMPLETION_TIME)
ORDER BY 1;
spool off





spool '&vn19'
SELECT '&1|&2|&3|&4|'||TO_CHAR(START_TIME,'YYYYMMDDHH24MI')||'|'||
       TO_CHAR(END_TIME,'YYYYMMDDHH24MI')||'|'||
       OUTPUT_DEVICE_TYPE||'|'||
       INPUT_TYPE||'|'||
       INPUT_BYTES_DISPLAY||'|'||
       OUTPUT_BYTES_DISPLAY||'|'||
       INPUT_BYTES_PER_SEC_DISPLAY||'|'||
       OUTPUT_BYTES_PER_SEC_DISPLAY||'|'||
       TIME_TAKEN_DISPLAY||'|'||
       STATUS
  FROM V$RMAN_BACKUP_JOB_DETAILS A where START_TIME >(trunc(sysdate)-1)
  ORDER BY START_TIME;
spool off
  
spool '&vn12'
select '&1|&2|&3|&4|'||owner||'|'||segment_name||'|'||segment_type||'|'||mb from (
               select owner,segment_name,segment_type,(bytes/1024/1024) "MB" 
               from dba_segments 
               where segment_name not in (select segment_name from dba_rollback_segs) 
               order by bytes desc
              )
         where  rownum <=30;
spool off

spool '&vn15'
SELECT '&1|&2|&3|&4|'||COMPONENT||'|'||OPER_TYPE||'|'||OPER_MODE||'|'||PARAMETER||'|'||INITIAL_SIZE||'|'||TARGET_SIZE||'|'||FINAL_SIZE||'|'||STATUS||'|'||to_char(START_TIME,'yyyymmdd hh24miss')||'|'||to_char(END_TIME,'yyyymmdd hh24miss') 
FROM V$SGA_RESIZE_OPS where START_TIME > trunc(sysdate) ;
spool off  


exit