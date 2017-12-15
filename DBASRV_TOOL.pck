CREATE OR REPLACE PACKAGE DBASRV_TOOL AUTHID CURRENT_USER IS

  -- Author  : JEREL
  -- Created : 2013/4/15 .. 11:11:56
  -- Purpose : FOR DDSC DBA SERVICE DATA

  PROCEDURE imp_alert(v_directory IN VARCHAR2);
  PROCEDURE imp_osdata(v_directory IN VARCHAR2,
                       v_os        IN VARCHAR2 DEFAULT 'unix');
  FUNCTION get_nmon_hostname(v_directory IN VARCHAR2) RETURN VARCHAR2;
  --PROCEDURE imp_osdata(v_directory IN VARCHAR2,v_errbuf OUT VARCHAR2,v_retcode OUT NUMBER);
  PROCEDURE delete_all_by_custid(v_cust_id IN VARCHAR2);
  PROCEDURE delete_all_by_date(v_cust_id IN VARCHAR2,yyyymmdd IN VARCHAR2);
  PROCEDURE delete_dbdata_by_inst(v_inst IN VARCHAR2);
  PROCEDURE delete_dbdata_by_host(v_host IN VARCHAR2);
  PROCEDURE delete_osdata_by_host(v_host IN VARCHAR2);
  PROCEDURE delete_os_dup_data(v_cust_id IN VARCHAR2);
  PROCEDURE delete_db_dup_data(v_cust_id IN VARCHAR2, v_inst IN VARCHAR2);
  PROCEDURE delete_db_dup_data(v_cust_id IN VARCHAR2);
  --PROCEDURE delete_db_dup_direct(v_cust_id IN VARCHAR2);
  PROCEDURE change_cust_id(old_cust_id IN VARCHAR2,
                           new_cust_id IN VARCHAR2);
  PROCEDURE count_data;
  TYPE bc_hit_type IS RECORD(
    SAMPLE_TIME DATE,
    HIT_RATE    NUMBER);
  TYPE bc_hit IS TABLE OF bc_hit_type;

  FUNCTION monitor_bf_cache(cust_id IN VARCHAR2,
                            v_btime IN VARCHAR2 DEFAULT NULL,
                            v_etime IN VARCHAR2 DEFAULT NULL) RETURN bc_hit
    PIPELINED;

  TYPE dd_hit_type IS RECORD(
    SAMPLE_TIME DATE,
    HIT_RATE    NUMBER);

  TYPE dd_hit_table IS TABLE OF dd_hit_type;
  FUNCTION monitor_dd_cache(cust_id IN VARCHAR2,
                            v_btime IN VARCHAR2 DEFAULT NULL,
                            v_etime IN VARCHAR2 DEFAULT NULL)
    RETURN dd_hit_table
    PIPELINED;

  TYPE lib_hit_type IS RECORD(
    SAMPLE_TIME DATE,
    HIT_RATE    NUMBER);

  TYPE lib_hit_table IS TABLE OF lib_hit_type;
  FUNCTION monitor_lib_cache(cust_id IN VARCHAR2,
                             v_btime IN VARCHAR2 DEFAULT NULL,
                             v_etime IN VARCHAR2 DEFAULT NULL)
    RETURN lib_hit_table
    PIPELINED;
  /*
   TYPE db_hit_type IS RECORD (
      SAMPLE_TIME       DATE,
      bf_hit            number,
      lib_hit           number,
      dd_hit            number
   );
  TYPE db_hit_table IS TABLE OF db_hit_type;
  
    FUNCTION monitor_all_hit(cust_id IN VARCHAR2,
                             v_btime IN VARCHAR2 DEFAULT NULL,
                             v_etime IN VARCHAR2 DEFAULT NULL) RETURN bc_hit
     PIPELINED;*/
  PROCEDURE nmon_rawdata_imp(v_cust_id   IN VARCHAR2,
                             v_directory IN VARCHAR2);
  PROCEDURE nmon_rawdata_imp_rhel4(v_cust_id   IN VARCHAR2,
                             v_directory IN VARCHAR2);  
  PROCEDURE nmon_rawdata_imp_nfs(v_cust_id   IN VARCHAR2,
                             v_directory IN VARCHAR2);                                                    
  PROCEDURE nmon_rawdata_imp_byfile(v_cust_id   IN VARCHAR2,
                                    v_directory IN VARCHAR2,
                                    v_filename  IN VARCHAR2);
  PROCEDURE win_rawdata_imp(v_cust_id IN VARCHAR2, v_directory IN VARCHAR2);
  PROCEDURE nmon_calc(v_cust_id   IN VARCHAR2,
                      v_directory IN VARCHAR2,
                      IO_TYPE     IN VARCHAR2);
  PROCEDURE nmon_calc_byfile(v_cust_id   IN VARCHAR2,
                             v_directory IN VARCHAR2,
                             IO_TYPE     IN VARCHAR2,
                             v_filename  IN VARCHAR2);
  PROCEDURE nmon_calc_imp(v_cust_id IN VARCHAR2, v_directory IN VARCHAR2);
  PROCEDURE nmon_calc_imp_nfs(v_cust_id IN VARCHAR2, v_directory IN VARCHAR2);
  PROCEDURE nmon_calc_imp_byfile(v_cust_id   IN VARCHAR2,
                                 v_directory IN VARCHAR2,
                                 v_filename  IN VARCHAR2);
  PROCEDURE nmon_tab_drop;

END DBASRV_TOOL;
/
CREATE OR REPLACE PACKAGE BODY DBASRV_TOOL IS
  v_hostname VARCHAR2(20);
  PROCEDURE imp_alert(v_directory IN VARCHAR2) IS
    v_alert_csv VARCHAR2(50);
  
  BEGIN
    EXECUTE IMMEDIATE 'alter table DDSC_DB_ALERT_EXT  default directory ' ||
                      v_directory;
  
    SELECT basename(column_value)
      INTO v_alert_csv
      FROM (SELECT * FROM TABLE(LIST_FILES(v_directory)))
     WHERE basename(column_value) LIKE '%_alert.csv';
  
    EXECUTE IMMEDIATE 'alter table DDSC_DB_ALERT_EXT location (''' ||
                      v_alert_csv || ''')';
  
    INSERT INTO DDSC_DB_ALERT
      (cust_id, host, inst_name, alert_time, text)
      SELECT cust_id, host, inst_name, alert_time, text
        FROM DDSC_DB_ALERT_EXT;
    COMMIT;
  
  END;

  PROCEDURE imp_osdata(v_directory IN VARCHAR2,
                       v_os        IN VARCHAR2 DEFAULT 'unix') IS
    --PROCEDURE imp_osdata(v_directory IN VARCHAR2,v_errbuf OUT VARCHAR2,v_retcode OUT NUMBER) IS
  
    --v_type   VARCHAR2(10);
    v_sql     VARCHAR2(1000);
    v_count   NUMBER;
    v_timelen NUMBER;
  BEGIN
    --v_type  := 'cpu';
    v_count := 0;
  
    EXECUTE IMMEDIATE 'alter table DDSC_TIMESTAMP_EXT  default directory ' ||
                      v_directory;
    EXECUTE IMMEDIATE 'alter table DDSC_OS_CPU_EXT  default directory ' ||
                      v_directory;
    EXECUTE IMMEDIATE 'alter table DDSC_OS_MEM_EXT  default directory ' ||
                      v_directory;
    EXECUTE IMMEDIATE 'alter table DDSC_OS_PGSP_EXT  default directory ' ||
                      v_directory;
    EXECUTE IMMEDIATE 'alter table DDSC_OS_DISKIO_EXT  default directory ' ||
                      v_directory;
  
    --NMON DATA
  
    FOR i IN (SELECT basename(column_value, v_separator => '\') AS NFILE
                FROM (SELECT *
                        FROM TABLE(LIST_FILES(v_directory))
                       WHERE basename(column_value, v_separator => '\') LIKE
                             '%_win.csv'
                          OR basename(column_value, v_separator => '\') LIKE
                             '%_nmon.csv')
               ORDER BY 1
              --IF BIP SERVER on WINDOWS Below is unusable
              /*UNION ALL
              SELECT basename(column_value) AS NFILE
                FROM (SELECT *
                        FROM TABLE(LIST_FILES(v_directory))
                       WHERE basename(column_value) LIKE '%_nmon.csv')
               ORDER BY 1*/
              ) LOOP
      v_count := v_count + 1;
      IF split(i.nfile, 3, '_') = 'timestamp' THEN
        v_sql := 'alter table DDSC_TIMESTAMP_EXT location (''' || i.nfile ||
                 ''')';
        --dbms_output.put_line(v_sql);
        EXECUTE IMMEDIATE v_sql;
      ELSIF split(i.nfile, 3, '_') = 'cpu' THEN
        v_sql := 'alter table DDSC_OS_CPU_EXT location (''' || i.nfile ||
                 ''')';
        --dbms_output.put_line(v_sql);
        EXECUTE IMMEDIATE v_sql;
      ELSIF split(i.nfile, 3, '_') = 'mem' THEN
        v_sql := 'alter table DDSC_OS_MEM_EXT location (''' || i.nfile ||
                 ''')';
        --dbms_output.put_line(v_sql);
        EXECUTE IMMEDIATE v_sql;
      ELSIF split(i.nfile, 3, '_') = 'pgsp' THEN
        v_sql := 'alter table DDSC_OS_PGSP_EXT location (''' || i.nfile ||
                 ''')';
        --dbms_output.put_line(v_sql);
        EXECUTE IMMEDIATE v_sql;
      ELSIF split(i.nfile, 3, '_') = 'diskio' THEN
        v_sql := 'alter table DDSC_OS_DISKIO_EXT location (''' || i.nfile ||
                 ''')';
        --dbms_output.put_line(v_sql);
        EXECUTE IMMEDIATE v_sql;
      END IF;
      BEGIN
        IF v_os = 'unix' OR v_os = 'UNIX' THEN
          IF MOD(v_count, 5) = 0 THEN
            --CPU INSERT SQL
            SELECT length(ndate)
              INTO v_timelen
              FROM DDSC_TIMESTAMP_EXT
             WHERE rownum < 2;
            IF v_timelen = 6 THEN
              INSERT INTO DDSC_OS_CPU
                SELECT a.cust_id,
                       a.host,
                       to_date(b.ndate || ' ' || b.ntime,
                               'YYMMDD HH24:MI:SS') "SAMPLE_TIME",
                       a.user_pct,
                       a.sys_pct,
                       a.iowait,
                       a.idle,
                       a.busy,
                       a.num_cpus,
                       a.cpu_load
                  FROM DDSC_OS_CPU_EXT a, DDSC_TIMESTAMP_EXT b
                 WHERE a.time_id = b.time_id;
              COMMIT;
              --MEMORY INSERT SQL
              INSERT INTO DDSC_OS_MEM
                (cust_id, host, sample_time, total, free)
                SELECT a.cust_id,
                       a.host,
                       to_date(b.ndate || ' ' || b.ntime,
                               'YYMMDD HH24:MI:SS') "SAMPLE_TIME",
                       a.TOTAL,
                       a.FREE
                  FROM DDSC_OS_MEM_EXT a, DDSC_TIMESTAMP_EXT b
                 WHERE a.time_id = b.time_id;
              COMMIT;
              --DISKIO INSERT SQL
              INSERT INTO DDSC_OS_DISKIO
                SELECT a.cust_id,
                       a.host,
                       to_date(b.ndate || ' ' || b.ntime,
                               'YYMMDD HH24:MI:SS') "SAMPLE_TIME",
                       a.READ_KBS,
                       a.WRITE_KBS,
                       a.IOPS
                  FROM DDSC_OS_DISKIO_EXT a, DDSC_TIMESTAMP_EXT b
                 WHERE a.time_id = b.time_id;
              COMMIT;
              --PAGING SPACE INSERT SQL
              INSERT INTO DDSC_OS_PGSP
                SELECT a.cust_id,
                       a.host,
                       to_date(b.ndate || ' ' || b.ntime,
                               'YYMMDD HH24:MI:SS') "SAMPLE_TIME",
                       a.pgsin,
                       a.pgsout
                  FROM DDSC_OS_PGSP_EXT a, DDSC_TIMESTAMP_EXT b
                 WHERE a.time_id = b.time_id;
              COMMIT;
            ELSE
              --CPU INSERT SQL
              INSERT INTO DDSC_OS_CPU
                SELECT a.cust_id,
                       a.host,
                       to_date(b.ndate || ' ' || b.ntime,
                               'DD-MON-YYYY HH24:MI:SS') "SAMPLE_TIME",
                       a.user_pct,
                       a.sys_pct,
                       a.iowait,
                       a.idle,
                       a.busy,
                       a.num_cpus,
                       a.cpu_load
                  FROM DDSC_OS_CPU_EXT a, DDSC_TIMESTAMP_EXT b
                 WHERE a.time_id = b.time_id;
              COMMIT;
              --MEMORY INSERT SQL
              INSERT INTO DDSC_OS_MEM
                (cust_id, host, sample_time, total, free)
                SELECT a.cust_id,
                       a.host,
                       to_date(b.ndate || ' ' || b.ntime,
                               'DD-MON-YYYY HH24:MI:SS') "SAMPLE_TIME",
                       a.TOTAL,
                       a.FREE
                  FROM DDSC_OS_MEM_EXT a, DDSC_TIMESTAMP_EXT b
                 WHERE a.time_id = b.time_id;
              COMMIT;
              --DISKIO INSERT SQL
              INSERT INTO DDSC_OS_DISKIO
                SELECT a.cust_id,
                       a.host,
                       to_date(b.ndate || ' ' || b.ntime,
                               'DD-MON-YYYY HH24:MI:SS') "SAMPLE_TIME",
                       a.READ_KBS,
                       a.WRITE_KBS,
                       a.IOPS
                  FROM DDSC_OS_DISKIO_EXT a, DDSC_TIMESTAMP_EXT b
                 WHERE a.time_id = b.time_id;
              COMMIT;
              --PAGING SPACE INSERT SQL
              INSERT INTO DDSC_OS_PGSP
                SELECT a.cust_id,
                       a.host,
                       to_date(b.ndate || ' ' || b.ntime,
                               'DD-MON-YYYY HH24:MI:SS') "SAMPLE_TIME",
                       a.pgsin,
                       a.pgsout
                  FROM DDSC_OS_PGSP_EXT a, DDSC_TIMESTAMP_EXT b
                 WHERE a.time_id = b.time_id;
              COMMIT;
            END IF;
          END IF;
        ELSIF v_os = 'win' OR v_os = 'WIN' THEN
          IF MOD(v_count, 4) = 0 THEN
            --WINDOWS CPU INSERT SQL
            INSERT INTO DDSC_OS_CPU
              SELECT a.cust_id,
                     a.host,
                     to_timestamp(a.time_id, 'MM/DD/YYYY HH24:MI:SS.FF3') "SAMPLE_TIME",
                     a.user_pct,
                     a.sys_pct,
                     a.iowait,
                     a.idle,
                     a.busy,
                     a.num_cpus,
                     a.cpu_load
                FROM DDSC_OS_CPU_EXT a;
            COMMIT;
            --WINDOWS MEMORY INSERT SQL
            INSERT INTO DDSC_OS_MEM
              (cust_id, host, sample_time, total, free)
              SELECT a.cust_id,
                     a.host,
                     to_timestamp(a.time_id, 'MM/DD/YYYY HH24:MI:SS.FF3') "SAMPLE_TIME",
                     a.TOTAL,
                     a.FREE
                FROM DDSC_OS_MEM_EXT a;
            COMMIT;
            --WINDOWS DISKIO INSERT SQL
            INSERT INTO DDSC_OS_DISKIO
              SELECT a.cust_id,
                     a.host,
                     to_timestamp(a.time_id, 'MM/DD/YYYY HH24:MI:SS.FF3') "SAMPLE_TIME",
                     a.READ_KBS,
                     a.WRITE_KBS,
                     a.IOPS
                FROM DDSC_OS_DISKIO_EXT a;
            COMMIT;
            --WINDOWS PAGING SPACE INSERT SQL
            INSERT INTO DDSC_OS_PGSP
              SELECT a.cust_id,
                     a.host,
                     to_timestamp(a.time_id, 'MM/DD/YYYY HH24:MI:SS.FF3') "SAMPLE_TIME",
                     a.pgsin,
                     a.pgsout
                FROM DDSC_OS_PGSP_EXT a;
            COMMIT;
          END IF;
        END IF;
      
      EXCEPTION
        WHEN OTHERS THEN
          COMMIT;
          dbms_output.put_line(SQLERRM);
        
      END;
    END LOOP;
  
  END;

  FUNCTION get_nmon_hostname(v_directory IN VARCHAR2) RETURN VARCHAR2 IS
    v_rec        VARCHAR2(32766);
    v_ifile      VARCHAR2(100);
    v_input_file UTL_FILE.file_type;
  BEGIN
  
    SELECT basename(column_value, NULL, '\') "NAME"
      INTO v_ifile
      FROM TABLE(list_files(v_directory))
     WHERE rownum < 2;
    v_input_file := utl_file.fopen(v_directory, v_ifile, 'R', 32767);
    LOOP
      utl_file.get_line(v_input_file, v_rec, 32767);
      IF split(v_rec, 1, ',') || split(v_rec, 2, ',') = 'AAAhost' THEN
        v_hostname := split(v_rec, 3, ',');
        RETURN v_hostname;
        utl_file.fclose(v_input_file);
        EXIT;
      END IF;
    END LOOP;
  END;

  PROCEDURE delete_all_by_custid(v_cust_id IN VARCHAR2) IS
    v_sql VARCHAR2(1000);
  BEGIN
    FOR c IN (SELECT *
                FROM user_tables
               WHERE table_name NOT IN ('DDSC_CUSTMAIN')
                 AND (table_name LIKE 'DDSC_%' OR table_name LIKE 'TOP5_%')
                 AND table_name NOT LIKE '%_EXT') LOOP
      v_sql := 'delete ' || c.table_name || ' where upper(CUST_ID)=' || '''' ||
               upper(V_CUST_ID) || '''';
      --  dbms_output.put_line(v_sql);
      EXECUTE IMMEDIATE v_sql;
      COMMIT;
    END LOOP;
  END;

  PROCEDURE delete_all_by_date(v_cust_id IN VARCHAR2,yyyymmdd IN VARCHAR2) IS
    v_sql VARCHAR2(1000);
    v_date date;
  BEGIN
    v_date :=to_date(yyyymmdd,'yyyymmdd');
    FOR c IN (SELECT *
                FROM user_tables
               WHERE table_name NOT IN ('DDSC_CUSTMAIN')
                 AND (table_name LIKE 'DDSC_%' OR table_name LIKE 'TOP5_%')
                 AND table_name NOT LIKE '%_EXT') LOOP
    begin             
        if c.table_name='DDSC_DB_ALERT' then
          v_sql := 'delete ' || c.table_name || ' where upper(CUST_ID)=' || '''' ||
               upper(V_CUST_ID) || ''' and ALERT_TIME <= to_date('''||yyyymmdd||''',''yyyymmdd'')';
          else          
      v_sql := 'delete ' || c.table_name || ' where upper(CUST_ID)=' || '''' ||
               upper(V_CUST_ID) || ''' and sample_time <= to_date('''||yyyymmdd||''',''yyyymmdd'')';
         end if;     
      --dbms_output.put_line(v_sql);
      EXECUTE IMMEDIATE v_sql;
      COMMIT;
    exception when others then
     dbms_output.put_line(c.table_name||':'||sqlerrm);
     commit;
    end;
    END LOOP;
  END;


  PROCEDURE delete_dbdata_by_inst(v_inst IN VARCHAR2) IS
    v_sql VARCHAR2(1000);
  BEGIN
    FOR c IN (SELECT *
                FROM user_tables
               WHERE (table_name LIKE 'DDSC_DB_%' OR
                     table_name LIKE 'TOP5_%')
                 AND table_name NOT LIKE '%_EXT') LOOP
      v_sql := 'delete ' || c.table_name || ' where upper(INST_NAME)=' || '''' ||
               upper(v_inst) || '''';
      --  dbms_output.put_line(v_sql);
      EXECUTE IMMEDIATE v_sql;
      COMMIT;
    END LOOP;
  END;

  PROCEDURE delete_dbdata_by_host(v_host IN VARCHAR2) IS
    v_sql VARCHAR2(1000);
  BEGIN
    FOR c IN (SELECT *
                FROM user_tables
               WHERE (table_name LIKE 'DDSC_DB_%' OR
                     table_name LIKE 'TOP5_%')
                 AND table_name NOT LIKE '%_EXT') LOOP
      v_sql := 'delete ' || c.table_name || ' where upper(HOST)=' || '''' ||
               upper(v_host) || '''';
      --  dbms_output.put_line(v_sql);
      EXECUTE IMMEDIATE v_sql;
      COMMIT;
    END LOOP;
  END;

  PROCEDURE delete_osdata_by_host(v_host IN VARCHAR2) IS
    v_sql VARCHAR2(1000);
  BEGIN
    FOR c IN (SELECT *
                FROM user_tables
               WHERE table_name LIKE ('DDSC_OS%')
                 AND table_name NOT LIKE '%_EXT') LOOP
      v_sql := 'delete ' || c.table_name || ' where upper(HOST)=' || '''' ||
               upper(v_host) || '''';
      --  dbms_output.put_line(v_sql);
      EXECUTE IMMEDIATE v_sql;
      COMMIT;
    END LOOP;
  END;

  PROCEDURE delete_os_dup_data(v_cust_id IN VARCHAR2) IS
    v_rowid  VARCHAR2(64);
    v_count  NUMBER;
    v_delete VARCHAR2(500);
    v_del    VARCHAR2(500);
    -- v_cust_id VARCHAR2(20);
  BEGIN
    --v_cust_id := 'HCT';
    SELECT COUNT(1)
      INTO v_count
      FROM user_tables
     WHERE table_name = 'DEL_ROWID';
    IF v_count = 0 THEN
      EXECUTE IMMEDIATE 'create table DEL_ROWID (tab_name varchar2(30),rowdata rowid)';
    END IF;
    EXECUTE IMMEDIATE 'truncate table DEL_ROWID';
    FOR OS IN (SELECT table_name
                 FROM user_tables
                WHERE table_name LIKE 'DDSC_OS%'
                  AND TEMPORARY = 'N'
                  AND table_name NOT IN
                      (SELECT table_name FROM user_external_tables)) LOOP
      BEGIN
        v_delete := 'DELETE  FROM ' || os.table_name || ' where cust_id =' || '''' ||
                    v_cust_id || '''' ||
                    ' AND ROWID IN (SELECT ROWID FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id, host, to_char(sample_time, ''yyyy/mm/dd hh24mi'') ORDER BY ROWID) rn FROM ' ||
                    os.table_name || ' WHERE cust_id =' || '''' ||
                    v_cust_id || '''' || ') WHERE rn <> 1)';
      
        dbms_output.put_line(v_delete);
        EXECUTE IMMEDIATE v_delete;
        COMMIT;
        --dbms_output.put_line(c.cust_id||','||c.host||','||c.sample_time||','||v_rowid);
        --execute immediate ' DELETE'||os.table_name||' WHERE ROWID ='||c.min_rowid;
      EXCEPTION
        WHEN OTHERS THEN
          dbms_output.put_line(SQLERRM);
      END;
    
    END LOOP;
    COMMIT;
  END;

  /*  BAD WAY
      PROCEDURE delete_db_dup_data(v_cust_id IN VARCHAR2) IS
      v_rowid  VARCHAR2(64);
      v_count  NUMBER;
      v_insert VARCHAR2(500);
      v_del    VARCHAR2(500);
    BEGIN
      SELECT COUNT(1)
        INTO v_count
        FROM user_tables
       WHERE table_name = 'DEL_ROWID';
      IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'create table DEL_ROWID (tab_name varchar2(30),rowdata rowid)';
      END IF;
      EXECUTE IMMEDIATE 'truncate table DEL_ROWID';
      FOR DB IN (SELECT table_name
                   FROM user_tables
                  WHERE (table_name LIKE 'DDSC_DB_%' OR
                        table_name LIKE 'TOP5_%')
                    AND table_name NOT IN
                        (SELECT table_name FROM user_external_tables)) LOOP
        BEGIN
  
          v_insert := NULL;
          --TOP5 --cust_id,hst_name,to_char(sample_time, 'yyyy/mm/dd hh24mi'),sql_id
          --to_char(sample_time, 'yyyy/mm/dd hh24mi')
          IF upper(db.table_name) LIKE 'TOP5_%' THEN
            v_insert := 'INSERT INTO DEL_ROWID SELECT ''' || db.table_name ||
                        ''', ROWID  FROM ' || db.table_name ||
                        ' a where cust_id =' || '''' || v_cust_id || '''' ||
                        ' AND exists  (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi''),sql_id  ORDER BY ROWID) rn FROM ' ||
                        db.table_name || ' WHERE cust_id =' || '''' ||
                        v_cust_id || '''' ||
                        ') b  WHERE rn <> 1 and a.rowid=b.rowid)';
  
            --NORMAL TABLE cust_id,host,inst_name,to_char(sample_time, 'yyyy/mm/dd hh24mi')
            --to_char(sample_time, 'yyyy/mm/dd hh24mi')
          ELSIF upper(db.table_name) IN
                ('DDSC_DB_INST',
                 'DDSC_DB_LIB',
                 'DDSC_DB_DICT',
                 'DDSC_DB_LOGSW',
                 'DDSC_DB_PGA',
                 'DDSC_DB_SESS',
                 'DDSC_DB_SHARE',
                 'DDSC_DB_SIZE',
                 'DDSC_DB_SYSSTAT',
                 'DDSC_DB_TEMP') THEN
  
            v_insert := 'INSERT INTO DEL_ROWID SELECT ''' || db.table_name ||
                        ''', ROWID  FROM ' || db.table_name ||
                        ' a where cust_id =' || '''' || v_cust_id || '''' ||
                        ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi'')    ORDER BY ROWID) rn FROM ' ||
                        db.table_name || ' WHERE cust_id =' || '''' ||
                        v_cust_id || '''' ||
                        ') b WHERE rn <> 1 and a.rowid=b.rowid)';
  
            --DDSC_DB_DF     --cust_id,host,inst_name,to_char(sample_time, 'yyyy/mm/dd hh24mi'),file_id
          ELSIF upper(db.table_name) = 'DDSC_DB_DF' THEN
  
            v_insert := 'INSERT INTO DEL_ROWID SELECT ''' || db.table_name ||
                        ''', ROWID  FROM ' || db.table_name ||
                        ' a where cust_id =' || '''' || v_cust_id || '''' ||
                        ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi''),file_id    ORDER BY ROWID) rn FROM ' ||
                        db.table_name || ' WHERE cust_id =' || '''' ||
                        v_cust_id || '''' ||
                        ') b WHERE rn <> 1 and a.rowid=b.rowid)';
  
            --DDSC_DB_IDX   cust_id,host,inst_name,to_char(sample_time, 'yyyy/mm/dd hh24mi'),owner,index_name
          ELSIF upper(db.table_name) = 'DDSC_DB_IDX' THEN
  
            v_insert := 'INSERT INTO DEL_ROWID SELECT ''' || db.table_name ||
                        ''', ROWID  FROM ' || db.table_name ||
                        ' a where cust_id =' || '''' || v_cust_id || '''' ||
                        ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi''),owner,index_name      ORDER BY ROWID) rn FROM ' ||
                        db.table_name || ' WHERE cust_id =' || '''' ||
                        v_cust_id || '''' ||
                        ') b WHERE rn <> 1 and a.rowid=b.rowid)';
            --DDSC_DB_ALERT   cust_id host inst_name alert_time  text
          ELSIF upper(db.table_name) = 'DDSC_DB_ALERT' THEN
  
            v_insert := 'INSERT INTO DEL_ROWID SELECT ''' || db.table_name ||
                        ''', ROWID  FROM ' || db.table_name ||
                        ' a where cust_id =' || '''' || v_cust_id || '''' ||
                        ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(alert_time, ''yyyy/mm/dd hh24miss''),text      ORDER BY ROWID) rn FROM ' ||
                        db.table_name || ' WHERE cust_id =' || '''' ||
                        v_cust_id || '''' ||
                        ') b WHERE rn <> 1 and a.rowid=b.rowid)';
            --DDSC_DB_BAK   cust_id host inst_name start_time input_type
          ELSIF upper(db.table_name) = 'DDSC_DB_BAK' THEN
  
            v_insert := 'INSERT INTO DEL_ROWID SELECT ''' || db.table_name ||
                        ''', ROWID  FROM ' || db.table_name ||
                        ' a where cust_id =' || '''' || v_cust_id || '''' ||
                        ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(start_time, ''yyyy/mm/dd hh24miss''), input_type       ORDER BY ROWID) rn FROM ' ||
                        db.table_name || ' WHERE cust_id =' || '''' ||
                        v_cust_id || '''' ||
                        ') b WHERE rn <> 1 and a.rowid=b.rowid)';
            --DDSC_DB_OBJ   cust_id,host,inst_name,to_char(sample_time, 'yyyy/mm/dd hh24mi'),owner,object_type,object_name
          ELSIF upper(db.table_name) = 'DDSC_DB_OBJ' THEN
  
            v_insert := 'INSERT INTO DEL_ROWID SELECT ''' || db.table_name ||
                        ''', ROWID  FROM ' || db.table_name ||
                        ' a where cust_id =' || '''' || v_cust_id || '''' ||
                        ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd''),owner,object_type,object_name      ORDER BY ROWID) rn FROM ' ||
                        db.table_name || ' WHERE cust_id =' || '''' ||
                        v_cust_id || '''' ||
                        ') b WHERE rn <> 1 and a.rowid=b.rowid)';
  
            --DDSC_DB_SEG   cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi''),owner,segment_name
          ELSIF upper(db.table_name) = 'DDSC_DB_SEG' THEN
            v_insert := 'INSERT INTO DEL_ROWID SELECT ''' || db.table_name ||
                        ''', ROWID  FROM ' || db.table_name ||
                        ' a where cust_id =' || '''' || v_cust_id || '''' ||
                        ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd''),owner,segment_name      ORDER BY ROWID) rn FROM ' ||
                        db.table_name || ' WHERE cust_id =' || '''' ||
                        v_cust_id || '''' ||
                        ')  b WHERE rn <> 1 and a.rowid=b.rowid)';
  
            --DDSC_DB_SGACOM  --cust_id host inst_name to_char(sample_time, ''yyyy/mm/dd hh24mi'') component
          ELSIF upper(db.table_name) = 'DDSC_DB_SGACOM' THEN
            v_insert := 'INSERT INTO DEL_ROWID SELECT ''' || db.table_name ||
                        ''', ROWID  FROM ' || db.table_name ||
                        ' a where cust_id =' || '''' || v_cust_id || '''' ||
                        ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi''),component       ORDER BY ROWID) rn FROM ' ||
                        db.table_name || ' WHERE cust_id =' || '''' ||
                        v_cust_id || '''' ||
                        ') b WHERE rn <> 1 and a.rowid=b.rowid)';
  
            --DDSC_DB_SGARS   --cust_id host inst_name to_char(sample_time, ''yyyy/mm/dd hh24mi'') component
          ELSIF upper(db.table_name) = 'DDSC_DB_SGARS' THEN
            v_insert := 'INSERT INTO DEL_ROWID SELECT ''' || db.table_name ||
                        ''', ROWID  FROM ' || db.table_name ||
                        ' a where cust_id =' || '''' || v_cust_id || '''' ||
                        ' AND exists (SELECT ROWID FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi''),component      ORDER BY ROWID) rn FROM ' ||
                        db.table_name || ' WHERE cust_id =' || '''' ||
                        v_cust_id || '''' ||
                        ') b WHERE rn <> 1 and a.rowid=b.rowid)';
            --DDSC_DB_TBS  cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd''),tablespace
          ELSIF upper(db.table_name) = 'DDSC_DB_TBS' THEN
            v_insert := 'INSERT INTO DEL_ROWID SELECT ''' || db.table_name ||
                        ''', ROWID  FROM ' || db.table_name ||
                        ' a where cust_id =' || '''' || v_cust_id || '''' ||
                        ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd''),tablespace      ORDER BY ROWID) rn FROM ' ||
                        db.table_name || ' WHERE cust_id =' || '''' ||
                        v_cust_id || '''' ||
                        ') b WHERE rn <> 1 and a.rowid=b.rowid)';
            --DDSC_DB_UNDO    --cust_id host inst_name sample_time sid serial# program machine
          ELSIF upper(db.table_name) = 'DDSC_DB_UNDO' THEN
            v_insert := 'INSERT INTO DEL_ROWID SELECT ''' || db.table_name ||
                        ''', ROWID  FROM ' || db.table_name ||
                        ' a where cust_id =' || '''' || v_cust_id || '''' ||
                        ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi''),sid,serial#,program,machine      ORDER BY ROWID) rn FROM ' ||
                        db.table_name || ' WHERE cust_id =' || '''' ||
                        v_cust_id || '''' ||
                        ') b WHERE rn <> 1 and a.rowid=b.rowid)';
  
            --DDSC_DB_PAR --cust_id host inst_name to_char(sample_time, ''yyyy/mm/dd''),name
  
          ELSIF upper(db.table_name) = 'DDSC_DB_PAR' THEN
            v_insert := 'INSERT INTO DEL_ROWID SELECT ''' || db.table_name ||
                        ''', ROWID  FROM ' || db.table_name ||
                        ' a where cust_id =' || '''' || v_cust_id || '''' ||
                        ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd''),name      ORDER BY ROWID) rn FROM ' ||
                        db.table_name || ' WHERE cust_id =' || '''' ||
                        v_cust_id || '''' ||
                        ') b WHERE rn <> 1 and a.rowid=b.rowid)';
  
          END IF;
          --dbms_output.put_line(v_insert);
          EXECUTE IMMEDIATE v_insert;
          COMMIT;
        EXCEPTION
          WHEN OTHERS THEN
            dbms_output.put_line(SQLERRM);
        END;
  
      END LOOP;
      COMMIT;
  
      FOR i IN (SELECT * FROM DEL_ROWID) LOOP
        BEGIN
          v_del := 'delete ' || i.tab_name || ' where rowid=' || '''' ||
                   i.rowdata || '''';
          EXECUTE IMMEDIATE v_del;
        EXCEPTION
          WHEN OTHERS THEN
            dbms_output.put_line(SQLERRM);
        END;
      END LOOP;
      COMMIT;
  
    END;
  */
  PROCEDURE delete_db_dup_data(v_cust_id IN VARCHAR2, v_inst IN VARCHAR2) IS
    v_rowid  VARCHAR2(64);
    v_count  NUMBER;
    v_delete VARCHAR2(1000);
    v_where  VARCHAR2(200);
  BEGIN
    /*  
      SELECT COUNT(1)
      INTO v_count
      FROM user_tables
     WHERE table_name = 'DEL_ROWID';
    IF v_count = 0 THEN
      EXECUTE IMMEDIATE 'create table DEL_ROWID (tab_name varchar2(30),rowdata rowid)';
    END IF;
    EXECUTE IMMEDIATE 'truncate table DEL_ROWID';
    */
    FOR DB IN (SELECT table_name
                 FROM user_tables
                WHERE (table_name LIKE 'DDSC_DB_%' OR
                      table_name LIKE 'TOP5_%')
                  AND table_name NOT IN
                      (SELECT table_name FROM user_external_tables)) LOOP
      BEGIN
        IF v_inst IS NOT NULL THEN
          v_where := ' and inst_name=' || '''' || v_inst || '''';
        END IF;
        v_delete := NULL;
        --TOP5 --cust_id,hst_name,to_char(sample_time, 'yyyy/mm/dd hh24mi'),sql_id
        --to_char(sample_time, 'yyyy/mm/dd hh24mi')
        IF upper(db.table_name) LIKE 'TOP5_%' THEN
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists  (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi''),sql_id  ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' || v_where ||
                      ') b  WHERE rn <> 1 and a.rowid=b.rowid)';
        
          --NORMAL TABLE cust_id,host,inst_name,to_char(sample_time, 'yyyy/mm/dd hh24mi')
          --to_char(sample_time, 'yyyy/mm/dd hh24mi')
        ELSIF upper(db.table_name) IN
              ('DDSC_DB_INST',
               'DDSC_DB_LIB',
               'DDSC_DB_DICT',
               /*'DDSC_DB_LOGSW',*/
               'DDSC_DB_PGA',
               'DDSC_DB_SESS',
               'DDSC_DB_SHARE',
               'DDSC_DB_SIZE',
               'DDSC_DB_SYSSTAT',
               'DDSC_DB_TEMP') THEN
        
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi'')    ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' || v_where ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
        
          --DDSC_DB_LOGSW
        ELSIF upper(db.table_name) = 'DDSC_DB_LOGSW' THEN
          v_delete := 'DELETE FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(log_date, ''yyyy/mm/dd hh24'')    ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
        
          --DDSC_DB_DF     --cust_id,host,inst_name,to_char(sample_time, 'yyyy/mm/dd hh24mi'),file_id
        ELSIF upper(db.table_name) = 'DDSC_DB_DF' THEN
        
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi''),file_id    ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' || v_where ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
        
          --DDSC_DB_IDX   cust_id,host,inst_name,to_char(sample_time, 'yyyy/mm/dd hh24mi'),owner,index_name
        ELSIF upper(db.table_name) = 'DDSC_DB_IDX' THEN
        
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi''),owner,index_name      ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' || v_where ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
          --DDSC_DB_ALERT   cust_id host inst_name alert_time  text
        ELSIF upper(db.table_name) = 'DDSC_DB_ALERT' THEN
        
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(alert_time, ''yyyy/mm/dd hh24miss''),text      ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' || v_where ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
          --DDSC_DB_BAK   cust_id host inst_name start_time input_type
        ELSIF upper(db.table_name) = 'DDSC_DB_BAK' THEN
        
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(start_time, ''yyyy/mm/dd hh24miss''), input_type       ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' || v_where ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
          --DDSC_DB_OBJ   cust_id,host,inst_name,to_char(sample_time, 'yyyy/mm/dd hh24mi'),owner,object_type,object_name
        ELSIF upper(db.table_name) = 'DDSC_DB_OBJ' THEN
        
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd''),owner,object_type,object_name      ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' || v_where ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
        
          --DDSC_DB_SEG   cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi''),owner,segment_name
        ELSIF upper(db.table_name) = 'DDSC_DB_SEG' THEN
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd''),owner,segment_name      ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' || v_where ||
                      ')  b WHERE rn <> 1 and a.rowid=b.rowid)';
        
          --DDSC_DB_SGACOM  --cust_id host inst_name to_char(sample_time, ''yyyy/mm/dd hh24mi'') component
        ELSIF upper(db.table_name) = 'DDSC_DB_SGACOM' THEN
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi''),component       ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' || v_where ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
        
          --DDSC_DB_SGARS   --cust_id host inst_name to_char(sample_time, ''yyyy/mm/dd hh24mi'') component
        ELSIF upper(db.table_name) = 'DDSC_DB_SGARS' THEN
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT ROWID FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi''),component      ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' || v_where ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
          --DDSC_DB_TBS  cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd''),tablespace
        ELSIF upper(db.table_name) = 'DDSC_DB_TBS' THEN
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||v_where||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd''),tablespace      ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' || v_where ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
          --DDSC_DB_UNDO    --cust_id host inst_name sample_time sid serial# program machine
        ELSIF upper(db.table_name) = 'DDSC_DB_UNDO' THEN
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi''),sid,serial#,program,machine      ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' || v_where ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
        
          --DDSC_DB_PAR --cust_id host inst_name to_char(sample_time, ''yyyy/mm/dd''),name
        
        ELSIF upper(db.table_name) = 'DDSC_DB_PAR' THEN
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd''),name      ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' || v_where ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
        
        END IF;
        --dbms_output.put_line(v_insert);
        EXECUTE IMMEDIATE v_delete;
        COMMIT;
      EXCEPTION
        WHEN OTHERS THEN
          dbms_output.put_line(SQLERRM);
      END;
    
    END LOOP;
    COMMIT;
  
  END;

  PROCEDURE delete_db_dup_data(v_cust_id IN VARCHAR2) IS
    v_rowid  VARCHAR2(64);
    v_count  NUMBER;
    v_delete VARCHAR2(500);
    v_del    VARCHAR2(500);
  BEGIN
  
    FOR DB IN (SELECT table_name
                 FROM user_tables
                WHERE (table_name LIKE 'DDSC_DB_%' OR
                      table_name LIKE 'TOP5_%')
                  AND table_name NOT IN
                      (SELECT table_name FROM user_external_tables)) LOOP
      BEGIN
      
        v_delete := NULL;
        --TOP5 --cust_id,hst_name,to_char(sample_time, 'yyyy/mm/dd hh24mi'),sql_id
        --to_char(sample_time, 'yyyy/mm/dd hh24mi')
        IF upper(db.table_name) LIKE 'TOP5_%' THEN
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists  (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi''),sql_id  ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' ||
                      ') b  WHERE rn <> 1 and a.rowid=b.rowid)';
        
          --NORMAL TABLE cust_id,host,inst_name,to_char(sample_time, 'yyyy/mm/dd hh24mi')
          --to_char(sample_time, 'yyyy/mm/dd hh24mi')
        ELSIF upper(db.table_name) IN
              ('DDSC_DB_INST',
               'DDSC_DB_LIB',
               'DDSC_DB_DICT',
               'DDSC_DB_PGA',
               'DDSC_DB_SESS',
               'DDSC_DB_SHARE',
               'DDSC_DB_SIZE',
               'DDSC_DB_SYSSTAT',
               'DDSC_DB_TEMP') THEN
        
          v_delete := 'DELETE FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi'')    ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
        
          --DDSC_DB_LOGSW
        ELSIF upper(db.table_name) = 'DDSC_DB_LOGSW' THEN
          v_delete := 'DELETE FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(log_date, ''yyyy/mm/dd hh24'')    ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
        
          --DDSC_DB_DF     --cust_id,host,inst_name,to_char(sample_time, 'yyyy/mm/dd hh24mi'),file_id
        ELSIF upper(db.table_name) = 'DDSC_DB_DF' THEN
        
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi''),file_id    ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
        
          --DDSC_DB_IDX   cust_id,host,inst_name,to_char(sample_time, 'yyyy/mm/dd hh24mi'),owner,index_name
        ELSIF upper(db.table_name) = 'DDSC_DB_IDX' THEN
        
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi''),owner,index_name      ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
          --DDSC_DB_ALERT   cust_id host inst_name alert_time  text
        ELSIF upper(db.table_name) = 'DDSC_DB_ALERT' THEN
        
          v_delete := 'DELETE FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(alert_time, ''yyyy/mm/dd hh24miss''),text      ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
          --DDSC_DB_BAK   cust_id host inst_name start_time input_type
        ELSIF upper(db.table_name) = 'DDSC_DB_BAK' THEN
        
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(start_time, ''yyyy/mm/dd hh24miss''), input_type       ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
          --DDSC_DB_OBJ   cust_id,host,inst_name,to_char(sample_time, 'yyyy/mm/dd hh24mi'),owner,object_type,object_name
        ELSIF upper(db.table_name) = 'DDSC_DB_OBJ' THEN
        
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd''),owner,object_type,object_name      ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
        
          --DDSC_DB_SEG   cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi''),owner,segment_name
        ELSIF upper(db.table_name) = 'DDSC_DB_SEG' THEN
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd''),owner,segment_name      ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' ||
                      ')  b WHERE rn <> 1 and a.rowid=b.rowid)';
        
          --DDSC_DB_SGACOM  --cust_id host inst_name to_char(sample_time, ''yyyy/mm/dd hh24mi'') component
        ELSIF upper(db.table_name) = 'DDSC_DB_SGACOM' THEN
          v_delete := 'DELETE FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi''),component       ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
        
          --DDSC_DB_SGARS   --cust_id host inst_name to_char(sample_time, ''yyyy/mm/dd hh24mi'') component
        ELSIF upper(db.table_name) = 'DDSC_DB_SGARS' THEN
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT ROWID FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi''),component      ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
          --DDSC_DB_TBS  cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd''),tablespace
        ELSIF upper(db.table_name) = 'DDSC_DB_TBS' THEN
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd''),tablespace      ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
          --DDSC_DB_UNDO    --cust_id host inst_name sample_time sid serial# program machine
        ELSIF upper(db.table_name) = 'DDSC_DB_UNDO' THEN
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd hh24mi''),sid,serial#,program,machine      ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
        
          --DDSC_DB_PAR --cust_id host inst_name to_char(sample_time, ''yyyy/mm/dd''),name
        
        ELSIF upper(db.table_name) = 'DDSC_DB_PAR' THEN
          v_delete := 'DELETE  FROM ' || db.table_name ||
                      ' a where cust_id =' || '''' || v_cust_id || '''' ||
                      ' AND exists (SELECT 1 FROM (SELECT ROWID,dense_rank() over(PARTITION BY cust_id,host,inst_name,to_char(sample_time, ''yyyy/mm/dd''),name      ORDER BY ROWID) rn FROM ' ||
                      db.table_name || ' WHERE cust_id =' || '''' ||
                      v_cust_id || '''' ||
                      ') b WHERE rn <> 1 and a.rowid=b.rowid)';
        
        END IF;
        --dbms_output.put_line(v_delete);
        EXECUTE IMMEDIATE v_delete;
        COMMIT;
      EXCEPTION
        WHEN OTHERS THEN
          dbms_output.put_line(SQLERRM);
      END;
    
    END LOOP;
    COMMIT;
  END;

  PROCEDURE change_cust_id(old_cust_id IN VARCHAR2,
                           new_cust_id IN VARCHAR2) IS
    v_sql VARCHAR2(1000);
  BEGIN
    FOR c IN (SELECT *
                FROM user_tables
               WHERE (table_name LIKE ('DDSC_OS%') OR
                     table_name LIKE ('DDSC_DB%') OR
                     table_name LIKE ('TOP5%'))
                 AND table_name NOT LIKE '%_EXT') LOOP
      v_sql := 'update ' || c.table_name || ' set CUST_ID=' || '''' ||
               upper(new_cust_id) || '''' || ' where upper(cust_id)=' || '''' ||
               upper(old_cust_id) || '''';
      --dbms_output.put_line(v_sql);
      EXECUTE IMMEDIATE v_sql;
      COMMIT;
    END LOOP;
  END;

  PROCEDURE count_data IS
    p_count NUMBER;
    p_run   VARCHAR2(500);
    cc      SYS_REFCURSOR;
  BEGIN
  
    FOR c IN (SELECT *
                FROM user_tables
               WHERE table_name NOT IN ('DDSC_CUSTMAIN')
                 AND table_name NOT LIKE '%_EXT') LOOP
    
      BEGIN
        p_run := 'select count(1) from ' || c.table_name;
      
        OPEN cc FOR p_run;
        FETCH cc
          INTO p_count;
      
        -- if p_count != 0 then
        dbms_output.put_line(c.table_name || ' ===> ' || p_count);
        --else
        --  null;
        --end if;
      EXCEPTION
        WHEN OTHERS THEN
          dbms_output.put_line('ERROR TABLE:!!!' || c.table_name);
        
      END;
    END LOOP;
  END;

  FUNCTION monitor_bf_cache(cust_id IN VARCHAR2,
                            v_btime IN VARCHAR2 DEFAULT NULL,
                            v_etime IN VARCHAR2 DEFAULT NULL) RETURN bc_hit
    PIPELINED IS
    pre_phy_rd_cache              ddsc_db_sysstat.physical_reads_cache%TYPE;
    pre_const_gets_from_cache     ddsc_db_sysstat.consistent_gets_from_cache%TYPE;
    pre_db_block_gets_from_cache  ddsc_db_sysstat.db_block_gets_from_cache%TYPE;
    diff_phy_rd_cache             ddsc_db_sysstat.physical_reads_cache%TYPE;
    diff_const_gets_from_cache    ddsc_db_sysstat.consistent_gets_from_cache%TYPE;
    diff_db_block_gets_from_cache ddsc_db_sysstat.db_block_gets_from_cache%TYPE;
    out_buff                      bc_hit_type;
    --i number;
    buff_hit NUMBER;
  
    -- rec_c_cache ddsc.buffer_cache_hit%rowtype;
    CURSOR c_cache(cust_id IN VARCHAR2,
                   v_btime IN VARCHAR2 DEFAULT NULL,
                   v_etime IN VARCHAR2 DEFAULT NULL) IS
      SELECT *
        FROM ddsc_db_sysstat a
       WHERE a.cust_id = cust_id
         AND a.sample_time BETWEEN v_btime AND v_etime
       ORDER BY sample_time;
  
    rec_c_cache c_cache%ROWTYPE;
  
  BEGIN
    IF v_btime IS NULL AND v_etime IS NULL THEN
      OPEN c_cache(cust_id,
                   trunc(SYSDATE, 'MM'),
                   add_months(trunc(SYSDATE, 'MM'), 1));
      dbms_output.put_line(cust_id || trunc(SYSDATE, 'MM') ||
                           add_months(trunc(SYSDATE, 'MM'), 1));
    ELSIF v_btime IS NOT NULL AND v_etime IS NULL THEN
      OPEN c_cache(cust_id,
                   to_date(v_btime, 'yyyymm'),
                   add_months(to_date(v_btime, 'yyyymm'), 1));
    ELSIF length(v_btime) = 8 AND length(v_etime) = 8 THEN
      OPEN c_cache(cust_id,
                   to_date(v_btime, 'yyyymmdd'),
                   to_date(v_etime, 'yyyymmdd'));
    ELSE
      OPEN c_cache(cust_id,
                   to_date(v_btime, 'yyyymm'),
                   add_months(to_date(v_etime, 'yyyymm'), 1));
    END IF;
    LOOP
      FETCH c_cache
        INTO rec_c_cache;
      EXIT WHEN c_cache%NOTFOUND;
      --i:=i+1;
      diff_phy_rd_cache             := rec_c_cache.physical_reads_cache -
                                       pre_phy_rd_cache;
      diff_const_gets_from_cache    := rec_c_cache.consistent_gets_from_cache -
                                       pre_const_gets_from_cache;
      diff_db_block_gets_from_cache := rec_c_cache.db_block_gets_from_cache -
                                       pre_db_block_gets_from_cache;
    
      IF diff_phy_rd_cache < 0 OR diff_const_gets_from_cache < 0 OR
         diff_db_block_gets_from_cache < 0 THEN
        diff_phy_rd_cache             := rec_c_cache.physical_reads_cache;
        diff_const_gets_from_cache    := rec_c_cache.consistent_gets_from_cache;
        diff_db_block_gets_from_cache := rec_c_cache.db_block_gets_from_cache;
      
        buff_hit := round((1 - (diff_phy_rd_cache /
                          (diff_const_gets_from_cache +
                          diff_db_block_gets_from_cache))) * 100,
                          3);
      
        out_buff.SAMPLE_TIME := rec_c_cache.sample_time;
        out_buff.HIT_RATE    := buff_hit;
      
        PIPE ROW(out_buff);
      
      ELSIF diff_phy_rd_cache IS NULL AND
            diff_const_gets_from_cache IS NULL AND
            diff_const_gets_from_cache IS NULL THEN
      
        diff_phy_rd_cache             := rec_c_cache.physical_reads_cache;
        diff_const_gets_from_cache    := rec_c_cache.consistent_gets_from_cache;
        diff_db_block_gets_from_cache := rec_c_cache.db_block_gets_from_cache;
        buff_hit                      := round((1 -
                                               (diff_phy_rd_cache /
                                               (diff_const_gets_from_cache +
                                               diff_db_block_gets_from_cache))) * 100,
                                               3);
        out_buff.SAMPLE_TIME          := rec_c_cache.sample_time;
        out_buff.HIT_RATE             := buff_hit;
        PIPE ROW(out_buff);
      
      ELSE
        diff_phy_rd_cache             := rec_c_cache.physical_reads_cache -
                                         pre_phy_rd_cache;
        diff_const_gets_from_cache    := rec_c_cache.consistent_gets_from_cache -
                                         pre_const_gets_from_cache;
        diff_db_block_gets_from_cache := rec_c_cache.db_block_gets_from_cache -
                                         pre_db_block_gets_from_cache;
      
        buff_hit             := round((1 - (diff_phy_rd_cache /
                                      (diff_const_gets_from_cache +
                                      diff_db_block_gets_from_cache))) * 100,
                                      3);
        out_buff.SAMPLE_TIME := rec_c_cache.sample_time;
        out_buff.HIT_RATE    := buff_hit;
      
        PIPE ROW(out_buff);
      
      END IF;
      pre_phy_rd_cache             := rec_c_cache.physical_reads_cache;
      pre_const_gets_from_cache    := rec_c_cache.consistent_gets_from_cache;
      pre_db_block_gets_from_cache := rec_c_cache.db_block_gets_from_cache;
    END LOOP;
    CLOSE c_cache;
  END monitor_bf_cache;

  FUNCTION monitor_dd_cache(cust_id IN VARCHAR2,
                            v_btime IN VARCHAR2 DEFAULT NULL,
                            v_etime IN VARCHAR2 DEFAULT NULL)
    RETURN dd_hit_table
    PIPELINED IS
    pre_gets       ddsc_db_dict.gets%TYPE;
    pre_getmisses  ddsc_db_dict.getmisses%TYPE;
    diff_gets      ddsc_db_dict.gets%TYPE;
    diff_getmisses ddsc_db_dict.getmisses%TYPE;
    out_buff       dd_hit_type;
  
    dd_hit NUMBER;
  
    CURSOR c_dd_cache(cust_id IN VARCHAR2,
                      v_btime IN VARCHAR2 DEFAULT NULL,
                      v_etime IN VARCHAR2 DEFAULT NULL) IS
      SELECT * FROM ddsc_db_dict ORDER BY sample_time;
    rec_c_dd_cache c_dd_cache%ROWTYPE;
  
  BEGIN
    IF v_btime IS NULL AND v_etime IS NULL THEN
      OPEN c_dd_cache(cust_id,
                      trunc(SYSDATE, 'MM'),
                      add_months(trunc(SYSDATE, 'MM'), 1));
      dbms_output.put_line(cust_id || trunc(SYSDATE, 'MM') ||
                           add_months(trunc(SYSDATE, 'MM'), 1));
    ELSIF v_btime IS NOT NULL AND v_etime IS NULL THEN
      OPEN c_dd_cache(cust_id,
                      to_date(v_btime, 'yyyymm'),
                      add_months(to_date(v_btime, 'yyyymm'), 1));
    ELSIF length(v_btime) = 8 AND length(v_etime) = 8 THEN
      OPEN c_dd_cache(cust_id,
                      to_date(v_btime, 'yyyymmdd'),
                      to_date(v_etime, 'yyyymmdd'));
    ELSE
      OPEN c_dd_cache(cust_id,
                      to_date(v_btime, 'yyyymm'),
                      add_months(to_date(v_etime, 'yyyymm'), 1));
    END IF;
  
    --OPEN c_dd_cache;
    LOOP
      FETCH c_dd_cache
        INTO rec_c_dd_cache;
      EXIT WHEN c_dd_cache%NOTFOUND;
    
      diff_gets      := rec_c_dd_cache.gets - pre_gets;
      diff_getmisses := rec_c_dd_cache.getmisses - pre_getmisses;
    
      IF diff_gets < 0 OR diff_getmisses < 0 THEN
        diff_gets      := rec_c_dd_cache.gets;
        diff_getmisses := rec_c_dd_cache.getmisses;
      
        dd_hit               := round((1 - (diff_getmisses / diff_gets)) * 100,
                                      3);
        out_buff.SAMPLE_TIME := rec_c_dd_cache.sample_time;
        out_buff.HIT_RATE    := dd_hit;
        PIPE ROW(out_buff);
      
      ELSIF diff_getmisses = 0 THEN
        dd_hit         := 100;
        diff_gets      := rec_c_dd_cache.gets - pre_gets;
        diff_getmisses := rec_c_dd_cache.getmisses - pre_getmisses;
      
        out_buff.SAMPLE_TIME := rec_c_dd_cache.sample_time;
        out_buff.HIT_RATE    := dd_hit;
        PIPE ROW(out_buff);
      
      ELSIF diff_gets IS NULL AND diff_getmisses IS NULL THEN
      
        diff_gets      := rec_c_dd_cache.gets;
        diff_getmisses := rec_c_dd_cache.getmisses;
      
        dd_hit               := round((1 - (diff_getmisses / diff_gets)) * 100,
                                      3);
        out_buff.SAMPLE_TIME := rec_c_dd_cache.sample_time;
        out_buff.HIT_RATE    := dd_hit;
        PIPE ROW(out_buff);
      
      ELSE
        diff_gets      := rec_c_dd_cache.gets - pre_gets;
        diff_getmisses := rec_c_dd_cache.getmisses - pre_getmisses;
      
        dd_hit               := round((1 - (diff_getmisses / diff_gets)) * 100,
                                      3);
        out_buff.SAMPLE_TIME := rec_c_dd_cache.sample_time;
        out_buff.HIT_RATE    := dd_hit;
      
        PIPE ROW(out_buff);
      
      END IF;
      pre_gets      := rec_c_dd_cache.gets;
      pre_getmisses := rec_c_dd_cache.getmisses;
    
    END LOOP;
    CLOSE c_dd_cache;
  END monitor_dd_cache;

  FUNCTION monitor_lib_cache(cust_id IN VARCHAR2,
                             v_btime IN VARCHAR2 DEFAULT NULL,
                             v_etime IN VARCHAR2 DEFAULT NULL)
    RETURN lib_hit_table
    PIPELINED IS
    pre_pins     ddsc_db_lib.pins%TYPE;
    pre_pinhits  ddsc_db_lib.pinhits%TYPE;
    diff_pins    ddsc_db_lib.pins%TYPE;
    diff_pinhits ddsc_db_lib.pinhits%TYPE;
    out_buff     lib_hit_type;
    lib_hit      NUMBER;
  
    CURSOR c_lib_cache(cust_id IN VARCHAR2,
                       
                       v_btime IN VARCHAR2 DEFAULT NULL,
                       v_etime IN VARCHAR2 DEFAULT NULL) IS
      SELECT * FROM ddsc_db_lib ORDER BY sample_time;
    rec_c_lib_cache c_lib_cache%ROWTYPE;
  
  BEGIN
    IF v_btime IS NULL AND v_etime IS NULL THEN
      OPEN c_lib_cache(cust_id,
                       trunc(SYSDATE, 'MM'),
                       add_months(trunc(SYSDATE, 'MM'), 1));
      dbms_output.put_line(cust_id || trunc(SYSDATE, 'MM') ||
                           add_months(trunc(SYSDATE, 'MM'), 1));
    ELSIF v_btime IS NOT NULL AND v_etime IS NULL THEN
      OPEN c_lib_cache(cust_id,
                       to_date(v_btime, 'yyyymm'),
                       add_months(to_date(v_btime, 'yyyymm'), 1));
    ELSIF length(v_btime) = 8 AND length(v_etime) = 8 THEN
      OPEN c_lib_cache(cust_id,
                       to_date(v_btime, 'yyyymmdd'),
                       to_date(v_etime, 'yyyymmdd'));
    ELSE
      OPEN c_lib_cache(cust_id,
                       to_date(v_btime, 'yyyymm'),
                       add_months(to_date(v_etime, 'yyyymm'), 1));
    END IF;
    --OPEN c_lib_cache;
    LOOP
      FETCH c_lib_cache
        INTO rec_c_lib_cache;
      EXIT WHEN c_lib_cache%NOTFOUND;
    
      diff_pins    := rec_c_lib_cache.pins - pre_pins;
      diff_pinhits := rec_c_lib_cache.pinhits - pre_pinhits;
    
      IF diff_pins < 0 OR diff_pinhits < 0 THEN
        diff_pins    := rec_c_lib_cache.pins;
        diff_pinhits := rec_c_lib_cache.pinhits;
      
        lib_hit              := round(((diff_pinhits / diff_pins)) * 100, 3);
        out_buff.SAMPLE_TIME := rec_c_lib_cache.sample_time;
        out_buff.HIT_RATE    := lib_hit;
        PIPE ROW(out_buff);
      
      ELSIF diff_pins IS NULL AND diff_pinhits IS NULL THEN
      
        diff_pins            := rec_c_lib_cache.pins;
        diff_pinhits         := rec_c_lib_cache.pinhits;
        lib_hit              := round(((diff_pinhits / diff_pins)) * 100, 3);
        out_buff.SAMPLE_TIME := rec_c_lib_cache.sample_time;
        out_buff.HIT_RATE    := lib_hit;
        PIPE ROW(out_buff);
      
      ELSE
        diff_pins    := rec_c_lib_cache.pins - pre_pins;
        diff_pinhits := rec_c_lib_cache.pinhits - pre_pinhits;
      
        lib_hit              := round(((diff_pinhits / diff_pins)) * 100, 3);
        out_buff.SAMPLE_TIME := rec_c_lib_cache.sample_time;
        out_buff.HIT_RATE    := lib_hit;
      
        PIPE ROW(out_buff);
      
      END IF;
      pre_pins    := rec_c_lib_cache.pins;
      pre_pinhits := rec_c_lib_cache.pinhits;
    
    END LOOP;
    CLOSE c_lib_cache;
  END monitor_lib_cache;

  /*FUNCTION monitor_all_hit(cust_id IN VARCHAR2,
                            v_btime IN VARCHAR2 DEFAULT NULL,
                            v_etime IN VARCHAR2 DEFAULT NULL) RETURN bc_hit
    PIPELINED IS
    out_buff     db_hit_type;
    buff_hit     NUMBER;
    lib_hit      NUMBER;
    dd_hit       NUMBER;
  BEGIN
  out_buff.bf_hit:=monitor_bf_cache(cust_id,v_btime,v_etime);
  out_buff.lib_hit:=monitor_dd_cache(cust_id,v_btime,v_etime);
  out_buff.lib_hit:=monitor_lib_cache(cust_id,v_btime,v_etime);
  
    PIPE ROW(out_buff);
  END monitor_all_hit;*/

  PROCEDURE nmon_rawdata_imp(v_cust_id   IN VARCHAR2,
                             v_directory IN VARCHAR2) IS
    v_input_file UTL_FILE.file_type;
    v_ifilename  VARCHAR2(100);
    v_rec        VARCHAR2(32766);
    v_crttab     VARCHAR2(5000);
    v_inst_sql   VARCHAR2(5000);
    v_spilt1     VARCHAR2(100);
    v_spilt2     VARCHAR2(100);
    v_time       VARCHAR2(500);
    strlen       NUMBER;
    v_timetable  VARCHAR2(30);
    v_timet_cnt  NUMBER;
    v_os         VARCHAR2(10);
  BEGIN
    --CREATE TIMEFLAG TABLE
    v_timetable := 'NMON_TIME';
    SELECT COUNT(1)
      INTO v_timet_cnt
      FROM user_tables
     WHERE table_name = 'NMON_TIME';
    IF v_timet_cnt = 0 THEN
      EXECUTE IMMEDIATE 'create table ' || v_timetable ||
                        '(timeflag varchar2(8),nmon_date date)';
    END IF;
  
    --execute  immediate   'create table NMON_TIME (timeflag varchar2(8),nmon_date date)';
    dbasrv_tool.nmon_tab_drop;
    EXECUTE IMMEDIATE 'truncate table nmon_time';
    FOR iname IN (SELECT basename(column_value, NULL, '\') "NAME"
                    FROM TABLE(list_files(v_directory))
                   WHERE column_value LIKE '%.nmon') LOOP
      v_input_file := utl_file.fopen(v_directory, iname.name, 'R', 32767);
      BEGIN
        LOOP
          BEGIN
            utl_file.get_line(v_input_file, v_rec, 32767);
          
            --GET HOSTNAME
            IF split(v_rec, 1, ',') || split(v_rec, 2, ',') = 'AAAhost' THEN
              v_hostname := split(v_rec, 3, ',');
            
            ELSIF split(v_rec, 1, ',') || split(v_rec, 2, ',') =
                  'AAArunname' THEN
              v_hostname := split(v_rec, 3, ',');
            
              --GET OS PLATFORM
            ELSIF upper(split(v_rec, 1, ',') || split(v_rec, 2, ',') ||
                        split(v_rec, 3, ',')) = 'AAAOSLINUX' THEN
              v_os := 'Linux';
              dbms_output.put_line(v_os);
            ELSIF upper(split(v_rec, 1, ',') || split(v_rec, 2, ',')) =
                  'AAAAIX' THEN
              v_os := 'AIX';
              dbms_output.put_line(v_os);
            
              ----CREATE TABLE
              --DISKREAD
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'DISKREAD%,Disk%' THEN
              v_spilt1 := split(v_rec, 1, ',');
              v_spilt2 := split(v_rec, 2, ',');
              SELECT length(v_spilt1 || ',' || v_spilt2)
                INTO strlen
                FROM dual;
              SELECT 'create table ' || v_spilt1 ||
                     ' (timeflag varchar2(8),' ||
                     REPLACE(substr(v_rec, strlen + 2), ',', ' number ,') ||
                     ' number)'
                INTO v_crttab
                FROM dual;
              v_crttab := REPLACE(v_crttab, '-', '_');
              /* dbms_output.put_line(strlen);
              dbms_output.put_line(v_spilt1);
              dbms_output.put_line(v_spilt2);
              dbms_output.put_line(v_spilt1 || ',' || v_spilt2);*/
              --dbms_output.put_line(v_crttab);
              --##REPLACE "/" for LINUX nmon
              --EXECUTE IMMEDIATE v_crttab;
              EXECUTE IMMEDIATE REPLACE(v_crttab, '/', '');
              --DISKWRITE
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'DISKWRITE%,Disk%' THEN
              v_spilt1 := split(v_rec, 1, ',');
              v_spilt2 := split(v_rec, 2, ',');
              SELECT length(v_spilt1 || ',' || v_spilt2)
                INTO strlen
                FROM dual;
              SELECT 'create table ' || v_spilt1 ||
                     ' (timeflag varchar2(8),' ||
                     REPLACE(substr(v_rec, strlen + 2), ',', ' number ,') ||
                     ' number)'
                INTO v_crttab
                FROM dual;
              v_crttab := REPLACE(v_crttab, '-', '_');
              /* dbms_output.put_line(strlen);
              dbms_output.put_line(v_spilt1);
              dbms_output.put_line(v_spilt2);
              dbms_output.put_line(v_spilt1 || ',' || v_spilt2);
              dbms_output.put_line(v_crttab);*/
              --##REPLACE "/" for LINUX nmon
              -- EXECUTE IMMEDIATE v_crttab;
              EXECUTE IMMEDIATE REPLACE(v_crttab, '/', '');
              --DISKXFER
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'DISKXFER%,Disk%' THEN
              v_spilt1 := split(v_rec, 1, ',');
              v_spilt2 := split(v_rec, 2, ',');
              SELECT length(v_spilt1 || ',' || v_spilt2)
                INTO strlen
                FROM dual;
              SELECT 'create table ' || v_spilt1 ||
                     ' (timeflag varchar2(8),' ||
                     REPLACE(substr(v_rec, strlen + 2), ',', ' number ,') ||
                     ' number)'
                INTO v_crttab
                FROM dual;
              v_crttab := REPLACE(v_crttab, '-', '_');
              /*   dbms_output.put_line(strlen);
              dbms_output.put_line(v_spilt1);
              dbms_output.put_line(v_spilt2);
              dbms_output.put_line(v_spilt1 || ',' || v_spilt2);
              dbms_output.put_line(v_crttab);*/
              --##REPLACE "/" for LINUX nmon
              -- EXECUTE IMMEDIATE v_crttab;
              EXECUTE IMMEDIATE REPLACE(v_crttab, '/', '');
            END IF;
          
            ---INSERT SQL
            --INSERT DISKREAD DATA
            IF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
               'DISKREAD%,T%' THEN
              v_spilt1 := split(v_rec, 1, ',');
              v_spilt2 := split(v_rec, 2, ',');
              SELECT length(v_spilt1 || ',' || v_spilt2)
                INTO strlen
                FROM dual;
              v_inst_sql := ' insert into ' || v_spilt1 || ' values(''' ||
                            v_spilt2 || ''',' || substr(v_rec, strlen + 2) || ')';
              EXECUTE IMMEDIATE v_inst_sql;
              --dbms_output.put_line('diskread: '||v_inst_sql);
            
              --INSERT DISKWRITE DATA
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'DISKWRITE%,T%' THEN
              v_spilt1 := split(v_rec, 1, ',');
              v_spilt2 := split(v_rec, 2, ',');
              SELECT length(v_spilt1 || ',' || v_spilt2)
                INTO strlen
                FROM dual;
              v_inst_sql := ' insert into ' || v_spilt1 || ' values(''' ||
                            v_spilt2 || ''',' || substr(v_rec, strlen + 2) || ')';
              EXECUTE IMMEDIATE v_inst_sql;
              --dbms_output.put_line(v_inst_sql);
              --INSERT DISKXFER DATA
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'DISKXFER%,T%' THEN
              v_spilt1 := split(v_rec, 1, ',');
              v_spilt2 := split(v_rec, 2, ',');
              SELECT length(v_spilt1 || ',' || v_spilt2)
                INTO strlen
                FROM dual;
              v_inst_sql := ' insert into ' || v_spilt1 || ' values(''' ||
                            v_spilt2 || ''',' || substr(v_rec, strlen + 2) || ')';
              EXECUTE IMMEDIATE v_inst_sql;
              --dbms_output.put_line(v_inst_sql);
              --INSET TIME DATA
            ELSIF split(v_rec, 1, ',') = 'ZZZZ' THEN
              v_time := split(v_rec, 4, ',') || ' ' || split(v_rec, 3, ',');
              --dbms_output.put_line(v_time);
              v_inst_sql := 'insert into ' || v_timetable || ' values (''' ||
                            split(v_rec, 2, ',') || ''',to_date(''' ||
                            v_time || '''' ||
                            ',''DD-MON-YYYY HH24:MI:SS''))' || '';
              EXECUTE IMMEDIATE v_inst_sql;
              --dbms_output.put_line(v_inst_sql);
              --FOR CPU_ALL DATA
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'CPU_ALL,T%' THEN
              v_spilt1 := split(v_rec, 1, ',');
              v_spilt2 := split(v_rec, 2, ',');
              SELECT length(v_spilt1 || ',' || v_spilt2)
                INTO strlen
                FROM dual;
              --modify nmon version,16beta linux --> CPU_ALL add  column Steal%  20160909
              --substr(v_rec, strlen + 2)  -->  replace(substr(v_rec, strlen + 2),',0.0,,',',,')
              v_inst_sql := ' insert into TMP_CPUALL ' || ' values(''' ||
                            v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
                            v_spilt2 || ''',' || replace(substr(v_rec, strlen + 2),',0.0,,',',,') || ')';
                           -- mark on 20160909 v_spilt2 || ''',' || substr(v_rec, strlen + 2) || ')';
              --EXECUTE IMMEDIATE v_inst_sql;
              v_inst_sql := REPLACE(v_inst_sql, ',,', ',null,');
              EXECUTE IMMEDIATE v_inst_sql;
              --dbms_output.put_line('CPU_SQL:' || v_inst_sql);
              --INSERT INTO DDSC_OS_CPU
              INSERT INTO ddsc_os_cpu
                (cust_id,
                 host,
                 sample_time,
                 user_pct,
                 sys_pct,
                 iowait,
                 idle,
                 busy,
                 num_cpus)
                SELECT c.cust_id,
                       c.host,
                       nt.nmon_date,
                       c.user_pct,
                       c.sys_pct,
                       c.iowait,
                       c.idle,
                       c.busy,
                       c.num_cpus
                  FROM tmp_cpuall c, NMON_TIME nt
                 WHERE c.timeflag = nt.timeflag;
              COMMIT;
            
              --MEMORY
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'MEM,T%' THEN
              IF v_os = 'AIX' THEN
                --BAK 20141009 before add paging column
                /*  v_inst_sql := ' insert into TMP_MEM ' || ' values(''' ||
                v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
                split(v_rec, 2, ',') || ''',' || '''' ||
                split(v_rec, 7, ',') || '''' || ',' || '''' ||
                split(v_rec, 5, ',') || '''' || ')';   */
                v_inst_sql := ' insert into TMP_MEM ' || ' values(''' ||
                              v_cust_id || '''' || ',' || '''' ||
                              v_hostname || '''' || ',' || '''' ||
                              split(v_rec, 2, ',') || ''',' || '''' ||
                              split(v_rec, 7, ',') || '''' || ',' || '''' ||
                              split(v_rec, 5, ',') || '''' || ',' || '''' ||
                              split(v_rec, 8, ',') || '''' || ',' || '''' ||
                              split(v_rec, 6, ',') || '''' || ')';
              
                EXECUTE IMMEDIATE v_inst_sql;
              
              ELSIF v_os = 'Linux' THEN
                v_inst_sql := ' insert into TMP_MEM ' || ' values(''' ||
                              v_cust_id || '''' || ',' || '''' ||
                              v_hostname || '''' || ',' || '''' ||
                              split(v_rec, 2, ',') || ''',' || '''' ||
                              split(v_rec, 3, ',') || '''' || ',' || '''' ||
                              split(v_rec, 7, ',') || '''' || ',' || '''' ||
                              split(v_rec, 6, ',') || '''' || ',' || '''' ||
                              split(v_rec, 10, ',') || '''' || ')';
                EXECUTE IMMEDIATE v_inst_sql;
              END IF;
            
              /*      v_inst_sql := ' insert into TMP_MEM ' || ' values(''' ||
                            v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
                            split(v_rec, 2, ',') || ''',' || '''' ||
                            split(v_rec, 3, ',') || '''' || ',' || '''' ||
                            split(v_rec, 7, ',') || '''' || ')';
                EXECUTE IMMEDIATE v_inst_sql;
              --dbms_output.put_line('MEM_SQL:' || v_inst_sql);*/
            
              --INSERT INTO DDSC_OS_MEM
              INSERT INTO DDSC_OS_MEM
                SELECT m.cust_id,
                       m.host,
                       nt.nmon_date,
                       m.total,
                       m.free,
                       m.paging_total,
                       m.paging_free
                  FROM tmp_mem m, NMON_TIME nt
                 WHERE m.timeflag = nt.timeflag;
              COMMIT;
            
              --PGSP  like ^PAGE(AIX) FOR AIX
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'PAGE,T%' THEN
              v_inst_sql := ' insert into TMP_PGSP ' || ' values(''' ||
                            v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
                            split(v_rec, 2, ',') || ''',' || '''' ||
                            split(v_rec, 6, ',') || '''' || ',' || '''' ||
                            split(v_rec, 7, ',') || '''' || ')';
              EXECUTE IMMEDIATE v_inst_sql;
              --dbms_output.put_line('MEM_SQL:' || v_inst_sql);
              --INSERT INTO DDSC_OS_PGSP
              INSERT INTO DDSC_OS_PGSP
                (cust_id, host, sample_time, PGSIN, PGSOUT)
                SELECT p.cust_id, p.host, nt.nmon_date, p.PGSIN, p.PGSOUT
                  FROM tmp_pgsp p, NMON_TIME nt
                 WHERE p.timeflag = nt.timeflag;
              COMMIT;
            
              --PGSP   FOR LINUX ^VM (LINUX)
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'VM,T%' THEN
              v_inst_sql := ' insert into TMP_PGSP ' || ' values(''' ||
                            v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
                            split(v_rec, 2, ',') || ''',' || '''' ||
                            split(v_rec, 11, ',') || '''' || ',' || '''' ||
                            split(v_rec, 12, ',') || '''' || ')';
              EXECUTE IMMEDIATE v_inst_sql;
              --dbms_output.put_line('MEM_SQL:' || v_inst_sql);
              --INSERT INTO DDSC_OS_PGSP
              INSERT INTO DDSC_OS_PGSP
                (cust_id, host, sample_time, PGSIN, PGSOUT)
                SELECT p.cust_id, p.host, nt.nmon_date, p.PGSIN, p.PGSOUT
                  FROM tmp_pgsp p, NMON_TIME nt
                 WHERE p.timeflag = nt.timeflag;
              COMMIT;
            
            END IF;
          
          EXCEPTION
            WHEN no_data_found THEN
              COMMIT;
              utl_file.fclose(v_input_file);
              EXIT;
            WHEN OTHERS THEN
              COMMIT;
              dbms_output.put_line(SQLERRM);
              DBMS_OUTPUT.put_line(DBMS_UTILITY.format_error_backtrace);
              utl_file.fclose(v_input_file);
              EXIT;
          END;
        END LOOP;
        --After create nmon table and imp nmon data calculate data then import
        --dbasrv_tool.nmon_calc_imp(v_cust_id,v_directory);
        dbasrv_tool.nmon_calc_imp(v_cust_id, v_directory);
      END;
      --initail table
      dbasrv_tool.nmon_tab_drop;
      EXECUTE IMMEDIATE 'truncate table nmon_time';
    
    END LOOP;
    utl_file.fclose(v_input_file);
  
  END;
 
 --RHEL 4  nmon THERE IS NO DISKIO
 PROCEDURE nmon_rawdata_imp_rhel4(v_cust_id   IN VARCHAR2,
                             v_directory IN VARCHAR2) IS
    v_input_file UTL_FILE.file_type;
    v_ifilename  VARCHAR2(100);
    v_rec        VARCHAR2(32766);
    v_crttab     VARCHAR2(5000);
    v_inst_sql   VARCHAR2(5000);
    v_spilt1     VARCHAR2(100);
    v_spilt2     VARCHAR2(100);
    v_time       VARCHAR2(500);
    strlen       NUMBER;
    v_timetable  VARCHAR2(30);
    v_timet_cnt  NUMBER;
    v_os         VARCHAR2(10);
  BEGIN
    --CREATE TIMEFLAG TABLE
    v_timetable := 'NMON_TIME';
    SELECT COUNT(1)
      INTO v_timet_cnt
      FROM user_tables
     WHERE table_name = 'NMON_TIME';
    IF v_timet_cnt = 0 THEN
      EXECUTE IMMEDIATE 'create table ' || v_timetable ||
                        '(timeflag varchar2(8),nmon_date date)';
    END IF;
  
    --execute  immediate   'create table NMON_TIME (timeflag varchar2(8),nmon_date date)';
    dbasrv_tool.nmon_tab_drop;
    EXECUTE IMMEDIATE 'truncate table nmon_time';
    FOR iname IN (SELECT basename(column_value, NULL, '\') "NAME"
                    FROM TABLE(list_files(v_directory))
                   WHERE column_value LIKE '%.nmon') LOOP
      v_input_file := utl_file.fopen(v_directory, iname.name, 'R', 32767);
      BEGIN
        LOOP
          BEGIN
            utl_file.get_line(v_input_file, v_rec, 32767);
          
            --GET HOSTNAME
            IF split(v_rec, 1, ',') || split(v_rec, 2, ',') = 'AAAhost' THEN
              v_hostname := split(v_rec, 3, ',');
            
            ELSIF split(v_rec, 1, ',') || split(v_rec, 2, ',') =
                  'AAArunname' THEN
              v_hostname := split(v_rec, 3, ',');
            
              --GET OS PLATFORM
            ELSIF upper(split(v_rec, 1, ',') || split(v_rec, 2, ',') ||
                        split(v_rec, 3, ',')) = 'AAAOSLINUX' THEN
              v_os := 'Linux';
              dbms_output.put_line(v_os);
            ELSIF upper(split(v_rec, 1, ',') || split(v_rec, 2, ',')) =
                  'AAAAIX' THEN
              v_os := 'AIX';
              dbms_output.put_line(v_os);
            
           
            END IF;
          
            ---INSERT SQL
            --INSET TIME DATA
            IF split(v_rec, 1, ',') = 'ZZZZ' THEN
              v_time := split(v_rec, 4, ',') || ' ' || split(v_rec, 3, ',');
              --dbms_output.put_line(v_time);
              v_inst_sql := 'insert into ' || v_timetable || ' values (''' ||
                            split(v_rec, 2, ',') || ''',to_date(''' ||
                            v_time || '''' ||
                            ',''DD-MON-YYYY HH24:MI:SS''))' || '';
              EXECUTE IMMEDIATE v_inst_sql;
              --dbms_output.put_line(v_inst_sql);
              --FOR CPU_ALL DATA
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'CPU_ALL,T%' THEN
              v_spilt1 := split(v_rec, 1, ',');
              v_spilt2 := split(v_rec, 2, ',');
              SELECT length(v_spilt1 || ',' || v_spilt2)
                INTO strlen
                FROM dual;
              --modify nmon version,16beta linux --> CPU_ALL add  column Steal%  20160909
              --substr(v_rec, strlen + 2)  -->  replace(substr(v_rec, strlen + 2),',0.0,,',',,')
              v_inst_sql := ' insert into TMP_CPUALL ' || ' values(''' ||
                            v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
                            v_spilt2 || ''',' || replace(substr(v_rec, strlen + 2),',0.0,,',',,') || ')';
                           -- mark on 20160909 v_spilt2 || ''',' || substr(v_rec, strlen + 2) || ')';
              --EXECUTE IMMEDIATE v_inst_sql;
              v_inst_sql := REPLACE(v_inst_sql, ',,', ',null,');
              EXECUTE IMMEDIATE v_inst_sql;
              --dbms_output.put_line('CPU_SQL:' || v_inst_sql);
              --INSERT INTO DDSC_OS_CPU
              INSERT INTO ddsc_os_cpu
                (cust_id,
                 host,
                 sample_time,
                 user_pct,
                 sys_pct,
                 iowait,
                 idle,
                 busy,
                 num_cpus)
                SELECT c.cust_id,
                       c.host,
                       nt.nmon_date,
                       c.user_pct,
                       c.sys_pct,
                       c.iowait,
                       c.idle,
                       c.busy,
                       c.num_cpus
                  FROM tmp_cpuall c, NMON_TIME nt
                 WHERE c.timeflag = nt.timeflag;
              COMMIT;
            
              --MEMORY
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'MEM,T%' THEN
              IF v_os = 'AIX' THEN
                --BAK 20141009 before add paging column
                /*  v_inst_sql := ' insert into TMP_MEM ' || ' values(''' ||
                v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
                split(v_rec, 2, ',') || ''',' || '''' ||
                split(v_rec, 7, ',') || '''' || ',' || '''' ||
                split(v_rec, 5, ',') || '''' || ')';   */
                v_inst_sql := ' insert into TMP_MEM ' || ' values(''' ||
                              v_cust_id || '''' || ',' || '''' ||
                              v_hostname || '''' || ',' || '''' ||
                              split(v_rec, 2, ',') || ''',' || '''' ||
                              split(v_rec, 7, ',') || '''' || ',' || '''' ||
                              split(v_rec, 5, ',') || '''' || ',' || '''' ||
                              split(v_rec, 8, ',') || '''' || ',' || '''' ||
                              split(v_rec, 6, ',') || '''' || ')';
              
                EXECUTE IMMEDIATE v_inst_sql;
              
              ELSIF v_os = 'Linux' THEN
                v_inst_sql := ' insert into TMP_MEM ' || ' values(''' ||
                              v_cust_id || '''' || ',' || '''' ||
                              v_hostname || '''' || ',' || '''' ||
                              split(v_rec, 2, ',') || ''',' || '''' ||
                              split(v_rec, 3, ',') || '''' || ',' || '''' ||
                              split(v_rec, 7, ',') || '''' || ',' || '''' ||
                              split(v_rec, 6, ',') || '''' || ',' || '''' ||
                              split(v_rec, 10, ',') || '''' || ')';
                EXECUTE IMMEDIATE v_inst_sql;
              END IF;
            
              /*      v_inst_sql := ' insert into TMP_MEM ' || ' values(''' ||
                            v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
                            split(v_rec, 2, ',') || ''',' || '''' ||
                            split(v_rec, 3, ',') || '''' || ',' || '''' ||
                            split(v_rec, 7, ',') || '''' || ')';
                EXECUTE IMMEDIATE v_inst_sql;
              --dbms_output.put_line('MEM_SQL:' || v_inst_sql);*/
            
              --INSERT INTO DDSC_OS_MEM
              INSERT INTO DDSC_OS_MEM
                SELECT m.cust_id,
                       m.host,
                       nt.nmon_date,
                       m.total,
                       m.free,
                       m.paging_total,
                       m.paging_free
                  FROM tmp_mem m, NMON_TIME nt
                 WHERE m.timeflag = nt.timeflag;
              COMMIT;
            
              --PGSP  like ^PAGE(AIX) FOR AIX
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'PAGE,T%' THEN
              v_inst_sql := ' insert into TMP_PGSP ' || ' values(''' ||
                            v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
                            split(v_rec, 2, ',') || ''',' || '''' ||
                            split(v_rec, 6, ',') || '''' || ',' || '''' ||
                            split(v_rec, 7, ',') || '''' || ')';
              EXECUTE IMMEDIATE v_inst_sql;
              --dbms_output.put_line('MEM_SQL:' || v_inst_sql);
              --INSERT INTO DDSC_OS_PGSP
              INSERT INTO DDSC_OS_PGSP
                (cust_id, host, sample_time, PGSIN, PGSOUT)
                SELECT p.cust_id, p.host, nt.nmon_date, p.PGSIN, p.PGSOUT
                  FROM tmp_pgsp p, NMON_TIME nt
                 WHERE p.timeflag = nt.timeflag;
              COMMIT;
            
              --PGSP   FOR LINUX ^VM (LINUX)
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'VM,T%' THEN
              v_inst_sql := ' insert into TMP_PGSP ' || ' values(''' ||
                            v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
                            split(v_rec, 2, ',') || ''',' || '''' ||
                            split(v_rec, 11, ',') || '''' || ',' || '''' ||
                            split(v_rec, 12, ',') || '''' || ')';
              EXECUTE IMMEDIATE v_inst_sql;
              --dbms_output.put_line('MEM_SQL:' || v_inst_sql);
              --INSERT INTO DDSC_OS_PGSP
              INSERT INTO DDSC_OS_PGSP
                (cust_id, host, sample_time, PGSIN, PGSOUT)
                SELECT p.cust_id, p.host, nt.nmon_date, p.PGSIN, p.PGSOUT
                  FROM tmp_pgsp p, NMON_TIME nt
                 WHERE p.timeflag = nt.timeflag;
              COMMIT;
            
            END IF;
          
          EXCEPTION
            WHEN no_data_found THEN
              COMMIT;
              utl_file.fclose(v_input_file);
              EXIT;
            WHEN OTHERS THEN
              COMMIT;
              dbms_output.put_line(SQLERRM);
              DBMS_OUTPUT.put_line(DBMS_UTILITY.format_error_backtrace);
              utl_file.fclose(v_input_file);
              EXIT;
          END;
        END LOOP;
        --After create nmon table and imp nmon data calculate data then import
        --dbasrv_tool.nmon_calc_imp(v_cust_id,v_directory);
        --dbasrv_tool.nmon_calc_imp(v_cust_id, v_directory);
      END;
      --initail table
      dbasrv_tool.nmon_tab_drop;
      EXECUTE IMMEDIATE 'truncate table nmon_time';
    
    END LOOP;
    utl_file.fclose(v_input_file);
  
  END;

PROCEDURE nmon_rawdata_imp_nfs(v_cust_id   IN VARCHAR2,
                             v_directory IN VARCHAR2) IS
    v_input_file UTL_FILE.file_type;
    v_ifilename  VARCHAR2(100);
    v_rec        VARCHAR2(32766);
    v_crttab     VARCHAR2(5000);
    v_inst_sql   VARCHAR2(5000);
    v_spilt1     VARCHAR2(100);
    v_spilt2     VARCHAR2(100);
    v_time       VARCHAR2(500);
    strlen       NUMBER;
    v_timetable  VARCHAR2(30);
    v_timet_cnt  NUMBER;
    v_os         VARCHAR2(10);
  BEGIN
    --CREATE TIMEFLAG TABLE
    v_timetable := 'NMON_TIME';
    SELECT COUNT(1)
      INTO v_timet_cnt
      FROM user_tables
     WHERE table_name = 'NMON_TIME';
    IF v_timet_cnt = 0 THEN
      EXECUTE IMMEDIATE 'create table ' || v_timetable ||
                        '(timeflag varchar2(8),nmon_date date)';
    END IF;
  
    --execute  immediate   'create table NMON_TIME (timeflag varchar2(8),nmon_date date)';
    dbasrv_tool.nmon_tab_drop;
    EXECUTE IMMEDIATE 'truncate table nmon_time';
    FOR iname IN (SELECT basename(column_value, NULL, '\') "NAME"
                    FROM TABLE(list_files(v_directory))
                   WHERE column_value LIKE '%.nmon') LOOP
      v_input_file := utl_file.fopen(v_directory, iname.name, 'R', 32767);
      BEGIN
        LOOP
          BEGIN
            utl_file.get_line(v_input_file, v_rec, 32767);
          
            --GET HOSTNAME
            IF split(v_rec, 1, ',') || split(v_rec, 2, ',') = 'AAAhost' THEN
              v_hostname := split(v_rec, 3, ',');
            
            ELSIF split(v_rec, 1, ',') || split(v_rec, 2, ',') =
                  'AAArunname' THEN
              v_hostname := split(v_rec, 3, ',');
            
              --GET OS PLATFORM
            ELSIF upper(split(v_rec, 1, ',') || split(v_rec, 2, ',') ||
                        split(v_rec, 3, ',')) = 'AAAOSLINUX' THEN
              v_os := 'Linux';
              dbms_output.put_line(v_os);
            ELSIF upper(split(v_rec, 1, ',') || split(v_rec, 2, ',')) =
                  'AAAAIX' THEN
              v_os := 'AIX';
              dbms_output.put_line(v_os);
            
              ----CREATE TABLE
              --DISKREAD
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'DISKREAD%,Disk%' THEN
              v_spilt1 := split(v_rec, 1, ',');
              v_spilt2 := split(v_rec, 2, ',');
              SELECT length(v_spilt1 || ',' || v_spilt2)
                INTO strlen
                FROM dual;
              SELECT 'create table ' || v_spilt1 ||
                     ' (timeflag varchar2(8),' ||
                     REPLACE(substr(v_rec, strlen + 2), ',', ' number ,') ||
                     ' number)'
                INTO v_crttab
                FROM dual;
              v_crttab := REPLACE(v_crttab, '-', '_');
              /* dbms_output.put_line(strlen);
              dbms_output.put_line(v_spilt1);
              dbms_output.put_line(v_spilt2);
              dbms_output.put_line(v_spilt1 || ',' || v_spilt2);*/
              --dbms_output.put_line(v_crttab);
              --##REPLACE "/" for LINUX nmon
              --EXECUTE IMMEDIATE v_crttab;
              EXECUTE IMMEDIATE REPLACE(v_crttab, '/', '');
            --DISKWRITE
              ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'DISKWRITE%,Disk%' THEN
              v_spilt1 := split(v_rec, 1, ',');
              v_spilt2 := split(v_rec, 2, ',');
              SELECT length(v_spilt1 || ',' || v_spilt2)
                INTO strlen
                FROM dual;
              SELECT 'create table ' || v_spilt1 ||
                     ' (timeflag varchar2(8),' ||
                     REPLACE(substr(v_rec, strlen + 2), ',', ' number ,') ||
                     ' number)'
                INTO v_crttab
                FROM dual;
              v_crttab := REPLACE(v_crttab, '-', '_');
              EXECUTE IMMEDIATE REPLACE(v_crttab, '/', '');
            --DISKXFER
              ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'DISKXFER%,Disk%' THEN
              v_spilt1 := split(v_rec, 1, ',');
              v_spilt2 := split(v_rec, 2, ',');
              SELECT length(v_spilt1 || ',' || v_spilt2)
                INTO strlen
                FROM dual;
              SELECT 'create table ' || v_spilt1 ||
                     ' (timeflag varchar2(8),' ||
                     REPLACE(substr(v_rec, strlen + 2), ',', ' number ,') ||
                     ' number)'
                INTO v_crttab
                FROM dual;
              v_crttab := REPLACE(v_crttab, '-', '_');
              EXECUTE IMMEDIATE REPLACE(v_crttab, '/', '');
            --NETWORK I/O FOR NFS TABLE
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE 
               'NET,Network I/O%' THEN
              v_spilt1 := split(v_rec, 1, ',');
              v_spilt2 := split(v_rec, 2, ',');
              SELECT length(v_spilt1 || ',' || v_spilt2)
                INTO strlen
                FROM dual;
              SELECT 'create table ' || v_spilt1 ||
                     ' (timeflag varchar2(8),' ||
                     REPLACE(substr(v_rec, strlen + 2), ',', ' number ,') ||
                     ' number)'
                INTO v_crttab
                FROM dual;
              v_crttab := REPLACE(v_crttab, '-', '_');
              --dbms_output.put_line(REPLACE(v_crttab, '/', '_PerSec'));
              EXECUTE IMMEDIATE REPLACE(v_crttab, '/', '_PerSec');
             END IF;
          
            ---INSERT SQL
            --INSERT DISKREAD DATA
            IF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
               'DISKREAD%,T%' THEN
              v_spilt1 := split(v_rec, 1, ',');
              v_spilt2 := split(v_rec, 2, ',');
              SELECT length(v_spilt1 || ',' || v_spilt2)
                INTO strlen
                FROM dual;
              v_inst_sql := ' insert into ' || v_spilt1 || ' values(''' ||
                            v_spilt2 || ''',' || substr(v_rec, strlen + 2) || ')';
              EXECUTE IMMEDIATE v_inst_sql;
              --dbms_output.put_line('diskread: '||v_inst_sql);
                                           
              --INSERT DISKWRITE DATA
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'DISKWRITE%,T%' THEN
              v_spilt1 := split(v_rec, 1, ',');
              v_spilt2 := split(v_rec, 2, ',');
              SELECT length(v_spilt1 || ',' || v_spilt2)
                INTO strlen
                FROM dual;
              v_inst_sql := ' insert into ' || v_spilt1 || ' values(''' ||
                            v_spilt2 || ''',' || substr(v_rec, strlen + 2) || ')';
              EXECUTE IMMEDIATE v_inst_sql;
              --dbms_output.put_line(v_inst_sql);
              --INSERT DISKXFER DATA
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'DISKXFER%,T%' THEN
              v_spilt1 := split(v_rec, 1, ',');
              v_spilt2 := split(v_rec, 2, ',');
              SELECT length(v_spilt1 || ',' || v_spilt2)
                INTO strlen
                FROM dual;
              v_inst_sql := ' insert into ' || v_spilt1 || ' values(''' ||
                            v_spilt2 || ''',' || substr(v_rec, strlen + 2) || ')';
              EXECUTE IMMEDIATE v_inst_sql;
              --dbms_output.put_line(v_inst_sql);          
 
          --INSERT NETWORK I/O 
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
               'NET,T%' THEN
              v_spilt1 := split(v_rec, 1, ',');
              v_spilt2 := split(v_rec, 2, ',');
              SELECT length(v_spilt1 || ',' || v_spilt2)
                INTO strlen
                FROM dual;
              v_inst_sql := ' insert into ' || v_spilt1 || ' values(''' ||
                            v_spilt2 || ''',' || substr(v_rec, strlen + 2) || ')';
              --dbms_output.put_line('Network I/O: '||v_inst_sql);              
              EXECUTE IMMEDIATE v_inst_sql;          
          
          
              --INSERT TIME DATA
            ELSIF split(v_rec, 1, ',') = 'ZZZZ' THEN
              v_time := split(v_rec, 4, ',') || ' ' || split(v_rec, 3, ',');
              --dbms_output.put_line(v_time);
              v_inst_sql := 'insert into ' || v_timetable || ' values (''' ||
                            split(v_rec, 2, ',') || ''',to_date(''' ||
                            v_time || '''' ||
                            ',''DD-MON-YYYY HH24:MI:SS''))' || '';
              EXECUTE IMMEDIATE v_inst_sql;
              --dbms_output.put_line(v_inst_sql);
              --FOR CPU_ALL DATA
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'CPU_ALL,T%' THEN
              v_spilt1 := split(v_rec, 1, ',');
              v_spilt2 := split(v_rec, 2, ',');
              SELECT length(v_spilt1 || ',' || v_spilt2)
                INTO strlen
                FROM dual;
              --modify nmon version,16beta linux --> CPU_ALL add  column Steal%  20160909
              --substr(v_rec, strlen + 2)  -->  replace(substr(v_rec, strlen + 2),',0.0,,',',,')
              v_inst_sql := ' insert into TMP_CPUALL ' || ' values(''' ||
                            v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
                            v_spilt2 || ''',' || replace(substr(v_rec, strlen + 2),',0.0,,',',,') || ')';
                           -- mark on 20160909 v_spilt2 || ''',' || substr(v_rec, strlen + 2) || ')';
              --EXECUTE IMMEDIATE v_inst_sql;
              v_inst_sql := REPLACE(v_inst_sql, ',,', ',null,');
              EXECUTE IMMEDIATE v_inst_sql;
              --dbms_output.put_line('CPU_SQL:' || v_inst_sql);
              --INSERT INTO DDSC_OS_CPU
              INSERT INTO ddsc_os_cpu
                (cust_id,
                 host,
                 sample_time,
                 user_pct,
                 sys_pct,
                 iowait,
                 idle,
                 busy,
                 num_cpus)
                SELECT c.cust_id,
                       c.host,
                       nt.nmon_date,
                       c.user_pct,
                       c.sys_pct,
                       c.iowait,
                       c.idle,
                       c.busy,
                       c.num_cpus
                  FROM tmp_cpuall c, NMON_TIME nt
                 WHERE c.timeflag = nt.timeflag;
              COMMIT;
            
              --MEMORY
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'MEM,T%' THEN
              IF v_os = 'AIX' THEN
                --BAK 20141009 before add paging column
                /*  v_inst_sql := ' insert into TMP_MEM ' || ' values(''' ||
                v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
                split(v_rec, 2, ',') || ''',' || '''' ||
                split(v_rec, 7, ',') || '''' || ',' || '''' ||
                split(v_rec, 5, ',') || '''' || ')';   */
                v_inst_sql := ' insert into TMP_MEM ' || ' values(''' ||
                              v_cust_id || '''' || ',' || '''' ||
                              v_hostname || '''' || ',' || '''' ||
                              split(v_rec, 2, ',') || ''',' || '''' ||
                              split(v_rec, 7, ',') || '''' || ',' || '''' ||
                              split(v_rec, 5, ',') || '''' || ',' || '''' ||
                              split(v_rec, 8, ',') || '''' || ',' || '''' ||
                              split(v_rec, 6, ',') || '''' || ')';
              
                EXECUTE IMMEDIATE v_inst_sql;
              
              ELSIF v_os = 'Linux' THEN
                v_inst_sql := ' insert into TMP_MEM ' || ' values(''' ||
                              v_cust_id || '''' || ',' || '''' ||
                              v_hostname || '''' || ',' || '''' ||
                              split(v_rec, 2, ',') || ''',' || '''' ||
                              split(v_rec, 3, ',') || '''' || ',' || '''' ||
                              split(v_rec, 7, ',') || '''' || ',' || '''' ||
                              split(v_rec, 6, ',') || '''' || ',' || '''' ||
                              split(v_rec, 10, ',') || '''' || ')';
                EXECUTE IMMEDIATE v_inst_sql;
              END IF;
            
              /*      v_inst_sql := ' insert into TMP_MEM ' || ' values(''' ||
                            v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
                            split(v_rec, 2, ',') || ''',' || '''' ||
                            split(v_rec, 3, ',') || '''' || ',' || '''' ||
                            split(v_rec, 7, ',') || '''' || ')';
                EXECUTE IMMEDIATE v_inst_sql;
              --dbms_output.put_line('MEM_SQL:' || v_inst_sql);*/
            
              --INSERT INTO DDSC_OS_MEM
              INSERT INTO DDSC_OS_MEM
                SELECT m.cust_id,
                       m.host,
                       nt.nmon_date,
                       m.total,
                       m.free,
                       m.paging_total,
                       m.paging_free
                  FROM tmp_mem m, NMON_TIME nt
                 WHERE m.timeflag = nt.timeflag;
              COMMIT;
            
              --PGSP  like ^PAGE(AIX) FOR AIX
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'PAGE,T%' THEN
              v_inst_sql := ' insert into TMP_PGSP ' || ' values(''' ||
                            v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
                            split(v_rec, 2, ',') || ''',' || '''' ||
                            split(v_rec, 6, ',') || '''' || ',' || '''' ||
                            split(v_rec, 7, ',') || '''' || ')';
              EXECUTE IMMEDIATE v_inst_sql;
              --dbms_output.put_line('MEM_SQL:' || v_inst_sql);
              --INSERT INTO DDSC_OS_PGSP
              INSERT INTO DDSC_OS_PGSP
                (cust_id, host, sample_time, PGSIN, PGSOUT)
                SELECT p.cust_id, p.host, nt.nmon_date, p.PGSIN, p.PGSOUT
                  FROM tmp_pgsp p, NMON_TIME nt
                 WHERE p.timeflag = nt.timeflag;
              COMMIT;
            
              --PGSP   FOR LINUX ^VM (LINUX)
            ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                  'VM,T%' THEN
              v_inst_sql := ' insert into TMP_PGSP ' || ' values(''' ||
                            v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
                            split(v_rec, 2, ',') || ''',' || '''' ||
                            split(v_rec, 11, ',') || '''' || ',' || '''' ||
                            split(v_rec, 12, ',') || '''' || ')';
              EXECUTE IMMEDIATE v_inst_sql;
              --dbms_output.put_line('MEM_SQL:' || v_inst_sql);
              --INSERT INTO DDSC_OS_PGSP
              INSERT INTO DDSC_OS_PGSP
                (cust_id, host, sample_time, PGSIN, PGSOUT)
                SELECT p.cust_id, p.host, nt.nmon_date, p.PGSIN, p.PGSOUT
                  FROM tmp_pgsp p, NMON_TIME nt
                 WHERE p.timeflag = nt.timeflag;
              COMMIT;
            
            END IF;
          
          EXCEPTION
            WHEN no_data_found THEN
              COMMIT;
              utl_file.fclose(v_input_file);
              EXIT;
            WHEN OTHERS THEN
              COMMIT;
              dbms_output.put_line(SQLERRM);
              DBMS_OUTPUT.put_line(DBMS_UTILITY.format_error_backtrace);
              utl_file.fclose(v_input_file);
              EXIT;
          END;
        END LOOP;
        --After create nmon table and imp nmon data calculate data then import
        --dbasrv_tool.nmon_calc_imp(v_cust_id,v_directory);
        dbasrv_tool.nmon_calc_imp_nfs(v_cust_id, v_directory);
      END;
      --initail table
      dbasrv_tool.nmon_tab_drop;
      EXECUTE IMMEDIATE 'truncate table nmon_time';
    
    END LOOP;
    utl_file.fclose(v_input_file);
  
  END;

  PROCEDURE nmon_rawdata_imp_byfile(v_cust_id   IN VARCHAR2,
                                    v_directory IN VARCHAR2,
                                    v_filename  IN VARCHAR2) IS
  
    v_input_file UTL_FILE.file_type;
    v_rec        VARCHAR2(32766);
    v_crttab     VARCHAR2(5000);
    v_inst_sql   VARCHAR2(5000);
    v_spilt1     VARCHAR2(100);
    v_spilt2     VARCHAR2(100);
    v_time       VARCHAR2(500);
    strlen       NUMBER;
    v_timetable  VARCHAR2(30);
    v_timet_cnt  NUMBER;
    v_os         VARCHAR2(10);
  BEGIN
  
   --CREATE TIMEFLAG TABLE
    v_timetable := 'NMON_TIME';
    SELECT COUNT(1)
      INTO v_timet_cnt
      FROM user_tables
     WHERE table_name = 'NMON_TIME';
    IF v_timet_cnt = 0 THEN
      EXECUTE IMMEDIATE 'create table ' || v_timetable ||
                        '(timeflag varchar2(8),nmon_date date)';
    END IF;
  
    --execute  immediate   'create table NMON_TIME (timeflag varchar2(8),nmon_date date)';
    --dbasrv_tool.nmon_tab_drop;
    v_input_file := utl_file.fopen(v_directory, v_filename, 'R', 32767);
    BEGIN
      LOOP
        BEGIN
          utl_file.get_line(v_input_file, v_rec, 32767);
        
          --GET HOSTNAME
          IF split(v_rec, 1, ',') || split(v_rec, 2, ',') = 'AAAhost' THEN
            v_hostname := split(v_rec, 3, ',');
          
          ELSIF split(v_rec, 1, ',') || split(v_rec, 2, ',') = 'AAArunname' THEN
            v_hostname := split(v_rec, 3, ',');
          
            --GET OS PLATFORM
          ELSIF upper(split(v_rec, 1, ',') || split(v_rec, 2, ',') ||
                      split(v_rec, 3, ',')) = 'AAAOSLINUX' THEN
            v_os := 'Linux';
            dbms_output.put_line(v_os);
          ELSIF upper(split(v_rec, 1, ',') || split(v_rec, 2, ',')) =
                'AAAAIX' THEN
            v_os := 'AIX';
            dbms_output.put_line(v_os);
          
            ----CREATE TABLE
            --CREATE TIME TABLE 
            EXECUTE IMMEDIATE 'create table ' || v_timetable ||
                              split(v_filename, 2, '_') ||
                              '(timeflag varchar2(8),nmon_date date)';
          
            --DISKREAD
          ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                'DISKREAD%,Disk%' THEN
          
            v_spilt1 := split(v_rec, 1, ',');
            v_spilt2 := split(v_rec, 2, ',');
            SELECT length(v_spilt1 || ',' || v_spilt2)
              INTO strlen
              FROM dual;
            SELECT 'create table ' || v_spilt1 || split(v_filename, 2, '_') ||
                   ' (timeflag varchar2(8),' ||
                   REPLACE(substr(v_rec, strlen + 2), ',', ' number ,') ||
                   ' number)'
              INTO v_crttab
              FROM dual;
            v_crttab := REPLACE(v_crttab, '-', '_');
            /* dbms_output.put_line(strlen);
            dbms_output.put_line(v_spilt1);
            dbms_output.put_line(v_spilt2);
            dbms_output.put_line(v_spilt1 || ',' || v_spilt2);*/
            --dbms_output.put_line(v_crttab);
            --##REPLACE "/" for LINUX nmon
            --EXECUTE IMMEDIATE v_crttab;
            EXECUTE IMMEDIATE REPLACE(v_crttab, '/', '');
            --DISKWRITE
          ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                'DISKWRITE%,Disk%' THEN
            v_spilt1 := split(v_rec, 1, ',');
            v_spilt2 := split(v_rec, 2, ',');
            SELECT length(v_spilt1 || ',' || v_spilt2)
              INTO strlen
              FROM dual;
            SELECT 'create table ' || v_spilt1 || split(v_filename, 2, '_') ||
                   ' (timeflag varchar2(8),' ||
                   REPLACE(substr(v_rec, strlen + 2), ',', ' number ,') ||
                   ' number)'
              INTO v_crttab
              FROM dual;
            v_crttab := REPLACE(v_crttab, '-', '_');
            /* dbms_output.put_line(strlen);
            dbms_output.put_line(v_spilt1);
            dbms_output.put_line(v_spilt2);
            dbms_output.put_line(v_spilt1 || ',' || v_spilt2);
            dbms_output.put_line(v_crttab);*/
            --##REPLACE "/" for LINUX nmon
            -- EXECUTE IMMEDIATE v_crttab;
            EXECUTE IMMEDIATE REPLACE(v_crttab, '/', '');
            --DISKXFER
          ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                'DISKXFER%,Disk%' THEN
            v_spilt1 := split(v_rec, 1, ',');
            v_spilt2 := split(v_rec, 2, ',');
            SELECT length(v_spilt1 || ',' || v_spilt2)
              INTO strlen
              FROM dual;
            SELECT 'create table ' || v_spilt1 || split(v_filename, 2, '_') ||
                   ' (timeflag varchar2(8),' ||
                   REPLACE(substr(v_rec, strlen + 2), ',', ' number ,') ||
                   ' number)'
              INTO v_crttab
              FROM dual;
            v_crttab := REPLACE(v_crttab, '-', '_');
            /*   dbms_output.put_line(strlen);
            dbms_output.put_line(v_spilt1);
            dbms_output.put_line(v_spilt2);
            dbms_output.put_line(v_spilt1 || ',' || v_spilt2);
            dbms_output.put_line(v_crttab);*/
            --##REPLACE "/" for LINUX nmon
            -- EXECUTE IMMEDIATE v_crttab;
            EXECUTE IMMEDIATE REPLACE(v_crttab, '/', '');
          END IF;
        
          ---INSERT SQL
          --INSERT DISKREAD DATA
          IF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
             'DISKREAD%,T%' THEN
            v_spilt1 := split(v_rec, 1, ',');
            v_spilt2 := split(v_rec, 2, ',');
            SELECT length(v_spilt1 || ',' || v_spilt2)
              INTO strlen
              FROM dual;
            v_inst_sql := ' insert into ' || v_spilt1 ||
                          split(v_filename, 2, '_') || ' values(''' ||
                          v_spilt2 || ''',' || substr(v_rec, strlen + 2) || ')';
            EXECUTE IMMEDIATE v_inst_sql;
            --dbms_output.put_line('diskread: '||v_inst_sql);
          
            --INSERT DISKWRITE DATA
          ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                'DISKWRITE%,T%' THEN
            v_spilt1 := split(v_rec, 1, ',');
            v_spilt2 := split(v_rec, 2, ',');
            SELECT length(v_spilt1 || ',' || v_spilt2)
              INTO strlen
              FROM dual;
            v_inst_sql := ' insert into ' || v_spilt1 ||
                          split(v_filename, 2, '_') || ' values(''' ||
                          v_spilt2 || ''',' || substr(v_rec, strlen + 2) || ')';
            EXECUTE IMMEDIATE v_inst_sql;
            --dbms_output.put_line(v_inst_sql);
            --INSERT DISKXFER DATA
          ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                'DISKXFER%,T%' THEN
            v_spilt1 := split(v_rec, 1, ',');
            v_spilt2 := split(v_rec, 2, ',');
            SELECT length(v_spilt1 || ',' || v_spilt2)
              INTO strlen
              FROM dual;
            v_inst_sql := ' insert into ' || v_spilt1 ||
                          split(v_filename, 2, '_') || ' values(''' ||
                          v_spilt2 || ''',' || substr(v_rec, strlen + 2) || ')';
            EXECUTE IMMEDIATE v_inst_sql;
            --dbms_output.put_line(v_inst_sql);
            --INSET TIME DATA
          ELSIF split(v_rec, 1, ',') = 'ZZZZ' THEN
            v_time := split(v_rec, 4, ',') || ' ' || split(v_rec, 3, ',');
            --dbms_output.put_line(v_time);
            v_inst_sql := 'insert into ' || v_timetable || ' values (''' ||
                          split(v_rec, 2, ',') || ''',to_date(''' || v_time || '''' ||
                          ',''DD-MON-YYYY HH24:MI:SS''))' || '';
            EXECUTE IMMEDIATE v_inst_sql;
            --dbms_output.put_line(v_inst_sql);
            --FOR CPU_ALL DATA
          ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                'CPU_ALL,T%' THEN
            v_spilt1 := split(v_rec, 1, ',');
            v_spilt2 := split(v_rec, 2, ',');
            SELECT length(v_spilt1 || ',' || v_spilt2)
              INTO strlen
              FROM dual;
            v_inst_sql := ' insert into TMP_CPUALL ' || ' values(''' ||
                          v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
                          v_spilt2 || ''',' || substr(v_rec, strlen + 2) || ')';
            --EXECUTE IMMEDIATE v_inst_sql;
            v_inst_sql := REPLACE(v_inst_sql, ',,', ',null,');
            EXECUTE IMMEDIATE v_inst_sql;
            --dbms_output.put_line('CPU_SQL:' || v_inst_sql);
            --INSERT INTO DDSC_OS_CPU
            INSERT INTO ddsc_os_cpu
              (cust_id,
               host,
               sample_time,
               user_pct,
               sys_pct,
               iowait,
               idle,
               busy,
               num_cpus)
              SELECT c.cust_id,
                     c.host,
                     nt.nmon_date,
                     c.user_pct,
                     c.sys_pct,
                     c.iowait,
                     c.idle,
                     c.busy,
                     c.num_cpus
                FROM tmp_cpuall c, NMON_TIME nt
               WHERE c.timeflag = nt.timeflag;
            COMMIT;
          
            --MEMORY
          ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                'MEM,T%' THEN
            IF v_os = 'AIX' THEN
              --BAK 20141009 before add paging column
              /*  v_inst_sql := ' insert into TMP_MEM ' || ' values(''' ||
              v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
              split(v_rec, 2, ',') || ''',' || '''' ||
              split(v_rec, 7, ',') || '''' || ',' || '''' ||
              split(v_rec, 5, ',') || '''' || ')';   */
              v_inst_sql := ' insert into TMP_MEM ' || ' values(''' ||
                            v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
                            split(v_rec, 2, ',') || ''',' || '''' ||
                            split(v_rec, 7, ',') || '''' || ',' || '''' ||
                            split(v_rec, 5, ',') || '''' || ',' || '''' ||
                            split(v_rec, 8, ',') || '''' || ',' || '''' ||
                            split(v_rec, 6, ',') || '''' || ')';
            
              EXECUTE IMMEDIATE v_inst_sql;
            
            ELSIF v_os = 'Linux' THEN
              v_inst_sql := ' insert into TMP_MEM ' || ' values(''' ||
                            v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
                            split(v_rec, 2, ',') || ''',' || '''' ||
                            split(v_rec, 3, ',') || '''' || ',' || '''' ||
                            split(v_rec, 7, ',') || '''' || ',' || '''' ||
                            split(v_rec, 6, ',') || '''' || ',' || '''' ||
                            split(v_rec, 10, ',') || '''' || ')';
              EXECUTE IMMEDIATE v_inst_sql;
            END IF;
          
            /*      v_inst_sql := ' insert into TMP_MEM ' || ' values(''' ||
                          v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
                          split(v_rec, 2, ',') || ''',' || '''' ||
                          split(v_rec, 3, ',') || '''' || ',' || '''' ||
                          split(v_rec, 7, ',') || '''' || ')';
              EXECUTE IMMEDIATE v_inst_sql;
            --dbms_output.put_line('MEM_SQL:' || v_inst_sql);*/
          
            --INSERT INTO DDSC_OS_MEM
            INSERT INTO DDSC_OS_MEM
              SELECT m.cust_id,
                     m.host,
                     nt.nmon_date,
                     m.total,
                     m.free,
                     m.paging_total,
                     m.paging_free
                FROM tmp_mem m, NMON_TIME nt
               WHERE m.timeflag = nt.timeflag;
            COMMIT;
          
            --PGSP  like ^PAGE(AIX) FOR AIX
          ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                'PAGE,T%' THEN
            v_inst_sql := ' insert into TMP_PGSP ' || ' values(''' ||
                          v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
                          split(v_rec, 2, ',') || ''',' || '''' ||
                          split(v_rec, 6, ',') || '''' || ',' || '''' ||
                          split(v_rec, 7, ',') || '''' || ')';
            EXECUTE IMMEDIATE v_inst_sql;
            --dbms_output.put_line('MEM_SQL:' || v_inst_sql);
            --INSERT INTO DDSC_OS_PGSP
            INSERT INTO DDSC_OS_PGSP
              (cust_id, host, sample_time, PGSIN, PGSOUT)
              SELECT p.cust_id, p.host, nt.nmon_date, p.PGSIN, p.PGSOUT
                FROM tmp_pgsp p, NMON_TIME nt
               WHERE p.timeflag = nt.timeflag;
            COMMIT;
          
            --PGSP   FOR LINUX ^VM (LINUX)
          ELSIF split(v_rec, 1, ',') || ',' || split(v_rec, 2, ',') LIKE
                'VM,T%' THEN
            v_inst_sql := ' insert into TMP_PGSP ' || ' values(''' ||
                          v_cust_id || '''' || ',' || '''' || v_hostname || '''' || ',' || '''' ||
                          split(v_rec, 2, ',') || ''',' || '''' ||
                          split(v_rec, 11, ',') || '''' || ',' || '''' ||
                          split(v_rec, 12, ',') || '''' || ')';
            EXECUTE IMMEDIATE v_inst_sql;
            --dbms_output.put_line('MEM_SQL:' || v_inst_sql);
            --INSERT INTO DDSC_OS_PGSP
            INSERT INTO DDSC_OS_PGSP
              (cust_id, host, sample_time, PGSIN, PGSOUT)
              SELECT p.cust_id, p.host, nt.nmon_date, p.PGSIN, p.PGSOUT
                FROM tmp_pgsp p, NMON_TIME nt
               WHERE p.timeflag = nt.timeflag;
            COMMIT;
          
          END IF;
        
        EXCEPTION
          WHEN no_data_found THEN
            COMMIT;
            utl_file.fclose(v_input_file);
            EXIT;
          WHEN OTHERS THEN
            COMMIT;
            dbms_output.put_line(SQLERRM);
            DBMS_OUTPUT.put_line(DBMS_UTILITY.format_error_backtrace);
            utl_file.fclose(v_input_file);
            EXIT;
        END;
    --    utl_file.fclose(v_input_file);
      END LOOP;
      --After create nmon table and imp nmon data calculate data then import
      --dbasrv_tool.nmon_calc_imp(v_cust_id,v_directory);
      dbasrv_tool.nmon_calc_imp_byfile(v_cust_id, v_directory, v_filename);
    END;
    --initail table
    --dbasrv_tool.nmon_tab_drop;
    --EXECUTE IMMEDIATE 'truncate table nmon_time';
    utl_file.fclose(v_input_file);
  
  END;

  PROCEDURE win_rawdata_imp(v_cust_id IN VARCHAR2, v_directory IN VARCHAR2) IS
    v_input_file UTL_FILE.file_type;
    v_ifilename  VARCHAR2(100);
    v_rec        VARCHAR2(32766);
  
  BEGIN
  
    FOR iname IN (SELECT basename(column_value, NULL, '\') "NAME"
                    FROM TABLE(list_files(v_directory))) LOOP
      v_input_file := utl_file.fopen(v_directory, iname.name, 'R', 32767);
      --GET HOSTNAME
      v_hostname := split(iname.name, 2, '_');
      BEGIN
        --CPU
        IF upper(split(iname.name, 3, '_')) = 'CPU' THEN
          LOOP
            BEGIN
              utl_file.get_line(v_input_file, v_rec, 32767);
              --dbms_output.put_line( replace((split(v_rec, 3, ',')),'"','')    );
              INSERT INTO ddsc_os_cpu
                (cust_id,
                 host,
                 sample_time,
                 user_pct,
                 sys_pct,
                 iowait,
                 idle,
                 busy,
                 num_cpus,
                 cpu_load)
              VALUES
                (v_cust_id,
                 TRIM(REPLACE(split(v_rec, 2, ','), '"', '')),
                 to_timestamp(REPLACE(split(v_rec, 3, ','), '"', ''),
                              'MM/DD/YYYY HH24:MI:SS.FF3'),
                 TRIM(REPLACE(split(v_rec, 4, ','), '"', '')),
                 TRIM(REPLACE(split(v_rec, 5, ','), '"', '')),
                 NULL,
                 TRIM(REPLACE(split(v_rec, 7, ','), '"', '')),
                 NULL,
                 NULL,
                 NULL);
            
            EXCEPTION
              WHEN no_data_found THEN
                COMMIT;
                utl_file.fclose(v_input_file);
                EXIT;
              WHEN OTHERS THEN
                COMMIT;
                dbms_output.put_line(iname.name);
                dbms_output.put_line(SQLERRM);
                DBMS_OUTPUT.put_line(DBMS_UTILITY.format_error_backtrace);
                utl_file.fclose(v_input_file);
                EXIT;
              
            END;
          END LOOP;
          utl_file.fclose(v_input_file);
          COMMIT;
          --DISKIO
        ELSIF upper(split(iname.name, 3, '_')) = 'DISKIO' THEN
          LOOP
            BEGIN
              utl_file.get_line(v_input_file, v_rec, 32767);
              INSERT INTO DDSC_OS_DISKIO
                (cust_id, host, sample_time, read_kbs, write_kbs, iops)
              VALUES
                (v_cust_id,
                 TRIM(REPLACE(split(v_rec, 2, ','), '"', '')),
                 to_timestamp(REPLACE(split(v_rec, 3, ','), '"', ''),
                              'MM/DD/YYYY HH24:MI:SS.FF3'),
                 TRIM(REPLACE(split(v_rec, 4, ','), '"', '')),
                 TRIM(REPLACE(split(v_rec, 5, ','), '"', '')),
                 TRIM(REPLACE(split(v_rec, 6, ','), '"', '')));
            EXCEPTION
              WHEN no_data_found THEN
                COMMIT;
                utl_file.fclose(v_input_file);
                EXIT;
              WHEN OTHERS THEN
                COMMIT;
                dbms_output.put_line(SQLERRM);
                DBMS_OUTPUT.put_line(DBMS_UTILITY.format_error_backtrace);
                utl_file.fclose(v_input_file);
                EXIT;
              
            END;
          END LOOP;
          COMMIT;
          utl_file.fclose(v_input_file);
          --MEMORY
        ELSIF upper(split(iname.name, 3, '_')) = 'MEM' THEN
          LOOP
          
            BEGIN
              utl_file.get_line(v_input_file, v_rec, 32767);
              IF to_number(TRIM(REPLACE(split(v_rec, 4, ','), '"', ''))) >
                 to_number(TRIM(REPLACE(split(v_rec, 5, ','), '"', ''))) THEN
                INSERT INTO DDSC_OS_MEM
                  (cust_id,
                   host,
                   sample_time,
                   TOTAL,
                   FREE,
                   PAGING_TOTAL,
                   PAGING_FREE)
                VALUES
                  (v_cust_id,
                   TRIM(REPLACE(split(v_rec, 2, ','), '"', '')),
                   to_timestamp(REPLACE(split(v_rec, 3, ','), '"', ''),
                                'MM/DD/YYYY HH24:MI:SS.FF3'),
                   TRIM(REPLACE(split(v_rec, 4, ','), '"', '')),
                   TRIM(REPLACE(split(v_rec, 5, ','), '"', '')),
                   NULL,
                   NULL);
              ELSE
                INSERT INTO DDSC_OS_MEM
                  (cust_id,
                   host,
                   sample_time,
                   FREE,
                   TOTAL,
                   PAGING_TOTAL,
                   PAGING_FREE)
                VALUES
                  (v_cust_id,
                   TRIM(REPLACE(split(v_rec, 2, ','), '"', '')),
                   to_timestamp(REPLACE(split(v_rec, 3, ','), '"', ''),
                                'MM/DD/YYYY HH24:MI:SS.FF3'),
                   TRIM(REPLACE(split(v_rec, 4, ','), '"', '')),
                   TRIM(REPLACE(split(v_rec, 5, ','), '"', '')),
                   NULL,
                   NULL);
              END IF;
            
            EXCEPTION
              WHEN no_data_found THEN
                COMMIT;
                utl_file.fclose(v_input_file);
                EXIT;
              WHEN OTHERS THEN
                COMMIT;
                dbms_output.put_line(SQLERRM);
                DBMS_OUTPUT.put_line(DBMS_UTILITY.format_error_backtrace);
                --utl_file.fclose(v_input_file);
              --EXIT;
            
            END;
          END LOOP;
          COMMIT;
          utl_file.fclose(v_input_file);
        
          --PGSP
        ELSIF upper(split(iname.name, 3, '_')) = 'PGSP' THEN
          LOOP
            BEGIN
              utl_file.get_line(v_input_file, v_rec, 32767);
              INSERT INTO DDSC_OS_PGSP
                (cust_id, host, sample_time, PGSIN, PGSOUT)
              VALUES
                (v_cust_id,
                 TRIM(REPLACE(split(v_rec, 2, ','), '"', '')),
                 to_timestamp(REPLACE(split(v_rec, 3, ','), '"', ''),
                              'MM/DD/YYYY HH24:MI:SS.FF3'),
                 TRIM(REPLACE(split(v_rec, 4, ','), '"', '')),
                 TRIM(REPLACE(split(v_rec, 5, ','), '"', '')));
            EXCEPTION
              WHEN no_data_found THEN
                COMMIT;
                utl_file.fclose(v_input_file);
                EXIT;
              WHEN OTHERS THEN
                COMMIT;
                dbms_output.put_line(SQLERRM);
                DBMS_OUTPUT.put_line(DBMS_UTILITY.format_error_backtrace);
                utl_file.fclose(v_input_file);
                EXIT;
              
            END;
          END LOOP;
          COMMIT;
          utl_file.fclose(v_input_file);
        
        END IF;
      
      END;
      utl_file.fclose(v_input_file);
    END LOOP;
  
  END;

  PROCEDURE nmon_calc(v_cust_id   IN VARCHAR2,
                      v_directory IN VARCHAR2,
                      IO_TYPE     IN VARCHAR2) IS
  
    v_where        VARCHAR2(32767);
    v_sql          VARCHAR2(32767);
    v_total_sql    VARCHAR2(32767);
    v_sql_first    VARCHAR2(1000);
    v_sql_last     VARCHAR2(1000);
    v_sumsql_first VARCHAR2(1000);
    v_sumsql_last  VARCHAR2(1000);
    v_tabname      VARCHAR2(50);
    v_like         VARCHAR2(50);
    pre_tabname    VARCHAR2(50);

  BEGIN
  
    --v_hostname     := get_nmon_hostname(v_directory);
    v_sql_first    := 'select timeflag,sum(VAL) "VAL" from (';
    v_sql_last     := ') group by timeflag order by timeflag';
    v_sumsql_first := 'select ' || '''' || v_cust_id || '''' || ',' || '''' ||
                      v_hostname || '''' ||
                      ',a.nmon_date,"VAL" from nmon_time a,(';
    v_sumsql_last  := ') b where a.timeflag=b.timeflag order by a.timeflag';
  
    IF upper(IO_TYPE) = 'READ' THEN
      v_like := '%DISKREAD%';
    ELSIF upper(IO_TYPE) = 'WRITE' THEN
      v_like := '%DISKWRITE%';
    ELSIF upper(IO_TYPE) = 'IOPS' THEN
      v_like := '%DISKXFER%';
    END IF;
  
    FOR READ IN (SELECT *
                   FROM user_tab_columns
                  WHERE table_name LIKE v_like
                    AND table_name NOT LIKE 'TMP_%'
                    AND column_name NOT LIKE '%HDISKPOWER%'
                       --and column_name not like 'DM_%'
                    AND column_name != 'TIMEFLAG'
                  ORDER BY table_name, column_name) LOOP
      pre_tabname := v_tabname; -- init is null
      v_tabname   := read.table_name;
    
      IF pre_tabname IS NULL THEN
        v_sql := 'select timeflag,(';
        --dbms_output.put_line(read.column_name);
        --AIX or LINUX HP or LINUX IBM or LINUX DELL
        IF upper(read.column_name) LIKE 'HDISK%' OR
           upper(read.column_name) LIKE 'CCISS%' OR
           upper(read.column_name) LIKE 'DM%' OR
           upper(read.column_name) LIKE 'SD%' OR
           upper(read.column_name) LIKE 'XV%' THEN
          v_where := read.column_name;
          v_sql   := v_sql || v_where || chr(10);
        END IF;
      
      ELSIF pre_tabname = read.table_name THEN
        IF upper(read.column_name) LIKE 'HDISK%' OR
           upper(read.column_name) LIKE 'CCISS%' OR
           upper(read.column_name) LIKE 'DM%' OR
           upper(read.column_name) LIKE 'SD%' OR
           upper(read.column_name) LIKE 'XV%' THEN
          v_where := '+' || read.column_name;
          v_sql   := v_sql || v_where || chr(10);
        END IF;
      
      ELSE
        v_sql       := v_sql || ') VAL from ' || pre_tabname || chr(10) ||
                       ' union all ' || chr(10);
        v_total_sql := v_total_sql || v_sql;
        --dbms_output.put_line(v_sql);
        --re-inti
        v_sql   := 'select timeflag,(';
        v_where := read.column_name;
        v_sql   := v_sql || v_where || chr(10);
      
      END IF;
    END LOOP;
    v_sql := v_sql || ') VAL from ' || pre_tabname || chr(10) ||
             ' order by timeflag';
    --dbms_output.put_line(v_sql);
    v_total_sql := v_sql_first || v_total_sql || v_sql || v_sql_last;
    --dbms_output.put_line(v_total_sql);
    v_total_sql := v_sumsql_first || v_total_sql || v_sumsql_last;
    --dbms_output.put_line(v_total_sql);
    --dbms_output.put_line('insert into tmp_disk'||IO_TYPE||' '|| v_total_sql);
   --RHEL 4 NMON NO DISK READ WRITE
    
     EXECUTE IMMEDIATE 'insert into tmp_disk' || IO_TYPE || ' ' ||
                      v_total_sql;
                      
       
     
  END;

  PROCEDURE nmon_calc_byfile(v_cust_id   IN VARCHAR2,
                             v_directory IN VARCHAR2,
                             IO_TYPE     IN VARCHAR2,
                             v_filename  IN VARCHAR2) IS
  
    v_where        VARCHAR2(32767);
    v_sql          VARCHAR2(32767);
    v_total_sql    VARCHAR2(32767);
    v_sql_first    VARCHAR2(1000);
    v_sql_last     VARCHAR2(1000);
    v_sumsql_first VARCHAR2(1000);
    v_sumsql_last  VARCHAR2(1000);
    v_tabname      VARCHAR2(50);
    v_like         VARCHAR2(50);
    pre_tabname    VARCHAR2(50);
  
  BEGIN
  
    --v_hostname     := get_nmon_hostname(v_directory);
    v_sql_first    := 'select timeflag,sum(VAL) "VAL" from (';
    v_sql_last     := ') group by timeflag order by timeflag';
    v_sumsql_first := 'select ' || '''' || v_cust_id || '''' || ',' || '''' ||
                      v_hostname || '''' ||
                      ',a.nmon_date,"VAL" from nmon_time a,(';
    v_sumsql_last  := ') b where a.timeflag=b.timeflag order by a.timeflag';
  
    IF upper(IO_TYPE) = 'READ' THEN
      v_like := '%DISKREAD%';
    ELSIF upper(IO_TYPE) = 'WRITE' THEN
      v_like := '%DISKWRITE%';
    ELSIF upper(IO_TYPE) = 'IOPS' THEN
      v_like := '%DISKXFER%';
    END IF;
  
    FOR READ IN (SELECT *
                   FROM user_tab_columns
                  WHERE table_name LIKE v_like
                    AND table_name NOT LIKE 'TMP_%'
                    AND column_name NOT LIKE '%HDISKPOWER%'
                       --and column_name not like 'DM_%'
                    AND column_name != 'TIMEFLAG'
                  ORDER BY table_name, column_name) LOOP
      pre_tabname := v_tabname; -- init is null
      v_tabname   := read.table_name;
    
      IF pre_tabname IS NULL THEN
        v_sql := 'select timeflag,(';
        dbms_output.put_line(read.column_name);
        --AIX or LINUX HP or LINUX IBM or LINUX DELL
        IF upper(read.column_name) LIKE 'HDISK%' OR
           upper(read.column_name) LIKE 'CCISS%' OR
           upper(read.column_name) LIKE 'DM%' OR
           upper(read.column_name) LIKE 'SD%' OR
           upper(read.column_name) LIKE 'XV%' THEN
          v_where := read.column_name;
          v_sql   := v_sql || v_where || chr(10);
        END IF;
      
      ELSIF pre_tabname = read.table_name THEN
        IF upper(read.column_name) LIKE 'HDISK%' OR
           upper(read.column_name) LIKE 'CCISS%' OR
           upper(read.column_name) LIKE 'DM%' OR
           upper(read.column_name) LIKE 'SD%' OR
           upper(read.column_name) LIKE 'XV%' THEN
          v_where := '+' || read.column_name;
          v_sql   := v_sql || v_where || chr(10);
        END IF;
      
      ELSE
        v_sql       := v_sql || ') VAL from ' || pre_tabname || chr(10) ||
                       ' union all ' || chr(10);
        v_total_sql := v_total_sql || v_sql;
        --dbms_output.put_line(v_sql);
        --re-inti
        v_sql   := 'select timeflag,(';
        v_where := read.column_name;
        v_sql   := v_sql || v_where || chr(10);
      
      END IF;
    END LOOP;
    v_sql := v_sql || ') VAL from ' || pre_tabname || chr(10) ||
             ' order by timeflag';
    --dbms_output.put_line(v_sql);
    v_total_sql := v_sql_first || v_total_sql || v_sql || v_sql_last;
    --dbms_output.put_line(v_total_sql);
    v_total_sql := v_sumsql_first || v_total_sql || v_sumsql_last;
    --dbms_output.put_line(v_total_sql);
    dbms_output.put_line('insert into ' || 'tmp_disk' ||
                         split(v_filename, 2, '_') || IO_TYPE || ' ' ||
                         v_total_sql);
    EXECUTE IMMEDIATE 'insert into ' || 'tmp_disk' || IO_TYPE ||
                      split(v_filename, 2, '_') || ' ' || v_total_sql;
  
  END;

  PROCEDURE nmon_calc_imp(v_cust_id IN VARCHAR2, v_directory IN VARCHAR2) IS
    IO_TYPE VARCHAR2(10);

  BEGIN

    FOR i IN (SELECT 'READ' "TYPE"
                FROM dual
              UNION
              SELECT 'WRITE' "TYPE"
                FROM dual
              UNION
              SELECT 'IOPS' "TYPE" FROM dual) LOOP
      dbasrv_tool.nmon_calc(v_cust_id, v_directory, i.type);
      --dbasrv_tool.nmon_tab_drop;
    END LOOP;
    --insert to ddsc_os_diskio table
    INSERT INTO ddsc_os_diskio
      SELECT a.cust_id,
             a.host,
             a.sample_time,
             a.read_kbs,
             b.write_kbs,
             c.iops
        FROM tmp_diskread a, tmp_diskwrite b, tmp_diskiops c
       WHERE b.sample_time = c.sample_time
         AND a.sample_time = c.sample_time;
    COMMIT;
    
     
    
  
  END;
  
  PROCEDURE nmon_calc_imp_nfs(v_cust_id IN VARCHAR2, v_directory IN VARCHAR2) IS
    IO_TYPE VARCHAR2(10);
    v_netsql_1       VARCHAR2(32767);
    v_netsql_2       VARCHAR2(32767);
    v_hostname       VARCHAR2(30);
  BEGIN
   
    --FOR NETWORK I/O Data Insert ddsc_os_network
      v_hostname:=get_nmon_hostname(v_directory);
      select 'insert into ddsc_os_network  select '''||v_cust_id||''','''||v_hostname||''', t.nmon_date, (' ||
       listagg(column_name ,'+') WITHIN group  (ORDER BY column_name) ||
       ') as TOTAL_READ_KB,'
       into v_netsql_1
      from user_tab_columns
           where table_name = 'NET'
            and /*column_name not like '%BOND%'   and */
             upper(column_name) not like 'LO_%'
            and upper(column_name) like 'BOND%READ%';
            
         select '(' ||
       listagg(column_name ,'+') WITHIN group  (ORDER BY column_name) ||
       ') as TOTAL_WRITE_KB from NET n,NMON_TIME t
       where n.timeflag=t.timeflag order by n.timeflag'
       into v_netsql_2
      from user_tab_columns
           where table_name = 'NET'
            and /*column_name not like '%BOND%'   and */
             upper(column_name) not like 'LO_%'
            and upper(column_name) like 'BOND%WRITE%';
            
     --dbms_output.put_line(v_hostname);
     --dbms_output.put_line(v_netsql_1||v_netsql_2);
     execute immediate (v_netsql_1||v_netsql_2);
     commit;
     
     /*  insert into ddsc_os_network
  select v_cust_id,
         v_hostname,
         t.nmon_date,
         (BOND0_READ_KB_PERSECS + BONDTEN0_READ_KB_PERSECS) as TOTAL_READ_KB,
         (BOND0_WRITE_KB_PERSECS + BONDTEN0_WRITE_KB_PERSECS) as TOTAL_WRITE_KB
    from NET n, NMON_TIME t
   order by n.timeflag */  
  
  
    FOR i IN (SELECT 'READ' "TYPE"
                FROM dual
              UNION
              SELECT 'WRITE' "TYPE"
                FROM dual
              UNION
              SELECT 'IOPS' "TYPE" FROM dual) LOOP
      dbasrv_tool.nmon_calc(v_cust_id, v_directory, i.type);
      --dbasrv_tool.nmon_tab_drop;
    END LOOP;
    --insert to ddsc_os_diskio table
    INSERT INTO ddsc_os_diskio
      SELECT a.cust_id,
             a.host,
             a.sample_time,
             a.read_kbs,
             b.write_kbs,
             c.iops
        FROM tmp_diskread a, tmp_diskwrite b, tmp_diskiops c
       WHERE b.sample_time = c.sample_time
         AND a.sample_time = c.sample_time;
    COMMIT;
    
     
  
  END;
  
  PROCEDURE nmon_calc_imp_byfile(v_cust_id   IN VARCHAR2,
                                 v_directory IN VARCHAR2,
                                 v_filename  IN VARCHAR2) IS
    IO_TYPE       VARCHAR2(10);
    tmp_diskread  VARCHAR2(30);
    tmp_diskwrite VARCHAR2(30);
    tmp_diskiops  VARCHAR2(30);
  BEGIN
  
    tmp_diskread  := tmp_diskread || split(v_filename, 2, '_');
    tmp_diskwrite := tmp_diskwrite || split(v_filename, 2, '_');
    tmp_diskiops  := tmp_diskiops || split(v_filename, 2, '_');
  
    FOR i IN (SELECT 'READ' "TYPE"
                FROM dual
              UNION
              SELECT 'WRITE' "TYPE"
                FROM dual
              UNION
              SELECT 'IOPS' "TYPE" FROM dual) LOOP
      dbasrv_tool.nmon_calc_byfile(v_cust_id,
                                   v_directory,
                                   i.type,
                                   v_filename);
      --dbasrv_tool.nmon_tab_drop;
    END LOOP;
    --insert to ddsc_os_diskio table
    INSERT INTO ddsc_os_diskio
      SELECT a.cust_id,
             a.host,
             a.sample_time,
             a.read_kbs,
             b.write_kbs,
             c.iops
        FROM tmp_diskread a, tmp_diskwrite b, tmp_diskiops c
       WHERE b.sample_time = c.sample_time
         AND a.sample_time = c.sample_time;
    COMMIT;
  
  END;

  PROCEDURE nmon_tab_drop IS
    v_sql VARCHAR2(32767);
  BEGIN
    FOR c IN (SELECT *
                FROM user_tables
               WHERE (table_name LIKE 'DISK%'
              OR table_name LIKE 'NET%')
              ) LOOP
      v_sql := 'drop table ' || c.table_name || ' purge';
    
      EXECUTE IMMEDIATE v_sql;
    END LOOP;
  END;
END DBASRV_TOOL;
/
