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
SET LINE 2000
COLUMN SQL_TEXT FORMAT A32766 WORD WRAPPED

COL SNAP_ID NEW_VALUE V_BID
SELECT SNAP_ID
FROM STATS$SNAPSHOT
WHERE TO_CHAR(SNAP_TIME,'YYYY/MM/DD HH24') = TO_CHAR((SYSDATE-60/1440),'YYYY/MM/DD HH24');

COL SNAP_ID NEW_VALUE V_EID
SELECT SNAP_ID
FROM STATS$SNAPSHOT
WHERE TO_CHAR(SNAP_TIME,'YYYY/MM/DD HH24') = TO_CHAR(SYSDATE,'YYYY/MM/DD HH24');

--END SNAPSHOT TIME
COL ESANP_TIME NEW_VALUE V_ESANP_TIME
select  TO_CHAR(SNAP_TIME,'YYYYMMDD HH24MI') "ESANP_TIME"
FROM STATS$SNAPSHOT
where SNAP_ID='&V_EID';


COL DBID NEW_VALUE V_DBID
SELECT DBID FROM V$DATABASE;

COL NAME NEW_VALUE V_DBNAME
SELECT NAME FROM V$DATABASE;

COL NAME NEW_VALUE V_INSTNAME
SELECT NAME FROM V$DATABASE;

variable bid             number;
variable eid             number;
variable inst_num    number;
variable dbid           number;
variable para           varchar2(9);
variable db_name    varchar2(20);
variable inst_name  varchar2(20);

begin
  :bid       :=  '&v_bid';
  :eid       :=  '&v_eid';
  :para      := '0';
  :db_name   := '&v_dbname';
  :inst_name := '&v_instname';
  :inst_num :=1;
  :dbid  := '&v_dbid';
end;
/

set heading off;
variable lhtr   number;
variable bfwt   number;
variable tran   number;
variable chng   number;
variable ucal   number;
variable urol   number;
variable ucom   number;
variable rsiz   number;
variable phyr   number;
variable phyrd  number;
variable phyrdl number;
variable phyw   number;
variable prse   number;
variable hprs   number;
variable recr   number;
variable gets   number;
variable rlsr   number;
variable rent   number;
variable srtm   number;
variable srtd   number;
variable srtr   number;
variable strn   number;
variable call   number;
variable lhr    number;
variable sp     varchar2(512);
variable bc     varchar2(512);
variable lb     varchar2(512);
variable bs     varchar2(512);
variable twt    number;
variable logc   number;
variable prscpu number;
variable prsela number;
variable tcpu   number;
variable exe    number;
variable bspm   number;
variable espm   number;
variable bfrm   number;
variable efrm   number;
variable blog   number;
variable elog   number;
variable bocur  number;
variable eocur  number;

variable dmsd   number;
variable dmfc   number;
variable dmsi   number;
variable pmrv   number;
variable pmpt   number;
variable npmrv   number;
variable npmpt   number;
variable dbfr   number;
variable dpms   number;
variable dnpms   number;
variable glsg   number;
variable glag   number;
variable glgt   number;
variable glsc   number;
variable glac   number;
variable glct   number;
variable glrl   number;
variable gcdfr  number;
variable gcge   number;
variable gcgt   number;
variable gccv   number;
variable gcct   number;
variable gccrrv   number;
variable gccrrt   number;
variable gccurv   number;
variable gccurt   number;
variable gccrsv   number;
variable gccrbt   number;
variable gccrft   number;
variable gccrst   number;
variable gccusv   number;
variable gccupt   number;
variable gccuft   number;
variable gccust   number;
variable msgsq    number;
variable msgsqt   number;
variable msgsqk   number;
variable msgsqtk  number;
variable msgrq    number;
variable msgrqt   number;
                                                    
begin                                               
  STATSPACK.STAT_CHANGES
   ( :bid,    :eid
   , :dbid,   :inst_num
   , :para                 -- End of IN arguments
   , :lhtr,   :bfwt
   , :tran,   :chng
   , :ucal,   :urol
   , :rsiz
   , :phyr,   :phyrd
   , :phyrdl
   , :phyw,   :ucom
   , :prse,   :hprs
   , :recr,   :gets
   , :rlsr,   :rent
   , :srtm,   :srtd
   , :srtr,   :strn
   , :lhr,    :bc
   , :sp,     :lb
   , :bs,     :twt
   , :logc,   :prscpu
   , :tcpu,   :exe
   , :prsela
   , :bspm,   :espm
   , :bfrm, :efrm
   , :blog,   :elog
   , :bocur,  :eocur
   , :dmsd,   :dmfc    -- Begin of RAC
   , :dmsi
   , :pmrv,   :pmpt 
   , :npmrv,  :npmpt 
   , :dbfr
   , :dpms,   :dnpms 
   , :glsg,   :glag 
   , :glgt,   :glsc 
   , :glac,   :glct 
   , :glrl,   :gcdfr
   , :gcge,   :gcgt 
   , :gccv,   :gcct
   , :gccrrv, :gccrrt 
   , :gccurv, :gccurt 
   , :gccrsv
   , :gccrbt, :gccrft 
   , :gccrst, :gccusv 
   , :gccupt, :gccuft 
   , :gccust
   , :msgsq,  :msgsqt
   , :msgsqk, :msgsqtk
   , :msgrq,  :msgrqt           -- End RAC
   );
   :call := :ucal + :recr;
end;
/

repfooter off;
ttitle off;
btitle off;
set timing off veri off space 1 flush on pause off termout on numwidth 10;
set echo off feedback off pagesize 60 linesize 10000 newpage 1 recsep off;
set trimspool on trimout on;
define top_n_events = 5;
define top_n_sql = 65;
define top_n_segstat = 5;
define num_rows_per_hash=5;


SET HEAD OFF
SET ECHO OFF
SET FEEDBACK OFF


column hv noprint;
break on hv skip 1;
column hv noprint;
break on hv skip 1;

spool /home/ora9i/jerel/top_sql_exec.txt
select aa
  from ( select /*+ ordered use_nl (b st) */
          decode( st.piece
                , 0
                , '^^'||chr(10)||'&1|&2|&3|'||'&V_ESANP_TIME'||'|'||lpad(to_char((e.executions - nvl(b.executions,0))
                               ,'999,999,999')
                      ,12)||'|'||
                  lpad(to_char((nvl(e.rows_processed,0) - nvl(b.rows_processed,0))
                              ,'99,999,999,999')
                      ,15)||'|'||
                  lpad((to_char(decode(nvl(e.rows_processed,0) - nvl(b.rows_processed,0)
                                     ,0, 0
                                     ,(e.rows_processed - nvl(b.rows_processed,0)) /
                                      (e.executions - nvl(b.executions,0)))
                               ,'9,999,999,990.0'))
                      ,16) ||'|'||
                  lpad(nvl(to_char(   (e.cpu_time   - nvl(b.cpu_time,0))
                                     /(e.executions - nvl(b.executions,0))
                                   /1000000
                               , '999990.00'),' '),10) || '|' ||
                  lpad(nvl(to_char(   (e.elapsed_time - nvl(b.elapsed_time,0))
                                 /(e.executions   - nvl(b.executions,0))
                                 /1000000
                               , '9999990.00'),' '),11) || '||' ||
                  lpad(e.hash_value,10)||'|'||
                  decode(e.module,null,'|'||st.sql_text,'Module: '||e.module||'|'||st.sql_text)
                , st.sql_text) aa
          , e.hash_value hv
       from stats$sql_summary e
          , stats$sql_summary b
          , stats$sqltext     st
      where b.snap_id(+)         = '&V_BID' 
        and b.dbid(+)            = e.dbid
        and b.instance_number(+) = e.instance_number
        and b.hash_value(+)      = e.hash_value
        and b.address(+)         = e.address
        and b.text_subset(+)     = e.text_subset
        and e.snap_id            = '&V_EID' 
        and e.dbid               = '&V_DBID' 
        and e.instance_number    = userenv('instance')
        and e.hash_value         = st.hash_value
        and e.text_subset        = st.text_subset
        and st.piece             < 20
        and e.executions         > nvl(b.executions,0)
      order by (e.executions - nvl(b.executions,0)) desc, e.hash_value, st.piece
      )
where rownum < 65;

spool off
exit
