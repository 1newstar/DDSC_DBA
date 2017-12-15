SET CUSTID=HCT
SET tt=%date%
SET yy=%tt:~0,4%
SET mm=%tt:~5,2%
SET dd=%tt:~8,2%
SET OPDATE=%yy%%mm%%dd%
SET OPTIME=%OPDATE% %time:~0,8%
SET SCRIPT_HOME=F:\DBA_REPORT
SET LOCKFILE=%SCRIPT_HOME%\runshort.lock

REM CHECK RINNING PROCESS
cd /d %SCRIPT_HOME%
IF EXIST %LOCKFILE% (for /F %%i in ('wmic process where ^(CommandLine like 'cmd /c%%ddsc_rpt_5m.bat%%'^) get   CommandLine ^|grep ddsc_rpt_5m.bat ^|wc -l ^|awk "{print $1}"') do (set CMDCOUNT=%%i)) else (goto MAIN)
if %CMDCOUNT% GEQ 1 (echo "THERE IS ANOTHER PROCESS RUNNING" & goto END ) else (goto MAIN) 



:MAIN
SET DICT=%mm%_ddsc_dict.his
SET LIB=%mm%_ddsc_lib.his
SET PGA=%mm%_ddsc_pga.his
SET SESS=%mm%_ddsc_sess.his
REM SET SGACOM=%mm%_ddsc_sgacom.his
SET SGARS=%mm%_ddsc_sgars.his
SET SHARE=%mm%_ddsc_share.his
SET SYSSTAT=%mm%_ddsc_sysstat.his
SET UNDO=%mm%_ddsc_undo.his
SET TEMP=%mm%_ddsc_temp.his
SET ORACLE_HOME=D:\app\Administrator\product\11.2.0\dbhome_1

 
REM touch LOCKFILE
copy /y nul %LOCKFILE%


rem Change Oracle Regedit Key Name
REM FOR /F "TOKENS=1-3" %%A IN ('reg query HKEY_LOCAL_MACHINE\SOFTWARE\oracle\KEY_OraDb11g_home1 -v ORACLE_SID') DO SET ORACLE_SID=%%C
REM for /F %%i in ('REG QUERY HKLM\SOFTWARE\ORACLE  /v ORACLE_HOME /t REG_SZ  /s ^|grep "ORACLE_HOME" ^|awk "{print $3}"') do (set ORACLE_HOME=%%i)
REM for /F %%i in ('REG QUERY HKLM\SOFTWARE\ORACLE  /v ORACLE_SID /t REG_SZ  /s ^|grep "ORACLE_SID" ^|awk "{print $3}"') do (set ORACLE_SID=%%i)

sqlplus -s "/ as sysdba" @%SCRIPT_HOME%\ddsc_rpt_5m.sql %CUSTID% %COMPUTERNAME% %ORACLE_SID% "%OPTIME%"



cd %SCRIPT_HOME%

type ddsc_db_dict.txt    >> %DICT%
type ddsc_db_lib.txt     >> %LIB%
type ddsc_db_pga.txt     >> %PGA%
type ddsc_db_sess.txt    >> %SESS%
REM type ddsc_db_sgacom.txt  >> %SGACOM%
type ddsc_db_sgars.txt   >> %SGARS%
type ddsc_db_share.txt   >> %SHARE%
type ddsc_db_sysstat.txt >> %SYSSTAT%
type ddsc_db_undo.txt    >> %UNDO%
type ddsc_db_temp.txt    >> %TEMP%

del /Q %LOCKFILE%


:END
