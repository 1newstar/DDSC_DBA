SET CUSTID=HCT
SET tt=%date%
SET yy=%tt:~0,4%
SET mm=%tt:~5,2%
SET dd=%tt:~8,2%
SET OPDATE=%yy%%mm%%dd%
SET OPTIME=%OPDATE% %time:~0,8%
SET SCRIPT_HOME=F:\DBA_REPORT
SET LOCKFILE=%SCRIPT_HOME%\run1h.lock
SET ORACLE_HOME=D:\app\Administrator\product\11.2.0\dbhome_1


REM CHECK RINNING PROCESS
cd /d %SCRIPT_HOME%
IF EXIST %LOCKFILE% (for /F %%i in ('wmic process where ^(CommandLine like 'cmd /c%%ddsc_rpt_1h.bat%%'^) get   CommandLine ^|grep ddsc_rpt_1h.bat ^|wc -l ^|awk "{print $1}"') do (set CMDCOUNT=%%i)) else (goto MAIN)

if %CMDCOUNT% GEQ 1 (echo "THERE IS ANOTHER PROCESS RUNNING" & goto END ) else (goto MAIN) 


:MAIN
SET TOPCPU=%mm%_ddsc_topcpu.his
SET TOPELAS=%mm%_ddsc_topelas.his
SET TOPEXEC=%mm%_ddsc_topexec.his
SET TOPPYRD=%mm%_ddsc_toppyrd.his
SET TOPGETS=%mm%_ddsc_topgets.his




 
REM touch LOCKFILE
copy /y nul %LOCKFILE%

rem Change Oracle Regedit Key Name
REM FOR /F "TOKENS=1-3" %%A IN ('reg query HKEY_LOCAL_MACHINE\SOFTWARE\oracle\KEY_OraDb11g_home1 -v ORACLE_SID') DO SET ORACLE_SID=%%C
REM for /F %%i in ('REG QUERY HKLM\SOFTWARE\ORACLE  /v ORACLE_HOME /t REG_SZ  /s ^|grep "ORACLE_HOME" ^|awk "{print $3}"') do (set ORACLE_HOME=%%i)
REM for /F %%i in ('REG QUERY HKLM\SOFTWARE\ORACLE  /v ORACLE_SID /t REG_SZ  /s ^|grep "ORACLE_SID" ^|awk "{print $3}"') do (set ORACLE_SID=%%i)


sqlplus -s "/ as sysdba" @%SCRIPT_HOME%\top5_sql_cpu.sql  %CUSTID% %COMPUTERNAME% %ORACLE_SID%

sqlplus -s "/ as sysdba" @%SCRIPT_HOME%\top5_sql_elas.sql %CUSTID% %COMPUTERNAME% %ORACLE_SID%

sqlplus -s "/ as sysdba" @%SCRIPT_HOME%\top5_sql_exec.sql %CUSTID% %COMPUTERNAME% %ORACLE_SID%

sqlplus -s "/ as sysdba" @%SCRIPT_HOME%\top5_sql_gets.sql %CUSTID% %COMPUTERNAME% %ORACLE_SID%

sqlplus -s "/ as sysdba" @%SCRIPT_HOME%\top5_sql_read.sql %CUSTID% %COMPUTERNAME% %ORACLE_SID%


cd %SCRIPT_HOME%


type top5_sql_cpu.txt    >> %TOPCPU%
type top5_sql_elas.txt   >> %TOPELAS%
type top5_sql_exec.txt   >> %TOPEXEC%
type top5_sql_gets.txt   >> %TOPPYRD%
type top5_sql_read.txt   >> %TOPGETS%

del /Q %LOCKFILE%


:END