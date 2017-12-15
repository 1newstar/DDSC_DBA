SET CUSTID=HCT
SET tt=%date%
SET yy=%tt:~0,4%
SET mm=%tt:~5,2%
SET dd=%tt:~8,2%
SET OPDATE=%yy%%mm%%dd%
SET OPTIME=%OPDATE% %time:~0,8%
SET SCRIPT_HOME=F:\DBA_REPORT
SET RETEN_DAY=100
SET LOCKFILE=%SCRIPT_HOME%\run1d.lock
SET ORACLE_HOME=D:\app\Administrator\product\11.2.0\dbhome_1


REM CHECK RINNING PROCESS
cd /d %SCRIPT_HOME%
IF EXIST %LOCKFILE% (for /F %%i in ('wmic process where ^(CommandLine like 'cmd /c%%ddsc_rpt_1d.bat%%'^) get   CommandLine ^|grep ddsc_rpt_1d.bat ^|wc -l ^|awk "{print $1}"') do (set CMDCOUNT=%%i)) else (goto MAIN)

if %CMDCOUNT% GEQ 1 (echo "THERE IS ANOTHER PROCESS RUNNING" & goto END ) else (goto MAIN) 


:MAIN
SET BAK=%mm%_ddsc_bak.his
SET DF=%mm%_ddsc_df.his
SET IDX=%mm%_ddsc_idx.his
SET INST=%mm%_ddsc_inst.his
SET LOGSW=%mm%_ddsc_logsw.his
SET OBJ=%mm%_ddsc_obj.his
SET PAR=%mm%_ddsc_par.his
SET SIZE=%mm%_ddsc_size.his
SET TBS=%mm%_ddsc_tbs.his
SET SEG=%mm%_ddsc_seg.his

copy /y nul %LOCKFILE%

rem Change Oracle Regedit Key Name
REM FOR /F "TOKENS=1-3" %%A IN ('reg query HKEY_LOCAL_MACHINE\SOFTWARE\oracle\KEY_OraDb11g_home1 -v ORACLE_SID') DO SET ORACLE_SID=%%C
REM for /F %%i in ('REG QUERY HKLM\SOFTWARE\ORACLE  /v ORACLE_HOME /t REG_SZ  /s ^|grep "ORACLE_HOME" ^|awk "{print $3}"') do (set ORACLE_HOME=%%i)
REM for /F %%i in ('REG QUERY HKLM\SOFTWARE\ORACLE  /v ORACLE_SID /t REG_SZ  /s ^|grep "ORACLE_SID" ^|awk "{print $3}"') do (set ORACLE_SID=%%i)

sqlplus -s "/ as sysdba" @%SCRIPT_HOME%\ddsc_rpt_1d.sql %CUSTID% %COMPUTERNAME% %ORACLE_SID% "%OPTIME%"



cd %SCRIPT_HOME%


type ddsc_db_bak.txt     >> %BAK%
type ddsc_db_df.txt      >> %DF%
type ddsc_db_idx.txt     >> %IDX%
type ddsc_db_inst.txt    >> %INST%
type ddsc_db_logsw.txt   >> %LOGSW%
type ddsc_db_obj.txt     >> %OBJ%
type ddsc_db_par.txt     >> %PAR%
type ddsc_db_size.txt    >> %SIZE%
type ddsc_db_tbs.txt     >> %TBS%
type ddsc_db_seg.txt     >> %SEG%

find %SCRIPT_HOME% -name "*.his" -type f -mtime +90 -exec cmd /C del /Q {} \ ;
del /Q %LOCKFILE%

:END