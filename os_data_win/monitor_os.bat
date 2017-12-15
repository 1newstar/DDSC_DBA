REM A0001,p615,CPU Total p615,User%,Sys%,Wait%,Idle%,Busy,PhysicalCPUs,Runnable
REM USER PARAMETER
SET CUST_ID=A0001
set LOG_DIR="D:\公司\中菲_DDSC\中菲ORACLE DBA SERVICE\健檢改善_BIP\os_data\windows"
set count=19
set refresh=1
set retention=90



REM MAIN
cd /d %LOG_DIR%

REM GET PHYSICAL MEMORY
REM WMIC MEMORYCHIP GET capacity |findstr [0-9]|awk "{sum=+$0} END {print sum/1024/1024}"
REM for /F %%i in ('WMIC MEMORYCHIP GET capacity ^|findstr [0-9]^|awk "{sum=+$0} END {print sum/1024/1024}"') do set total_mem=%%i
for /F %%i in ('systeminfo ^| findstr -C:"Total Physical Memory" -C:"實體記憶體總計" ^|awk "{sub(/,/,null,$2);print $2}"') do set total_mem=%%i


for /f %%i in ('hostname') do set host=%%i
set CPU_LOGFILE=%date:~0,4%%date:~5,2%%date:~8,2%_%host%_cpu_usage_win.csv
set MEM_LOGFILE=%date:~0,4%%date:~5,2%%date:~8,2%_%host%_mem_free_win.csv
set DISK_LOGFILE=%date:~0,4%%date:~5,2%%date:~8,2%_%host%_diskio_win.csv
set PGSP_LOGFILE=%date:~0,4%%date:~5,2%%date:~8,2%_%host%_pgsp_win.csv

REM CPU ITEM
echo \processor(_total)\%% User Time >%LOG_DIR%\ddsc_cpu.txt
echo \processor(_total)\%% Privileged Time >>%LOG_DIR%\ddsc_cpu.txt
echo \processor(_total)\%% Interrupt Time  >>%LOG_DIR%\ddsc_cpu.txt
echo \processor(_total)\%% Idle Time  >>%LOG_DIR%\ddsc_cpu.txt


REM MEMORY ITEM
echo \memory\Available MBytes >%LOG_DIR%\ddsc_memory.txt

REM PAGING ITEM
echo \memory\Pages Input/sec >%LOG_DIR%\ddsc_paging.txt
echo \memory\Pages Output/sec >>%LOG_DIR%\ddsc_paging.txt

REM DISKIO ITEM
echo \PhysicalDisk(_Total)\Disk Read Bytes/sec >%LOG_DIR%\ddsc_diskio.txt
echo \PhysicalDisk(_Total)\Disk Write Bytes/sec >>%LOG_DIR%\ddsc_diskio.txt
echo \PhysicalDisk(_Total)\Disk Transfers/sec >>%LOG_DIR%\ddsc_diskio.txt






REM PAGING ITEM
echo \memory\Pages Input/sec >%LOG_DIR%\ddsc_paging.txt
echo \memory\Pages Output/sec >>%LOG_DIR%\ddsc_paging.txt



set /a v_cnt=%count%+3


echo typeperf -y -si %refresh%  -sc %count% -cf    ddsc_cpu.txt ^|awk "NR>2 && NR<%v_cnt% {print CUST_ID """,""",host """,""", $0 }" CUST_ID=%CUST_ID% host=%host% ^>^>  %LOG_DIR%\%CPU_LOGFILE%  >run_cpu.bat
echo exit >>run_cpu.bat

echo typeperf -y -si %refresh%  -sc %count% -cf ddsc_diskio.txt ^|awk "NR>2 && NR<%v_cnt% {print CUST_ID """,""",host """,""", $0 }" CUST_ID=%CUST_ID% host=%host% ^>^>  %LOG_DIR%\%DISK_LOGFILE% >run_diskio.bat
echo exit >>run_diskio.bat

REM echo typeperf -y -si %refresh%  -sc %count% -cf ddsc_memory.txt ^|awk "NR>2 && NR<%v_cnt% {print CUST_ID """,""",host """,""", $0 """,""" total_mem}" CUST_ID=%CUST_ID% host=%host% total_mem=%total_mem% ^>^>  %LOG_DIR%\%MEM_LOGFILE%  >run_mem.bat
echo typeperf -y -si %refresh%  -sc %count% -cf ddsc_memory.txt ^|awk -F"," "NR>2 && NR<%v_cnt% {print CUST_ID """,""",host """,""",$1 """,""",total_mem """,""",$2}" CUST_ID=%CUST_ID% host=%host% total_mem=%total_mem% ^>^>  %LOG_DIR%\%MEM_LOGFILE%  >run_mem.bat
echo exit >>run_mem.bat

echo typeperf -y -si %refresh%  -sc %count% -cf ddsc_paging.txt ^|awk "NR>2 && NR<%v_cnt% {print CUST_ID """,""",host """,""", $0 }" CUST_ID=%CUST_ID% host=%host% ^>^>  %LOG_DIR%\%PGSP_LOGFILE% >run_pgsp.bat
echo exit >>run_pgsp.bat


find . -name *.csv -type f -mtime +%retention% -print |awk "{print """del /Q""" , $0}" >rmfile.bat
echo exit >>rmfile.bat


start /B rmfile.bat

start /B run_cpu.bat
start /B run_diskio.bat
start /B run_mem.bat
start /B run_pgsp.bat



