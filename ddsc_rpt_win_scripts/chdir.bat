SET SCRIPT_HOME=%CD%
for /F "tokens=*" %%k in ('echo %SCRIPT_HOME%^|sed  "s/\\/\\\\/g"') do set SCRIPT_DIR=%%k
for /F %%i in ('dir /B *.sql') do sed "s/DATA_DIR/%SCRIPT_DIR%/g" %%i >%%i.tmp
for /F %%j in ('dir /B *.sql') do copy /Y %%j.tmp %%j
del /Q *.tmp


