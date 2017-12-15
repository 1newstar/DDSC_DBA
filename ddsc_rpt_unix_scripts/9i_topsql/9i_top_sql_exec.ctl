load data
infile 'top5_sql_exec.txt' "str X'5e5e0a'"
badfile "top5_sql_exec.bad"
discardfile "top5_sql_exec.dsd"
append into table TOP5_SQL_EXEC
fields terminated by '|' optionally enclosed by '"'
trailing nullcols
(
CUST_ID,
HOST,
INST_NAME,
SAMPLE_TIME DATE 'YYYYMMDD HH24MI',
EXECUTIONS "TO_NUMBER(:EXECUTIONS,'999,999,999,999')",
ROWS_PROCESSED "TO_NUMBER(:ROWS_PROCESSED,'999,999,999,999.0')",
ROWS_PER_EXEC "TO_NUMBER(:ROWS_PER_EXEC,'999,999,999,999.0')",
CPU_PER_EXEC "TO_NUMBER(:CPU_PER_EXEC,'999,999,999,999.00')",
ELAP_PER_EXEC "TO_NUMBER(:ELAP_PER_EXEC,'999,999,999,999.00')",
SQL_ID filler,
HASH_VALUE,
SQL_MODULE,
SQL_TEXT CHAR(10000) OPTIONALLY ENCLOSED BY '<clob>' AND '</clob>'
)
