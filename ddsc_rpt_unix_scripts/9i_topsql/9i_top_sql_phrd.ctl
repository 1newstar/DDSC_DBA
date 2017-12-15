load data
infile 'top5_sql_read.txt'  "str X'5e5e0a'"
badfile "top5_sql_read.bad"
discardfile "top5_sql_read.dsd"
append into table TOP5_SQL_READ
fields terminated by '|' optionally enclosed by '"'
trailing nullcols
(
CUST_ID,
HOST,
INST_NAME,
SAMPLE_TIME DATE 'YYYYMMDD HH24MI',
PHYSICAL_READS "TO_NUMBER(:PHYSICAL_READS,'999,999,999,999')",
EXECUTIONS "TO_NUMBER(:EXECUTIONS,'999,999,999,999')",
READS_PER_EXEC "TO_NUMBER(:READS_PER_EXEC,'999,999,999,999.0')",
TOTAL "TO_NUMBER(:TOTAL,'999,999,999,999.0')",
CPU_TIME "TO_NUMBER(:CPU_TIME,'999,999,999,999.00')",
ELAPSED_TIME "TO_NUMBER(:ELAPSED_TIME,'999,999,999,999.00')",
SQL_ID filler,
HASH_VALUE,
SQL_MODULE,
SQL_TEXT CHAR(10000) OPTIONALLY ENCLOSED BY '<clob>' AND '</clob>'
)
