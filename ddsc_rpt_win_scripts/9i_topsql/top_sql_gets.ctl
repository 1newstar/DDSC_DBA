load data
infile 'top_sql_gets.txt'  "str X'5e5e0a'"
badfile "top_sql_gets.bad"
discardfile "top_sql_gets.dsd"
append into table TOP5_SQL_GETS
fields terminated by '|' optionally enclosed by '"'
trailing nullcols
(
CUST_ID,
HOST,
INST_NAME,
SAMPLE_TIME DATE 'YYYYMMDD HH24MI',
BUFFER_GETS "TO_NUMBER(:BUFFER_GETS,'999,999,999,999')",
EXECUTIONS "TO_NUMBER(:EXECUTIONS,'999,999,999,999')",
GETS_PER_EXEC "TO_NUMBER(:GETS_PER_EXEC,'999,999,999,999.0')",
TOTAL "TO_NUMBER(:TOTAL,'999,999,999,999.0')",
CPU_TIME  "TO_NUMBER(:CPU_TIME,'999,999,999,999.00')",
ELAPSED_TIME "TO_NUMBER(:ELAPSED_TIME,'999,999,999,999.00')",
HASH_VALUE,
SQL_MODULE,
SQL_TEXT CHAR(10000) OPTIONALLY ENCLOSED BY '<clob>' AND '</clob>'   
)
