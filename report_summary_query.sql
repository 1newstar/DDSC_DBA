

--TABLESPCE ALLOCATE SPACE DIFF
select TBS, MIN, MAX, round(((MAX - MIN) / MIN) * 100,1) "ADD%"
  from (select a.tablespace TBS,
               (min(a.megs_alloc) / 1024) MIN,
               (max(a.megs_alloc) / 1024) MAX
          from ddsc_db_tbs a
         where cust_id = 'SKL'
           and inst_name = 'skproddb'
           and tablespace not like 'UNDOTBS%'
           and sample_time > to_date('20170301', 'yyyymmdd')
         group by tablespace
        )  order by 4 desc;


--TABLESPCE USED SPACE DIFF
select TBS, MIN, MAX, round(((MAX - MIN) / MIN) * 100,1) "ADD%"
  from (select a.tablespace TBS,
               (min(a.megs_used) / 1024) MIN,
               (max(a.megs_used) / 1024) MAX
          from ddsc_db_tbs a
         where cust_id = 'SKL'
           and inst_name = 'skproddb'
           and tablespace not like 'UNDOTBS%'
           and sample_time > to_date('20170301', 'yyyymmdd')
         group by tablespace
        )  order by 4 desc;



--DB SIZE DIFF

select 'OLDER',round(a.datfile_size/1024,1) 
  from ddsc_db_size a
 where cust_id = 'SKL'
   and inst_name = 'skproddb'
   and sample_time =
       (select min(sample_time)
          from ddsc_db_size a
         where cust_id = 'SKL'
           and inst_name = 'skproddb'
           and sample_time >= to_date('20170301', 'yyyymmdd'))
union
select 'NEWER',round(a.datfile_size/1024,1)
  from ddsc_db_size a
 where cust_id = 'SKL'
   and inst_name = 'skproddb'
   and sample_time =
       (select max(sample_time)
          from ddsc_db_size a
         where cust_id = 'SKL'
           and inst_name = 'skproddb'
           and sample_time >= to_date('20170301', 'yyyymmdd'))
union
select 'DIFF',
((select round(a.datfile_size/1024,1)
  from ddsc_db_size a
 where cust_id = 'SKL'
   and inst_name = 'skproddb'
   and sample_time =
       (select max(sample_time)
          from ddsc_db_size a
         where cust_id = 'SKL'
           and inst_name = 'skproddb'
           and sample_time >= to_date('20170301', 'yyyymmdd')))-
(select round(a.datfile_size/1024,1)
  from ddsc_db_size a
 where cust_id = 'SKL'
   and inst_name = 'skproddb'
   and sample_time =
       (select min(sample_time)
          from ddsc_db_size a
         where cust_id = 'SKL'
           and inst_name = 'skproddb'
           and sample_time >= to_date('20170301', 'yyyymmdd'))
           )) from dual;