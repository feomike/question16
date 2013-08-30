--create the driver table which is used by the cong_analysis.py

drop table if exists sbi2012dec.cong_driver;
create table sbi2012dec.cong_driver as 
select usf_id, usf_name, to_char(count(*),'999,999,999') as block_count, sum(pop_2012) as pop2012,
       to_char(sum(pop_2012),'999,999,999') as pop_2012
  from sbi2012dec.blockmaster2012
  where usf_id is not null
  group by usf_id, usf_name
  order by pop_2012;

alter table sbi2012dec.cong_driver 
  add column hoconum character varying(6),
  add column hoconame character varying(100),
  add column blcnt integer,
  add column pop integer,
  add column pop_dsl_bl integer,
  add column pop_50_bl integer,
  add column pop_cbl_bl integer, 
  add column pop_fw numeric,
  add column comb_bl integer
  add column percent numeric;

update sbi2012dec.cong_driver set pop_dsl_bl = 0;
update sbi2012dec.cong_driver set pop_50_bl = 0;
update sbi2012dec.cong_driver set pop_cbl_bl = 0;
update sbi2012dec.cong_driver set pop_fw = 0;
update sbi2012dec.cong_driver set comb_bl = 0;
update sbi2012dec.cong_driver set percent = 0;

select * from sbi2012dec.cong_driver
  order by pop2012 desc

--create a test driver table to test individual usf_id's on
drop table if exists sbi2012dec.cong_driver1;
create table sbi2012dec.cong_driver1 as 
  select * 
  from sbi2012dec.cong_driver where usf_id = '9740CA';
select * from sbi2012dec.cong_driver1;
select * from sbi2012dec.inc


--to get the incumbent, this code is run
SELECT hoconum, hoconame, transtech, count(distinct fullfipsid), sum (pop_2012) as pop 
   FROM sbi2012dec.block, sbi2012dec.blockmaster2012 
   WHERE block.fullfipsid=blockmaster2012.block_fips and usf_id = '9740CA' 
   and transtech = 10 
   GROUP BY hoconum, hoconame, transtech 
   ORDER BY POP DESC LIMIT 1; 

--the update is run when this is query is run to set the flag field for an overlapping
--provider with the incumbent
UPDATE sbi2012dec.inc SET pop_fw = 1  where block_fips in ( 
  SELECT DISTINCT block_fips from sbi2012dec.inc INNER JOIN sbi2012dec.fixed_wireless on (
    inc.block_fips=fixed_wireless.fullfipsid) 
    WHERE hoconum <> '130074' and transtech in (70,71) and maxaddown in (5,6,7,8,9,10,11) 
    and maxadup in (3,4,5,6,7,8,9,10,11)  and pct_blk_in_shape > 99 
    order by block_fips
    ) ; 
    COMMIT; 

--here is the example code for the wireless provider which is partially covered
UPDATE sbi2012dec.inc SET pop_fw = .5 where block_fips in ( 
  SELECT DISTINCT block_fips from sbi2012dec.inc INNER JOIN sbi2012dec.fixed_wireless on (
    inc.block_fips=fixed_wireless.fullfipsid) 
    WHERE hoconum <> '130223' and transtech in (70,71) and maxaddown in (5,6,7,8,9,10,11) 
    and maxadup in (3,4,5,6,7,8,9,10,11)  and 
    ( pct_blk_in_shape <= 99 and pct_blk_in_shape >= 1) and pop_fw is null
    ); 
    COMMIT; 

