--investigate some oddities in the resulting data

--find all of the rows after the analysis which have 0 population (that is no pop in the incumbent's footprint
select * from sbi2012dec.cong_driver
  where pop = 0
  order by pop_2012 desc;

--find out how the incumbent was selected
SELECT hoconum, hoconame, transtech, count(distinct fullfipsid), sum (pop_2012) as pop 
  FROM sbi2012dec.block, sbi2012dec.blockmaster2012 
  WHERE block.fullfipsid=blockmaster2012.block_fips and usf_id = '2404OR' and transtech = 10 
  GROUP BY hoconum, hoconame, transtech 
  ORDER BY POP DESC, count desc 
  LIMIT 1; 

--find all the rows after the analysis which have no overlap w/ any other provider
select * from sbi2012dec.cong_driver
  where comb_bl = 0
  order by pop_2012 desc;

--find all the rows after the analysis which have no incumbent
--this means the incumbent doesn't actually have a broadband service
select * from sbi2012dec.cong_driver
  where hoconum is null
  order by pop_2012 desc;


--find the rows w/ a certain percent overlap
select * from sbi2012dec.cong_driver
  where percent = 100
  order by pop_2012 desc

select * from sbi2012dec.cong_driver
  where percent > 99 and percent < 100
  order by pop_2012 desc

select * from sbi2012dec.cong_driver
  where percent > 98 and percent < 99
  order by pop_2012 desc

select * from sbi2012dec.cong_driver
  where percent > 90 
  order by pop_2012 desc

select * from sbi2012dec.cong_driver
--  where percent > 90 and percent < 95
  order by pop_2012 desc
  
