## ---------------------------------------------------------------------------
###   VERSION 0.1 (for postgis)
### cong_analysis.py
### Created on: august 25, 2013
### Created by: Michael Byrne
### Federal Communications Commission 
##
## ---------------------------------------------------------------------------

# Import system modules
import sys, string, os
import psycopg2
import time
now = time.localtime(time.time())
print "local time:", time.asctime(now)

#variables
myHost = "localhost"
myPort = "54321"
myUser = "postgres"
db = "feomike"
schema = "sbi2012dec"
driverTB = "cong_driver"

#update the driver table
def upd_driver(myField, myValue, myUSFID):
     updCur = conn.cursor()
     mySQL = "UPDATE " + schema + "." + driverTB + " set " + myField + " = " + myValue 
     mySQL = mySQL + " where usf_id = " + myUSFID + " ; COMMIT; "
     updCur.execute(mySQL)
     updCur.close()
     del mySQL, updCur

#find the incumbent in the usf area
def doIncumbent(myUSFid):
     incCur = conn.cursor()
     mySQL = "SELECT hoconum, hoconame, transtech, count(distinct fullfipsid), sum (pop_2012) as pop "
     mySQL = mySQL + "FROM " + schema + ".block, " + schema + ".blockmaster2012 "
     mySQL = mySQL + "WHERE block.fullfipsid=blockmaster2012.block_fips "
     mySQL = mySQL + "and usf_id = '" + myUSFid + "' and transtech = 10 "
     mySQL = mySQL + "GROUP BY hoconum, hoconame, transtech " 
     mySQL = mySQL + "ORDER BY POP DESC "
     mySQL = mySQL + "LIMIT 1; "
     print mySQL
     incCur.execute(mySQL)
     r = incCur.fetchone()
     if r <> None:
     	hoconum = r[0]
     	hoconame = r[1]
     	blcnt = r[3]
     	pop = int(r[4])
     	#insert the hoconum, hoconame, blcnt, pop into the driver table
     	upd_driver("hoconum", "'" + hoconum + "'" , "'" + myUSFid + "'")
     	#upd_driver("hoconame", "'" + hoconame + "'" , "'" + myUSFid + "'")
     	upd_driver("blcnt", str(blcnt) , "'" + myUSFid + "'")
     	upd_driver("pop", str(pop) , "'" + myUSFid + "'")     	     	     	     	
     	#print "        " + hoconum + "; " + hoconame + "; " + str(blcnt) + "; " + str(pop)
     	#create a table w/ the block footprint of the incumbent in the Study area
     	mySQL = "DROP TABLE IF EXISTS " + schema + ".inc; " 
     	mySQL = mySQL + "CREATE TABLE " + schema + ".inc "
     	mySQL = mySQL + "AS SELECT block_fips, pop_2012 FROM " + schema + ".block, "
     	mySQL = mySQL + schema + ".blockmaster2012 "
     	mySQL = mySQL + "WHERE hoconum = '" + hoconum + "' and transtech = 10 and "
     	mySQL = mySQL + "usf_id = '" + myUSFid + "' "
     	mySQL = mySQL + "and block.fullfipsid=blockmaster2012.block_fips "
     	mySQL = mySQL + "ORDER BY block_fips; "
     	mySQL = mySQL + "CREATE INDEX " + schema + "_inc_block_fips_btree " 
     	mySQL = mySQL + "ON " + schema + ".inc  using btree (block_fips); "
     	mySQL = mySQL + "COMMIT; "
     	#print mySQL
     	incCur.execute(mySQL)     	
#	    #get the number of blocks and pop for not the incumbent provider w/ Copper
        #likely need to redo this w/ 10, 20, and 30
     	doNotIncumbentBlock("block", hoconum, '(10)', 'pop_dsl_bl', myUSFid)
#     	#get the number of blocks and pop for not the incumbent provider w/ Fiber
     	doNotIncumbentBlock("block", hoconum, '(50)', 'pop_50_bl', myUSFid)
#     	#get the number of blocks and pop for not the incumbent provider w/ Cable
     	doNotIncumbentBlock("block", hoconum, '(40,41)', 'pop_cbl_bl', myUSFid)
#     	#get the number of blocks and pop for those with fixed wireless coverage
     	doNotIncumbentBlock("fixed_wireless", hoconum, '(70,71)', 'pop_fw', myUSFid)
     	del hoconame, hoconum, blcnt, pop
     	upd_combination_overlay(myUSFid)
     incCur.close()
     del r, mySQL, incCur    

#find the total of all other providers which overlap the footprint of the incumbent
def doNotIncumbentBlock(myTable, myHoconum, myTT, myField, myUSFid):
	 myCur = conn.cursor()
	 mySQL = "ALTER TABLE " + schema + ".inc ADD COLUMN " + myField + " numeric; "
	 mySQL = mySQL + "UPDATE " + schema + ".inc SET " + myField + " = 1  where block_fips in ( "
	 mySQL = mySQL + "SELECT DISTINCT block_fips from " + schema + ".inc "
	 mySQL = mySQL + "INNER JOIN " + schema + "." + myTable + " on (inc.block_fips=" 
	 mySQL = mySQL + myTable + ".fullfipsid) "
	 mySQL = mySQL + "WHERE hoconum <> '" + myHoconum + "' and transtech in " + myTT 
	 mySQL = mySQL + " and maxaddown in (5,6,7,8,9,10,11) and maxadup in (3,4,5,6,7,8,9,10,11) " 
	 #might need to add maxaddown, maxadup
	 ##if myTable = fixed_wireless, then add those that are 100% overlap
	 if myTable == "fixed_wireless":
		 mySQL = mySQL + " and pct_blk_in_shape > 99 "
	 mySQL = mySQL + ") ; COMMIT; "
	 myCur.execute(mySQL)
	 mySQL = "SELECT sum(pop_2012) FROM " + schema + ".inc WHERE " + myField + " = 1; "
	 myCur.execute(mySQL)
	 r = myCur.fetchone()
	 if r[0] <> None:
	 	#print "          " + str(myTT) + " pop is: " + str(r[0])
	    #insert the overlapping population into the driver table
	 	upd_driver(myField, str(r[0]) , "'" + myUSFid + "'")
	 if myTable == "fixed_wireless":
	 	doFixedWirelessHalf(myTable, myHoconum, myTT, myField, myUSFid)
	 myCur.close()
	 del r, myCur, mySQL

#at this stage you are working the fixed_wireless table and you need to add in just
#the partially covered population
def doFixedWirelessHalf(myTable, myHoconum, myTT, myField, myUSFid):
	 myCur = conn.cursor()
	 mySQL = "UPDATE " + schema + ".inc SET " + myField + " = .5  where block_fips in ( "
	 mySQL = mySQL + "SELECT DISTINCT block_fips from " + schema + ".inc "
	 mySQL = mySQL + "INNER JOIN " + schema + "." + myTable + " on (inc.block_fips=" 
	 mySQL = mySQL + myTable + ".fullfipsid) "
	 mySQL = mySQL + "WHERE hoconum <> '" + myHoconum + "' and transtech in " + myTT 
	 mySQL = mySQL + " and maxaddown in (5,6,7,8,9,10,11) and maxadup in (3,4,5,6,7,8,9,10,11) " 
	 mySQL = mySQL + " and ( pct_blk_in_shape <= 99 and pct_blk_in_shape >= 1) and pop_fw is null ); COMMIT; "
	 myCur.execute(mySQL)
	 #print mySQL
	 mySQL = "SELECT sum(pop_2012 * " + myField + ") FROM " + schema + ".inc WHERE " 
	 mySQL = mySQL + myField + " = .5 and (pop_dsl_bl is null and pop_50_bl is null and pop_cbl_bl is null) ; "
	 myCur.execute(mySQL)
	 r = myCur.fetchone()
	 if r[0] <> None:
	 	#print "          " + str(myTT) + " pop is: " + str(r[0])
	 	#insert the overlapping population into the driver table
	 	upd_driver(myField, str(r[0]) + "+pop_fw" , "'" + myUSFid + "'")
	 myCur.close()
	 del r, myCur, mySQL

#find the population of the combination of overlays
def upd_combination_overlay(myUSFID):
     #first add the values where any are = 1 (this is an or)
     myCur = conn.cursor()
     mySQL = "SELECT sum(pop_2012) FROM " + schema + ".inc WHERE ( pop_dsl_bl = 1 or "
     mySQL = mySQL + "pop_50_bl = 1 or pop_cbl_bl = 1 or pop_fw = 1); "
     #print mySQL
     myCur.execute(mySQL)
     r = myCur.fetchone()
     if r[0] <> None:
     	#print "           comb pop is: " + str(r[0])
     	#insert the overlapping population into the driver table
     	upd_driver("comb_bl", str(r[0]) , "'" + myUSFID + "'")
     #next add the values where only half are available
     mySQL = "SELECT sum(pop_2012*pop_fw) FROM " + schema + ".inc WHERE ( pop_dsl_bl is null and "
     mySQL = mySQL + "pop_50_bl is null and pop_cbl_bl is null and pop_fw = .5); "
     #print mySQL
     myCur.execute(mySQL)
     r = myCur.fetchone()
     if r[0] <> None:
     	#print "           comb pop is: " + str(r[0])
     	#insert the overlapping population into the driver table
     	upd_driver("comb_bl", str(r[0]) + "+comb_bl" , "'" + myUSFID + "'")
     #next add the values where only half are available
     myCur.close()
     del r, myCur, mySQL

#update the hoconame wholesale.  do this because the cursor method throws errors w/ names
def doFinalUpdate():
     myCur = conn.cursor()
     #update the hoconame field
     mySQL = "UPDATE " + schema + "." +  driverTB + " set hoconame = block.hoconame from "
     mySQL = mySQL + schema + ".block where " + driverTB + ".hoconum=block.hoconum; "
     mySQL = mySQL + " COMMIT; "
     myCur.execute(mySQL)     
     #update the percent field
     mySQL = "UPDATE " + schema + "." + driverTB + " set percent = (comb_bl::float / "
     mySQL = mySQL + "pop::float)*100 where pop > 0 and comb_bl > 0; COMMIT; "      
     myCur.execute(mySQL)     
     myCur.close()
     del myCur, mySQL

     
#set up the connection to the database
myConn = "dbname=" + db + " host=" + myHost + " port=" + myPort + " user=" + myUser
conn = psycopg2.connect(myConn)

##do the incumbent one
##loop for each usf
driveCur = conn.cursor()
driveSQL = "SELECT * FROM " + schema + "." + driverTB + "; "
driveCur.execute(driveSQL)
driveData = driveCur.fetchall()
for r in driveData:
	usfid = r[0]
	print "    doing the incumbent for: " + usfid + " and the name is: " + r[1]
	doIncumbent(usfid)
	del usfid
driveCur.close()     
doFinalUpdate()
del r, driveData, driveCur, driveSQL
now = time.localtime(time.time())
print "local time:", time.asctime(now)
