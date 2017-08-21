#!/bin/bash
#
#script to update the metasys data in graphite
#
#copy script runs on metasys windows pc daily at 7:10 am
#from synctoy via task scheduler
# this takes maybe 15 minutes
#
#data is copied into /local.1/metasys
# 
# * sync the data 
# * archive the data?

/home/timc/metasys2graphite/metasys2graphite.pl
/home/timc/metasys2graphite/metasys2graphite-harvey.pl

#remove some bizzare directories in the script I guess

#rotate to archive out

/bin/mail -s "metasys2graphite done" tczerwonka@gmail.com < /bin/date

exit 0
