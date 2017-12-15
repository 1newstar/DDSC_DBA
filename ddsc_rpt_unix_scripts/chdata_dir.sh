#!/usr/bin/ksh
file=`ls *.sql`
for i in $file
do
#echo $i
awk '{sub("SCRIPT_HOME",PWD,$0);print $0 }' PWD=$PWD  $i >$i.tmp
cp $i.tmp $i
rm -rf $i.tmp
done
