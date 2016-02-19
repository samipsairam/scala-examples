#!/bin/bash


tempcmd="tmp$RANDOM.cql"

for i in {1..2}
do
  echo "consistency ANY;\n" > $tempcmd

  for j in {1..3} ; do
    echo "INSERT INTO ks1.t2 (uid, date) VALUES ('sss$RANDOM', 'ddd$RANDOM');" >> $tempcmd
  done

  #echo "$mycql"
  cqlsh -f $tempcmd 192.168.99.100
done
