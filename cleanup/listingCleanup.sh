#!/bin/bash
source .env

for CHAIN in moonriver moonbeam
do
	node sql-generator.js --chain $CHAIN
	if [ ! -d "deletions" ]; then
	mkdir "deletions"
	fi
	deletionDir=`ls -t deletions`
	if [ -z "$deletionDir" ]
	then
		printf "NOTHING DELETED %s.\n" $(date +%s) >> deletions.txt
	else
		printf "ATTEMPTING TO DELETE %s.\n" $(date +%s)
		latestFile=`ls -t deletions/* | head -1`
		PGPASSWORD=$DBPASS psql -h $DBHOST -U $DBUSER -d $DBNAME -a -f $latestFile
		mv $latestFile old-$latestFile
		printf "RAN DELETION SCRIPT %s.\n" $latestFile >> deletions.txt
	fi
done
