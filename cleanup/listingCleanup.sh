node sql-generator.js
deletionDir=`ls -t deletions`
if [ -z "$deletionDir" ]
then
	printf "NOTHING DELETED %s.\n" $(date +%s) >> deletions.txt
else
	latestFile=`ls -t deletions/* | head -1`
	PGPASSWORD=xxxxx psql -h xxxxx -U postgres -d moonbeanstwochainz -a -f $latestFile
	mv $latestFile old-$latestFile
	printf "RAN DELETION SCRIPT %s.\n" $latestFile >> deletions.txt
fi
