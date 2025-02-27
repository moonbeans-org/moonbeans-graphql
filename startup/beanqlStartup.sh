#!/bin/bash
yum update -y
yum install git -y
git clone https://github.com/m00nbeans/moonbeans-graphql.git
cd moonbeans-graphql
curl -sL https://rpm.nodesource.com/setup_16.x | bash -
yum install -y nodejs
amazon-linux-extras enable postgresql10
yum install -y postgresql
sed -i 's/<DBPASS>/xxxxx/' ../chainIndexers/moonriver.js
sed -i 's/<DBHOST>/xxxxx/' ../chainIndexers/moonriver.js
sed -i 's/<DBPASS>/xxxxx/' ../chainIndexers/moonbeam.js
sed -i 's/<DBHOST>/xxxxx/' ../chainIndexers/moonbeam.js
npm i
npm install pm2
pm2 install pm2-logrotate
pm2 set pm2-logrotate:max_size 50M
pm2 set pm2-logrotate:retain 10
pm2 set pm2-logrotate:compress true
pm2 start moonriver.js
pm2 start moonbeam.js
mkdir deletions
mkdir old-deletions
touch deletions.txt
(crontab -l 2>/dev/null; echo "0 * * * * /home/ec2-user/moonbeans-graphql/cleanup/listingCleanup.sh") | crontab -
echo DONE