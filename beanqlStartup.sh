#!/bin/bash
yum update -y
yum install git -y
git clone https://github.com/m00nbeans/moonbeans-graphql.git
cd moonbeans-graphql
curl -sL https://rpm.nodesource.com/setup_16.x | bash -
yum install -y nodejs
amazon-linux-extras enable postgresql10
yum install -y postgresql
sed -i 's/<DBPASS>/xxxxx/' moonriver.js
sed -i 's/<DBHOST>/xxxxx/' moonriver.js
sed -i 's/<DBPASS>/xxxxx/' moonbeam.js
sed -i 's/<DBHOST>/xxxxx/' moonbeam.js
npm i
npm install pm2
pm2 install pm2-logrotate
pm2 set pm2-logrotate:max_size 50M
pm2 set pm2-logrotate:retain 10
pm2 set pm2-logrotate:compress true
pm2 start moonriver.js
pm2 start moonbeam.js
echo DONE