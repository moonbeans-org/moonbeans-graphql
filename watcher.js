require('dotenv').config()
const ConnectionFilterPlugin = require("postgraphile-plugin-connection-filter");
const fs = require("fs").promises;
const http = require("http");
const { postgraphile } = require("postgraphile");
const pgp = require("pg-promise")({});
console.log('setting up db connection...');
const cn = `postgres://${process.env.DBUSER}:${process.env.DBPASS}@${process.env.DBHOST}:5432/${process.env.DBNAME}`;
console.log(cn);
const db = pgp(cn);
console.log('db connection established...');
console.log('port env variable: ' + process.env.PORT);

console.log('try querying it just for fucks sake.');
db.any('SELECT 1')
  .then(() => {
    console.log('Successfully connected to the database.');
  })
  .catch((error) => {
    console.error('Failed to connect to the database:', error);
  });

//Health Check & Convenience Endpoints
http.createServer(async function (req, res) {
    console.log('Checking health...');
    res.writeHead(200, {'Content-Type': 'application/json'});

    let responseData = {};

    let lastBlocksQuery = await db.manyOrNone('SELECT name, value, timestamp FROM "meta"');
    if (lastBlocksQuery.length > 0) {
        for (let row of lastBlocksQuery) {
            responseData[row['name']] = row['value'];
        }
    }

    responseData['last_block_movr'] = parseInt(await fs.readFile("./chainIndexers/last_block_moonriver.txt"));
    responseData['last_block_glmr'] = parseInt(await fs.readFile("./chainIndexers/last_block_moonbeam.txt"));

    res.write(JSON.stringify(responseData));
    res.end();
    console.log('Health check completed!');
}).listen(8080);




//GraphQL server
http.createServer(
    postgraphile(
        cn,
        "public",
        {
            watchPg: true,
            graphiql: true,
            enhanceGraphiql: true,
            appendPlugins: [ConnectionFilterPlugin],
            enableCors: true,
            disableDefaultMutations: true,
        }
    )
).listen(process.env.PORT || 3000);