require('dotenv').config()
const ConnectionFilterPlugin = require("postgraphile-plugin-connection-filter");
const fs = require("fs").promises;
const http = require("http");
const { postgraphile } = require("postgraphile");
const pgp = require("pg-promise")({});
const cn = `postgres://${process.env.DBUSER}:${process.env.DBPASS}@${process.env.DBHOST}:5432/${process.env.DBNAME}`;
const db = pgp(cn);

//Health Check & Convenience Endpoints
http.createServer(async function (req, res) {
    console.log('Checking health...');
    try {
        res.writeHead(200, {'Content-Type': 'application/json'});

        let responseData = {};
    
        let lastBlocksChainQuery = await db.manyOrNone('SELECT name, "lastIndexedBlock" FROM "chains"');
        if (lastBlocksChainQuery.length > 0) {
            for (let row of lastBlocksChainQuery) {
                responseData[`last_block_${row['name']}`] = row['lastIndexedBlock'];
            }
        }
        console.log('Last Indexed Blocks:', responseData);
    
        let lastBlocksQuery = await db.manyOrNone('SELECT name, value, timestamp FROM "meta"');
        if (lastBlocksQuery.length > 0) {
            for (let row of lastBlocksQuery) {
                responseData[row['name']] = row['value'];
            }
        }
    
        res.write(JSON.stringify(responseData));
        res.end();
    } catch (e){
        console.log(e);
    }

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
