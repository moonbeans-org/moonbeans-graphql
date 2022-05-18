const ConnectionFilterPlugin = require("postgraphile-plugin-connection-filter");
const fs = require("fs").promises;
const http = require("http");
const { postgraphile } = require("postgraphile");
const pgp = require("pg-promise")({});
const cn = 'postgres://postgres:<DBPASS>@<DBHOST>:5432/moonbeanstwochainz';
const db = pgp(cn);

http.createServer(async function (req, res) {
    res.writeHead(200, {'Content-Type': 'application/json'});

    let responseData = {};
    
    let lastBlocksQuery = await db.manyOrNone('SELECT name, value, timestamp FROM "meta"');
    if (lastBlocksQuery.length > 0) {
        for (let row of lastBlocksQuery) {
            responseData[row['name']] = row['value'];
        }
    }
    
    res.write(JSON.stringify(responseData));
    res.end();
}).listen(8080);

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