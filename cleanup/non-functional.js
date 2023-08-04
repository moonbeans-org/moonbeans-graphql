const pgp = require("pg-promise")({});
const cn = 'postgres://postgres:<DBPASS>@<DBHOST>:5432/moonbeanstwochainz';
const db = pgp(cn);
const fs = require("fs").promises;
const Web3 = require("web3");
const { program, Option } = require('commander');
const { CHAINS, CHAIN_LIST } = require("../chainIndexers/utils/chains.js");

/*****************
    CHAIN SETUP
******************/

program
  .version('1.0.0', '-v, --version')
  .addOption(new Option('-c, --chain <value>', 'chain name ~ should be present in chains.js').choices(CHAIN_LIST))
  .parse();

const options = program.opts();
const CHAIN_NAME = options.chain;
let chainObject = CHAINS[CHAIN_NAME];

console.log("Scanning for invalid asks on " + CHAIN_NAME);

/*****************
    WEB3 SETUP
******************/

// Get our web3 provider setup
const provider = new Web3(new Web3.providers.WebsocketProvider(chainObject.rpc, {
    clientConfig: {
        maxReceivedFrameSize: 100000000,
        maxReceivedMessageSize: 100000000,
        keepalive: true,
        keepaliveInterval: -1
    },

    reconnect: {
        auto: true,
        delay: 50000000,
        maxAttempts: 5,
        onTimeout: true
    },

    timeout: 30000000
}));


async function main() {
    let mnbeansAsks = [];
    const collections = JSON.parse(await fs.readFile('./collections.json'));
    let collectionAddresses = {};
    let collectionChains = {};

    for (let [key, val] of Object.entries(collections)) {
        collectionAddresses[val['contractAddress']] = val['title'];
        collectionChains[val['contractAddress']] = val['chain'];
    }

    let oldAsks = [];
    mnbeansAsks = await db.manyOrNone('SELECT "tokenId", "collectionId", "timestamp", "tokenNumber", "transactionHash" FROM "asks"');
    console.log("Listed tokens: ", mnbeansAsks.length)
    if (mnbeansAsks.length > 0) {
        console.log("Processing token list..");
        try {
            while (mnbeansAsks.length > 0) {
                ask = mnbeansAsks[0];
                if (!(ask['collectionId'] in collectionAddresses)) {
                    mnbeansAsks.shift();
                    continue;
                }

                let holder = await db.oneOrNone('SELECT "currentOwner", "lastTransfer" FROM "holders" WHERE "id" = $1', [`${ask['tokenId']}-${ask['lister']}`]);
                if (holder === null) {
                    console.log("Skipping", ask['tokenId']);
                    mnbeansAsks.shift();
                    continue;
                }

                if (ask['timestamp'] < holder['lastTransfer']) {
                    let tx;
                    if (collectionChains[ask['collectionId']]  === 'moonriver') {
                        tx = await web3movr.eth.getTransaction(ask['transactionHash']);
                    } else if (collectionChains[ask['collectionId']] === 'moonbeam') {
                        tx = await web3glmr.eth.getTransaction(ask['transactionHash']);
                    } else if (collectionChains[ask['collectionId']] === 'arbitrumnova') {
                        // tx = await web3nova.eth.getTransaction(ask['transactionHash']);
                        console.log('new chain who dis');
                    }

                    if (tx['from'] != holder['currentOwner']) {
                        oldAsks.push(Object.assign({}, ask, holder));
                        // if (ask['listingHash'] !== "OLD_CONTRACT") {
                        //     //TODO: get relayer to delete listing from contract
                        // }
                    }
                }
                mnbeansAsks.shift();
            }
        } catch (e) {
            console.log(e);
            console.log("Retrying...");
            await sleep(120000);
        }
    }

    console.log("Token Transfers after their listing: ", oldAsks.length);

    let oldContracts = {};
    let tokenIds = [];
    for (let oldAsk of oldAsks) {
        if (!(oldAsk['collectionId'] in oldContracts)) {
            oldContracts[oldAsk['collectionId']] = {
                title: collectionAddresses[oldAsk['collectionId']],
                count: 0,
                entities: []
            }
        }

        oldContracts[oldAsk['collectionId']]['count']++;
        oldContracts[oldAsk['collectionId']]['entities'].push(oldAsk['tokenNumber']);
        tokenIds.push(oldAsk['tokenId']);
    }

    console.log(oldContracts);

    if (tokenIds.length) {
        console.log(`DELETE FROM "asks" WHERE "tokenId" IN ('${tokenIds.join("','")}');`);
        console.log(`UPDATE "tokens" SET "currentAsk" = 0 WHERE "id" IN ('${tokenIds.join("','")}');`);
        for (let collectionId in oldContracts) {
            console.log(`UPDATE "collections" SET "ceilingPrice" = (SELECT MAX("value") FROM "asks" WHERE "collectionId" = '${collectionId}'), "floorPrice" = (SELECT MIN("value") FROM "asks" WHERE "collectionId" = '${collectionId}') WHERE "id" = '${collectionId}';`);
        }
    }
}

function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

main().then((e => {
    console.log('Done');
    process.exit()
}))