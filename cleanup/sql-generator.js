require('dotenv').config()
const pgp = require("pg-promise")({});
const cn = `postgres://${process.env.DBUSER}:${process.env.DBPASS}@${process.env.DBHOST}:5432/${process.env.DBNAME}`;
const db = pgp(cn);
const fs = require("fs").promises;
const Web3 = require("web3");
const fetch = require('node-fetch');

const providerMOVR = new Web3.providers.WebsocketProvider("wss://moonriver.api.onfinality.io/public-ws", {
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
    })

const providerGLMR = new Web3.providers.WebsocketProvider("wss://moonbeam.api.onfinality.io/public-ws", {
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
    })

// const providerNOVA = new Web3.providers.WebsocketProvider("wss://bold-greatest-frog.nova-mainnet.discover.quiknode.pro/06c3e46fa798872b6c473beb9c53ef101695fbb7/", {
//     clientConfig: {
//         maxReceivedFrameSize: 100000000,
//         maxReceivedMessageSize: 100000000,
//         keepalive: true,
//         keepaliveInterval: -1
//     },

//     reconnect: {
//         auto: true,
//         delay: 50000000,
//         maxAttempts: 5,
//         onTimeout: true
//     },

//     timeout: 30000000
// })

const web3movr = new Web3(providerMOVR);
const web3glmr = new Web3(providerGLMR);
// const web3nova = new Web3(providerNOVA);

async function main() {
    let mnbeansAsks = [];

    await fetch("https://api.moonbeans.io/collection", {
        "method": "GET"
        }).then(res => {return res.json()}).then(async collections => {

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
        
                        let holder = await db.oneOrNone('SELECT "currentOwner", "lastTransfer" FROM "holders" WHERE "id" = $1', [ask['tokenId']]);
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
                let date_ob = new Date();
                let today = date_ob.getFullYear() + '-' + ("0" + (date_ob.getMonth() + 1)).slice(-2) + '-' + ("0" + date_ob.getDate()).slice(-2) + "-" + date_ob.getHours() + "-" + date_ob.getMinutes() + "-" + date_ob.getSeconds();
                await fs.writeFile(`./deletions/${today}.sql`, `DELETE FROM "asks" WHERE "tokenId" IN ('${tokenIds.join("','")}');`, { flag: 'a+' }, err => {});
                await fs.writeFile(`./deletions/${today}.sql`, `UPDATE "tokens" SET "currentAsk" = 0 WHERE "id" IN ('${tokenIds.join("','")}');`, { flag: 'a+' }, err => {});
        
                for (let collectionId in oldContracts) {
                    await fs.writeFile(`./deletions/${today}.sql`, `UPDATE "collections" SET "ceilingPrice" = (SELECT MAX("value") FROM "asks" WHERE "collectionId" = '${collectionId}'), "floorPrice" = (SELECT MIN("value") FROM "asks" WHERE "collectionId" = '${collectionId}') WHERE "id" = '${collectionId}';`, { flag: 'a+' }, err => {});
                }
            } 
            
        });
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
