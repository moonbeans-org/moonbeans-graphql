require('dotenv').config()
const pgp = require("pg-promise")({});
const cn = `postgres://${process.env.DBUSER}:${process.env.DBPASS}@${process.env.DBHOST}:5432/${process.env.DBNAME}`;
const db = pgp(cn);
const fs = require("fs").promises;
const Web3 = require("web3");
const fetch = require('node-fetch');
const { program, Option } = require('commander');
const { CHAINS, CHAIN_LIST } = require("./utils/chains.js");
const ABIS = require("./utils/abis.js");

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

    await fetch("https://api.moonbeans.io/collection", {
        "method": "GET"
        }).then(res => {return res.json()}).then(async collections => {

            let collectionAddresses = {};
            let collectionChains = {};
        
            for (let collection of collections) {
                collectionAddresses[collection['contractAddress']] = collection['title'];
                collectionChains[collection['contractAddress']] = collection['chain']; 
            }

            let oldAsks = [];
            mnbeansAsks = await db.manyOrNone('SELECT "tokenId", "collectionId", "timestamp", "tokenNumber", "transactionHash", "lister" FROM "asks"');
            console.log("Listed tokens: ", mnbeansAsks.length)
            if (mnbeansAsks.length > 0) {
                console.log("Processing token list..");
                try {
                    while (mnbeansAsks.length > 0) {
                        ask = mnbeansAsks[0];
                        if (!(ask['collectionId'] in collectionAddresses) || collectionChains[ask['collectionId']]  !== CHAIN_NAME) {
                            console.log("Skipping", ask['tokenId']);
                            mnbeansAsks.shift();
                            continue;
                        }
        
                        let holder = await db.oneOrNone('SELECT "currentOwner", "lastTransfer", "balance" FROM "holders" WHERE "id" = $1', [`${ask['tokenId']}-${ask['lister']}`]);
                        if (holder === null) {
                            console.log("Skipping", ask['tokenId']);
                            mnbeansAsks.shift();
                            continue;
                        }
        
                        if (ask['timestamp'] < holder['lastTransfer']) {
                            let tx;
                            tx = await provider.eth.getTransaction(ask['transactionHash']);
        
                            if (tx['from'] !== holder['currentOwner'] || holder['balance'] === "0" ) {
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

            //Handle 1155s
            let deadTrades = [];
            mnbeansFungibleListings = await db.manyOrNone(`SELECT "contractAddress", "tokenNumber", "lastUpdatedTimestamp", "status", "remainingQuantity", "transactionHash", "tradeType", "tradeHash" FROM "fungibleTrades" WHERE "tradeType" = $1 AND "status" IN ('OPEN','PARTIAL')`, ["SELL"]);
            console.log("Fungible Listings: ", mnbeansFungibleListings.length)
            if (mnbeansFungibleListings.length > 0) {
                console.log("Processing token list..");
                try {
                    const fungibleMarketPlaceContract = new provider.eth.Contract(ABIS.FUNGIBLE_MARKET, chainObject?.fungible_marketplace_contract_address);
                    while (mnbeansFungibleListings.length > 0) {
                        ask = mnbeansFungibleListings[0];
                        if (!(ask['contractAddress'] in collectionAddresses) || collectionChains[ask['contractAddress']] !== CHAIN_NAME) {
                            console.log("Skipping", `${ask['contractAddress']}-${ask['tokenNumber']}`);
                            mnbeansFungibleListings.shift();
                            continue;
                        }
        
                        // let holder = await db.oneOrNone('SELECT "currentOwner", "lastTransfer", "balance" FROM "holders" WHERE "id" = $1', [`${ask['contractAddress']}-${ask['tokenNumber']}-${ask['lister']}`]);
                        // if (holder === null) {
                        //     console.log("Skipping", `${ask['contractAddress']}-${ask['tokenNumber']} @ ${ask['lastUpdatedTimestamp']}. tradeHash: ${ask['tradeHash']}. transactionHash: ${ask['transactionHash']}`);
                        //     mnbeansFungibleListings.shift();
                        //     continue;
                        // }
        
                        // if ((ask['remainingQuantity'] < holder['balance']) || holder['balance' === "0"]) {
                        //     deadTrades.push(Object.assign({}, ask, holder));
                        //     //TODO: get relayer to delete listing from contract
                        // } else {
                        const isValidTrade = await fungibleMarketPlaceContract?.methods.isValidTrade(ask['tradeHash']).call();
                        const tradeData = await fungibleMarketPlaceContract?.methods.getTrade(ask['tradeHash']).call();

                        if (!isValidTrade || tradeData?.['price'] === "0") {
                            console.log(`found invalid trade ${ask['tradeHash']}`);
                            deadTrades.push(ask);
                        }
                        // }
                        mnbeansFungibleListings.shift();
                    }
                } catch (e) {
                    console.log(e);
                    console.log("Retrying...");
                    await sleep(120000);
                }
            }
        
            console.log("Invalid 1155 listings: ", deadTrades.length);

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
        
            let oldFungibleContracts = {};
            let tradeHashes = [];
            deadTrades.forEach((trade) => {
                tradeHashes.push(trade['tradeHash']);
                if (!(trade['contractAddress'] in oldFungibleContracts)) {
                    oldFungibleContracts[trade['contractAddress']] = {
                        title: collectionAddresses[trade['contractAddress']],
                        count: 0,
                        entities: new Set()
                    }
                }
        
                oldFungibleContracts[trade['contractAddress']]['count']++;
                oldFungibleContracts[trade['contractAddress']]['entities'].add(trade['tokenNumber']);
            })

            console.log(oldContracts, oldFungibleContracts);
        
            if (tokenIds.length) {
                let date_ob = new Date();
                let today = date_ob.getFullYear() + '-' + ("0" + (date_ob.getMonth() + 1)).slice(-2) + '-' + ("0" + date_ob.getDate()).slice(-2) + "-" + date_ob.getHours() + "-" + date_ob.getMinutes() + "-" + date_ob.getSeconds();
                await fs.writeFile(`./deletions/${today}.sql`, `DELETE FROM "asks" WHERE "tokenId" IN ('${tokenIds.join("','")}');`, { flag: 'a+' }, err => {});
                await fs.writeFile(`./deletions/${today}.sql`, `UPDATE "tokens" SET "currentAsk" = 0 WHERE "id" IN ('${tokenIds.join("','")}');`, { flag: 'a+' }, err => {});
        
                for (let collectionId in oldContracts) {
                    await fs.writeFile(`./deletions/${today}.sql`, `UPDATE "collections" SET "ceilingPrice" = (SELECT MAX("value") FROM "asks" WHERE "collectionId" = '${collectionId}'), "floorPrice" = (SELECT MIN("value") FROM "asks" WHERE "collectionId" = '${collectionId}') WHERE "id" = '${collectionId}';`, { flag: 'a+' }, err => {});
                }
            } 

            if (deadTrades.length) {
                let date_ob = new Date();
                let today = date_ob.getFullYear() + '-' + ("0" + (date_ob.getMonth() + 1)).slice(-2) + '-' + ("0" + date_ob.getDate()).slice(-2) + "-" + date_ob.getHours() + "-" + date_ob.getMinutes() + "-" + date_ob.getSeconds();
                await fs.writeFile(`./deletions/${today}.sql`, `DELETE FROM "fungibleTrades" WHERE "tradeHash" IN ('${tradeHashes.join("','")}');`, { flag: 'a+' }, err => {});

                for (let CA in oldFungibleContracts) {
                    // UPDATE COLLECTION FLOOR/CEILINGS
                    const [floorPrice, ceilingPrice] = await getFungibleCollectionPrices(CA, tradeHashes);
                    await fs.writeFile(`./deletions/${today}.sql`, `UPDATE "collections" SET "floorPrice" = ${floorPrice}, "ceilingPrice" = ${ceilingPrice} WHERE "id" = '${CA}';`, { flag: 'a+' }, err => {});

                    oldFungibleContracts[CA]?.entities?.forEach(async (tokenNumber) => {
                        // UPDATE TOKEN FLOOR/CEILINGS
                        const [highestBid, lowestAsk] = await getFungibleTokenPrices(CA, tokenNumber, tradeHashes);
                        await fs.writeFile(`./deletions/${today}.sql`, `UPDATE "tokens" SET "currentAsk" = ${lowestAsk}, "highestBid" = ${highestBid} WHERE "id" = '${CA}-${tokenNumber}';`, { flag: 'a+' }, err => {});
                    })
                }
            }
            
        });
}

function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}


async function getFungibleTokenPrices(CA, tokenId, tradeHashesToIgnore = []) {
    let highestBid = null;
    let lowestAsk = null;

    let trades = await db.manyOrNone(`SELECT * FROM "fungibleTrades" WHERE "contractAddress" = $1 AND "tokenNumber" = $2 AND ("status" = 'OPEN' OR "status" = 'PARTIAL') AND "tradeHash" NOT IN ('${tradeHashesToIgnore.join("','")}')`, [CA, tokenId]);
    if (trades.length > 0) {
        for (let trade of trades) {
            if (trade['tradeType'] === "BUY" && (highestBid === null || (provider.utils.toBN(trade['pricePerUnit'])).gte(provider.utils.toBN(highestBid)))) {
                highestBid = provider.utils.toBN(trade['pricePerUnit']);
            }
            if (trade['tradeType'] === "SELL" && (lowestAsk === null || (provider.utils.toBN(trade['pricePerUnit'])).lte(provider.utils.toBN(lowestAsk)))) {
                lowestAsk = provider.utils.toBN(trade['pricePerUnit']);
            }
        }
    }

    if (highestBid === null) {
        highestBid = provider.utils.toBN(0);
    }
    if (lowestAsk === null) {
        lowestAsk = provider.utils.toBN(0);
    }

    return [highestBid.toString(), lowestAsk.toString()];
}

async function getFungibleCollectionPrices(collectionId, tradeHashesToIgnore = []) {
    let floorPrice = null;
    let ceilingPrice = null;

    let trades = await db.manyOrNone(`SELECT * FROM "fungibleTrades" WHERE "contractAddress" = $1 AND ("status" = 'OPEN' OR "status" = 'PARTIAL') AND "tradeType" = 'SELL' AND "tradeHash" NOT IN ('${tradeHashesToIgnore.join("','")}')`, [collectionId]);
    if (trades.length > 0) {
        for (let trade of trades) {
            if (floorPrice === null || (provider.utils.toBN(trade['pricePerUnit'])).lte(provider.utils.toBN(floorPrice))) {
                floorPrice = provider.utils.toBN(trade['pricePerUnit']);
            }
            if (ceilingPrice === null || (provider.utils.toBN(trade['pricePerUnit'])).gte(provider.utils.toBN(ceilingPrice))) {
                ceilingPrice = provider.utils.toBN(trade['pricePerUnit']);
            }
        }
    }

    if (floorPrice === null) {
        floorPrice = provider.utils.toBN(0);
    }
    if (ceilingPrice === null) {
        ceilingPrice = provider.utils.toBN(0);
    }

    return [floorPrice.toString(), ceilingPrice.toString()];
}

main().then((e => {
    console.log('Done'); 
    process.exit()
}))
