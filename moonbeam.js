const ConnectionFilterPlugin = require("postgraphile-plugin-connection-filter");
const Web3 = require("web3");
const fs = require("fs").promises;
const http = require("http");
const { postgraphile } = require("postgraphile");
const pgp = require("pg-promise")({});
const cn = 'postgres://postgres:<DBPASS>@<DBHOST>:5432/moonbeanstwochainz';
const db = pgp(cn);

const marketPlaceAbi = [{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token","type":"address"},{"indexed":true,"internalType":"uint256","name":"id","type":"uint256"},{"indexed":true,"internalType":"uint256","name":"price","type":"uint256"},{"indexed":false,"internalType":"address","name":"buyer","type":"address"},{"indexed":false,"internalType":"bool","name":"escrowed","type":"bool"},{"indexed":false,"internalType":"uint256","name":"timestamp","type":"uint256"}],"name":"BidCancelled","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token","type":"address"},{"indexed":true,"internalType":"uint256","name":"id","type":"uint256"},{"indexed":true,"internalType":"uint256","name":"price","type":"uint256"},{"indexed":false,"internalType":"address","name":"buyer","type":"address"},{"indexed":false,"internalType":"uint256","name":"timestamp","type":"uint256"},{"indexed":false,"internalType":"bool","name":"escrowed","type":"bool"}],"name":"BidPlaced","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"user","type":"address"},{"indexed":true,"internalType":"uint256","name":"price","type":"uint256"}],"name":"EscrowReturned","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token","type":"address"},{"indexed":true,"internalType":"uint256","name":"id","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"timestamp","type":"uint256"}],"name":"TokenDelisted","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token","type":"address"},{"indexed":true,"internalType":"uint256","name":"id","type":"uint256"},{"indexed":true,"internalType":"uint256","name":"price","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"timestamp","type":"uint256"}],"name":"TokenListed","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"oldOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"},{"indexed":true,"internalType":"uint256","name":"price","type":"uint256"},{"indexed":false,"internalType":"address","name":"collection","type":"address"},{"indexed":false,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"TokenPurchased","type":"event"},{"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"RecoverMOVR","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"TOKEN","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"ca","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"uint256","name":"price","type":"uint256"},{"internalType":"address","name":"from","type":"address"},{"internalType":"bool","name":"escrowedBid","type":"bool"}],"name":"acceptOffer","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"addMoneyToEscrow","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"beanBuybackAddress","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"beanBuybackFee","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"beanieHolderAddress","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"beanieHolderFee","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"ca","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"uint256","name":"price","type":"uint256"},{"internalType":"bool","name":"escrowed","type":"bool"}],"name":"cancelOffer","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"checkEscrowAmount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"ca","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"clearAllBids","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"ca","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"clearAllListings","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"clearBidsAfterAcceptingOffer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"clearBidsAfterFulfillingListing","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"collectionOwnersCanSetRoyalties","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"defaultCollectionOwnerFee","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"delistAfterAcceptingOffer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"ca","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"delistToken","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"devAddress","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"devFee","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feesOn","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"ca","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"fulfillListing","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"ca","type":"address"}],"name":"getCollectionFee","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"ca","type":"address"}],"name":"getCollectionOwner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"ca","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"getCurrentListing","outputs":[{"components":[{"internalType":"uint256","name":"price","type":"uint256"},{"internalType":"uint256","name":"timestamp","type":"uint256"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"bool","name":"accepted","type":"bool"}],"internalType":"struct MarketPlace.Listing","name":"","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"ca","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"getCurrentListingPrice","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"getEscrowedAmount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"ca","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"getOffers","outputs":[{"components":[{"internalType":"uint256","name":"price","type":"uint256"},{"internalType":"uint256","name":"timestamp","type":"uint256"},{"internalType":"bool","name":"accepted","type":"bool"},{"internalType":"address","name":"buyer","type":"address"},{"internalType":"bool","name":"escrowed","type":"bool"}],"internalType":"struct MarketPlace.Offer[]","name":"","type":"tuple[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"ca","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"getTokenListingHistory","outputs":[{"components":[{"internalType":"uint256","name":"price","type":"uint256"},{"internalType":"uint256","name":"timestamp","type":"uint256"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"bool","name":"accepted","type":"bool"}],"internalType":"struct MarketPlace.Listing[]","name":"","type":"tuple[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"ca","type":"address"}],"name":"isCollectionTrading","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"ca","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"isListed","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"ca","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"uint256","name":"price","type":"uint256"}],"name":"listToken","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"ca","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"uint256","name":"price","type":"uint256"}],"name":"makeEscrowedOffer","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"ca","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"uint256","name":"price","type":"uint256"}],"name":"makeOffer","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"bytes","name":"","type":"bytes"}],"name":"onERC721Received","outputs":[{"internalType":"bytes4","name":"","type":"bytes4"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"recoverNFT","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"recoverToken","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"admin","type":"address"},{"internalType":"bool","name":"value","type":"bool"}],"name":"setAdmin","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"fee","type":"uint256"}],"name":"setBeanBuyBackFee","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_address","type":"address"}],"name":"setBeanBuybackAddress","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_address","type":"address"}],"name":"setBeanieHolderAddress","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"fee","type":"uint256"}],"name":"setBeanieHolderFee","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"_value","type":"bool"}],"name":"setClearBidsAfterAcceptingOffer","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"_value","type":"bool"}],"name":"setClearBidsAfterFulfillingListing","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"ca","type":"address"},{"internalType":"address","name":"owner","type":"address"}],"name":"setCollectionOwner","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"ca","type":"address"},{"internalType":"uint256","name":"fee","type":"uint256"}],"name":"setCollectionOwnerFee","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"_value","type":"bool"}],"name":"setCollectionOwnersCanSetRoyalties","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"ca","type":"address"},{"internalType":"bool","name":"value","type":"bool"}],"name":"setCollectionTrading","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"fee","type":"uint256"}],"name":"setDefaultCollectionOwnerFee","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"_value","type":"bool"}],"name":"setDelistAfterAcceptingOffer","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_address","type":"address"}],"name":"setDevAddress","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"fee","type":"uint256"}],"name":"setDevFee","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"_value","type":"bool"}],"name":"setFeesOn","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"}],"name":"setPaymentToken","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"gasAmount","type":"uint256"}],"name":"setSpecialGasTax","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"value","type":"bool"}],"name":"setSuperGasTaxes","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"value","type":"bool"}],"name":"setTrading","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"specialTaxGas","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalEscrowedAmount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalFees","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"tradingPaused","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"useSuperGasTaxes","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"withdrawMoneyFromEscrow","outputs":[],"stateMutability":"nonpayable","type":"function"},{"stateMutability":"payable","type":"receive"}];

const blockBatch = 200;

let methodSignatures = [];
let collections = [];
let blockTimestamps = {};

const web3 = new Web3(new Web3.providers.WebsocketProvider("wss://moonbeam.blastapi.io/b8f3e3d8-3ae3-4e8f-9879-29d4730dd73d", {
    clientConfig: {
        maxReceivedFrameSize: 100000000,
        maxReceivedMessageSize: 100000000,
        keepalive: true,
        keepaliveInterval: 60000
    },

    reconnect: {
        auto: true,
        delay: 50000,
        maxAttempts: 5,
        onTimeout: true
    },

    timeout: 30000
}));

marketPlaceAbi.map(function (abi) {
    if (abi.name) {
        const signature = web3.utils.sha3(
            abi.name +
            "(" +
            abi.inputs
                .map(_typeToString)
                .join(",") +
            ")"
        );
        if (abi.type !== "event") {
            methodSignatures[signature.slice(2, 10)] = abi.name;
        }
    }
});

const marketPlaceContract = new web3.eth.Contract(marketPlaceAbi, "0x683724817a7d526d6256Aec0D6f8ddF541b924de");

function _typeToString(input) {
    if (input.type === "tuple") {
        return "(" + input.components.map(_typeToString).join(",") + ")";
    }
    return input.type;
}

async function startListening() {
    startListeningMarketplace();
    //startListeningHolders();
}

async function startListeningMarketplace() {
    let startBlock = parseInt(await fs.readFile("last_block_glmr.txt"));

    if (startBlock == 0) {
        startBlock = 975368;
    }

    let lastBlock = await web3.eth.getBlockNumber();

    let endBlock = startBlock + blockBatch;
    if (endBlock > lastBlock) {
        endBlock = lastBlock;
    }
    
    handleMarketplaceLogs(startBlock, endBlock, lastBlock);
}

async function handleMarketplaceLogs(startBlock, endBlock, lastBlock) {
    try {
        while (true) {
            console.log(startBlock, endBlock, lastBlock);

            let events = await marketPlaceContract.getPastEvents("allEvents", { 'fromBlock': startBlock, 'toBlock': endBlock });

            let sortedEvents = events.reverse().sort(function (x, y) {
                return x.blockNumber - y.blockNumber || x.transactionIndex - y.transactionIndex;
            });

            for (let row of sortedEvents) {
                if (row.removed) {
                    continue;
                }

                row['transactionEventHash'] = row['transactionHash'] + "-" + row['transactionIndex'] + "-" + row['logIndex'];

                const txrow = await db.oneOrNone('SELECT * FROM "transactions" WHERE "id" = $1', [row['transactionEventHash']]);

                console.log(row['transactionEventHash']);
                if (txrow !== null) {
                    console.log("skipping");
                    continue;
                }

                let transactionHandled = true;

                if (row.event == "TokenListed") {
                    await handleTokenListed(row);
                    row['timestamp'] = row['returnValues']['timestamp'];
                } else if (row.event == "TokenDelisted") {
                    await handleTokenDelisted(row);
                    row['timestamp'] = row['returnValues']['timestamp'];
                } else if (row.event == "TokenPurchased") {
                    await handleTokenPurchased(row);
                } else if (row.event == "BidPlaced") {
                    await handledBidPlaced(row);
                    row['timestamp'] = row['returnValues']['timestamp'];
                } else if (row.event == "BidCancelled") {
                    await handleBidCancelled(row);
                    row['timestamp'] = row['returnValues']['timestamp'];
                } else {
                    transactionHandled = false;
                }

                if (transactionHandled) {
                    await handleTransaction(row);
                }
            }

            startBlock = endBlock;
            await fs.writeFile("./last_block_glmr.txt", "" + startBlock);
            if (startBlock >= lastBlock) {
                endBlock = await web3.eth.getBlockNumber();
                await sleep(120000);
            } else {
                endBlock += blockBatch;
                if (endBlock > lastBlock) {
                    endBlock = lastBlock;
                }
            }
        }
    } catch (e) {
        console.log(e);
        handleMarketplaceLogs(startBlock, endBlock, lastBlock);
    }
}

async function handleTokenListed(row) {
    const id = `${row['returnValues']['token']}-${row['returnValues']['id']}`;
    const price = web3.utils.toBN(row['returnValues']['price']);

    // CREATE OR GET COLLECTION
    let collection = await db.oneOrNone('SELECT * FROM "collections" WHERE "id" = $1', [row['returnValues']['token']]);
    if (collection === null) {
        await db.any('INSERT INTO "collections" ("id", "ceilingPrice", "floorPrice", "volumeOverall") VALUES ($1, $2, $3, $4)', [row['returnValues']['token'], 0, 0, 0]);
        collection = {
            'id': row['returnValues']['token'],
            'ceilingPrice': 0,
            'floorPrice': 0,
            'volumeOverall': 0
        };
    }

    // SAVE OR UPDATE TOKEN
    let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [id]);
    if (token === null) {
        await db.any('INSERT INTO "tokens" ("id", "tokenNumber", "collectionId", "currentAsk", "lowestBid", "heighestBid") VALUES ($1, $2, $3, $4, $5, $6)', [id, row['returnValues']['id'], row['returnValues']['token'], price.toString(), 0, 0]);
        token = {
            'id': id,
            'tokenNumber': row['returnValues']['id'],
            'collectionId': row['returnValues']['token'],
            'currentAsk': price,
            'lowestBid': 0,
            'heighestBid': 0
        };
    } else {
        await db.any('UPDATE "tokens" SET "currentAsk" = $1 WHERE "id" = $2', [price.toString(), id]);
        token['currentAsk'] = price;
    }

    if (web3.utils.toBN(collection['ceilingPrice']).lte(price)) {
        await db.any('UPDATE "collections" SET "ceilingPrice" = $1 WHERE "id" = $2', [price.toString(), row['returnValues']['token']]);
        collection['ceilingPrice'] = price;
    }

    if (web3.utils.toBN(collection['floorPrice']).gte(price)) {
        await db.any('UPDATE "collections" SET "floorPrice" = $1 WHERE "id" = $2', [price.toString(), row['returnValues']['token']]);
        collection['floorPrice'] = price;
    }

    // SAVE CURRENT ASK
    await db.any('DELETE FROM "asks" WHERE "id" = $1', [id]);
    await db.any('INSERT INTO "asks" ("id", "collectionId", "tokenNumber", "tokenId", "value", "timestamp", "transactionHash") VALUES ($1, $2, $3, $4, $5, $6, $7)',
        [id, row['returnValues']['token'], row['returnValues']['id'], id, price.toString(), row['returnValues']['timestamp'], row['transactionHash']]);

    // SAVE CURRENT ASK INTO HISTORY
    await db.any('INSERT INTO "askHistories" ("collectionId", "tokenNumber", "tokenId", "value", "timestamp", "accepted", "transactionHash") VALUES ($1, $2, $3, $4, $5, $6, $7)',
        [row['returnValues']['token'], row['returnValues']['id'], id, price.toString(), row['returnValues']['timestamp'], 0, row['transactionHash']]);

    console.log(`[TOKEN LISTED] tx: ${row['transactionHash']}; token: ${row['returnValues']['id']}; collection: ${row['returnValues']['token']}}; price: ${price}`);
}

async function handleTokenDelisted(row) {
    const id = `${row['returnValues']['token']}-${row['returnValues']['id']}`;

    // UDPDATE TOKEN
    let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [id]);
    if (token === null) {
        console.log("Token not in database");
    }

    await db.any('UPDATE "tokens" SET "currentAsk" = $1 WHERE "id" = $2', [0, id]);

    // REMOVE CURRENT ASK
    await db.any('DELETE FROM "asks" WHERE "id" = $1', [id]);

    // SAVE DELIST TO ASK HISTORY
    await db.any('INSERT INTO "askHistories" ("collectionId", "tokenNumber", "tokenId", "value", "timestamp", "accepted", "transactionHash") VALUES ($1, $2, $3, $4, $5, $6, $7)',
        [row['returnValues']['token'], row['returnValues']['id'], id, 0, row['returnValues']['timestamp'], 0, row['transactionHash']]);

    // UPDATE COLLECTION
    let collection = await db.oneOrNone('SELECT * FROM "collections" WHERE "id" = $1', [row['returnValues']['token']]);
    if (collection === null) {
        console.log("Collection not in database");
    }

    let [floorPrice, ceilingPrice] = await getCollectionPrices(row['returnValues']['token']);
    await db.any('UPDATE "collections" SET "floorPrice" = $1, "ceilingPrice" = $2 WHERE "id" = $3', [floorPrice, ceilingPrice, row['returnValues']['token']]);

    console.log(`[TOKEN DELISTED] tx: ${row['transactionHash']}; token: ${row['returnValues']['id']}; collection: ${row['returnValues']['token']};`);
}

async function handleTokenPurchased(row) {
    let block = await web3.eth.getBlock(row['blockNumber']);

    let tx = await web3.eth.getTransaction(row['transactionHash']);

    row['timestamp'] = block['timestamp'];

    if (methodSignatures[tx.input.slice(2, 10)] == "acceptOffer") {
        const id = `${row['returnValues']['collection']}-${row['returnValues']['tokenId']}`;
        const fillId = `${row['returnValues']['collection']}-${row['returnValues']['tokenId']}-${row['transactionHash']}`;

        // REMOVE CURRENT BID
        let bid = await db.oneOrNone('SELECT * FROM "bids" WHERE "tokenId" = $1 AND "buyer" = $2 AND "value" = $3 ORDER BY "timestamp" DESC LIMIT 1', [id, row['returnValues']['newOwner'], web3.utils.toBN(row['returnValues']['price']).toString()]);
        if (bid !== null) {
            // SAVE FILL
            await db.any('INSERT INTO "fills" ("id", "collectionId", "tokenNumber", "tokenId", "value", "timestamp", "buyer", "type") VALUES ($1, $2, $3, $4, $5, $6, $7, $8)', [fillId, row['returnValues']['collection'], row['returnValues']['tokenId'], id, web3.utils.toBN(row['returnValues']['price']).toString(), block['timestamp'], row['returnValues']['newOwner'], 'bid']);

            // REMOVE BID
            await db.any('DELETE FROM "bids" WHERE "id" = $1', [bid['id']]);

            // UPDATE TOKEN
            let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [id]);
            if (token === null) {
                console.log("Token not in database");
            }

            let [lowestBid, heighestBid] = await getTokenPrices(id);
            await db.any('UPDATE "tokens" SET "lowestBid" = $1, "heighestBid" = $2 WHERE "id" = $3', [lowestBid, heighestBid, id]);

            // UPDATE COLLECTION
            let collection = await db.oneOrNone('SELECT * FROM "collections" WHERE "id" = $1', [row['returnValues']['collection']]);
            if (collection === null) {
                console.log("Collection not in database");
            }

            await db.any('UPDATE "collections" SET "volumeOverall" = $1 WHERE "id" = $2', [((web3.utils.toBN(collection['volumeOverall'])).add(web3.utils.toBN(row['returnValues']['price']))).toString(), row['returnValues']['collection']]);

            console.log(`[FILL BID] tx: ${row['transactionHash']}; token: ${row['returnValues']['tokenId']}; collection: ${row['returnValues']['collection']}; from: ${row['returnValues']['newOwner']}; price: ${row['returnValues']['price']}`);
        } else {
            console.log("Bid not in database");
        }
    } else {
        const id = `${row['returnValues']['collection']}-${row['returnValues']['tokenId']}`;
        const fillId = `${row['returnValues']['collection']}-${row['returnValues']['tokenId']}-${row['transactionHash']}`;

        let filledAsk = await db.oneOrNone('SELECT * FROM "askHistories" WHERE "tokenId" = $1 AND "value" = $2 AND "accepted" = $3 ORDER BY "timestamp" DESC LIMIT 1', [id, web3.utils.toBN(row['returnValues']['price']).toString(), 0]);
        if (filledAsk !== null) {
            const askHistoryId = filledAsk['id'];

            // UDPDATE TOKEN
            let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [id]);
            if (token === null) {
                console.log("Token not in database");
            }

            await db.any('UPDATE "tokens" SET "currentAsk" = $1 WHERE "id" = $2', [0, id]);

            // SAVE FILL
            await db.any('INSERT INTO "fills" ("id", "collectionId", "tokenNumber", "tokenId", "value", "timestamp", "buyer", "type") VALUES ($1, $2, $3, $4, $5, $6, $7, $8)', [fillId, row['returnValues']['collection'], row['returnValues']['tokenId'], id, web3.utils.toBN(row['returnValues']['price']).toString(), block['timestamp'], row['returnValues']['newOwner'], 'ask']);

            // UPDATE OLD ASK HISTORY
            await db.any('UPDATE "askHistories" SET "accepted" = $1 WHERE "id" = $2', [1, askHistoryId]);

            /*// REMOVE CURRENT ASK
            await db.any('DELETE FROM "asks" WHERE "id" = $1', [id]);

            // SAVE DELIST TO ASK HISTORY
            await db.any('INSERT INTO "askHistories" ("collectionId", "tokenNumber", "tokenId", "value", "timestamp", "accepted", "transactionHash") VALUES ($1, $2, $3, $4, $5, $6, $7)',
            [row['returnValues']['collection'], row['returnValues']['tokenId'], id, 0, block['timestamp'], 0, row['transactionHash']]);
            */
            // UPDATE COLLECTION
            let collection = await db.oneOrNone('SELECT * FROM "collections" WHERE "id" = $1', [row['returnValues']['collection']]);
            if (collection === null) {
                console.log("Collection not in database");
            }

            //let [floorPrice, ceilingPrice] = await getCollectionPrices(row['returnValues']['token']);
            //await db.any('UPDATE "collections" SET "floorPrice" = $1, "ceilingPrice" = $2, "volumeOverall" = $3 WHERE "id" = $4', [floorPrice, ceilingPrice, ((web3.utils.toBN(collection['volumeOverall'])).add(web3.utils.toBN(row['returnValues']['price']))).toString(), row['returnValues']['collection']]);
            await db.any('UPDATE "collections" SET "volumeOverall" = $1 WHERE "id" = $2', [((web3.utils.toBN(collection['volumeOverall'])).add(web3.utils.toBN(row['returnValues']['price']))).toString(), row['returnValues']['collection']]);

            console.log(`[FILL ASK] tx: ${row['transactionHash']}; token: ${row['returnValues']['tokenId']}; collection: ${row['returnValues']['collection']}; price: ${row['returnValues']['price']}`)
        } else {
            console.log("Ask not in database");
        }
    }
}

async function handledBidPlaced(row) {
    const id = `${row['returnValues']['token']}-${row['returnValues']['id']}`;
    const bidId = `${row['returnValues']['token']}-${row['returnValues']['id']}-${row['returnValues']['buyer']}-${row['transactionHash']}`;
    const price = web3.utils.toBN(row['returnValues']['price']);

    // CREATE OR GET COLLECTION
    let collection = await db.oneOrNone('SELECT * FROM "collections" WHERE "id" = $1', [row['returnValues']['token']]);
    if (collection === null) {
        await db.any('INSERT INTO "collections" ("id", "ceilingPrice", "floorPrice", "volumeOverall") VALUES ($1, $2, $3, $4)', [row['returnValues']['token'], 0, 0, 0]);
        collection = {
            'id': row['returnValues']['token'],
            'ceilingPrice': 0,
            'floorPrice': 0,
            'volumeOverall': 0
        };
    }

    // SAVE OR UPDATE TOKEN
    let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [id]);
    if (token === null) {
        await db.any('INSERT INTO "tokens" ("id", "tokenNumber", "collectionId", "currentAsk", "lowestBid", "heighestBid") VALUES ($1, $2, $3, $4, $5, $6)', [id, row['returnValues']['id'], row['returnValues']['token'], price.toString(), 0, 0]);
        token = {
            'id': id,
            'tokenNumber': row['returnValues']['id'],
            'collectionId': row['returnValues']['token'],
            'currentAsk': 0,
            'lowestBid': price,
            'heighestBid': price
        };
    } else {
        if (web3.utils.toBN(token['lowestBid']).lte(price)) {
            await db.any('UPDATE "tokens" SET "lowestBid" = $1 WHERE "id" = $2', [price.toString(), id]);
            token['lowestBid'] = price;
        }

        if (web3.utils.toBN(token['heighestBid']).gte(price)) {
            await db.any('UPDATE "tokens" SET "heighestBid" = $1 WHERE "id" = $2', [price.toString(), id]);
            token['heighestBid'] = price;
        }
    }

    // SAVE BID
    await db.any('INSERT INTO "bids" ("id", "collectionId", "tokenNumber", "tokenId", "value", "timestamp", "buyer", "transactionHash") VALUES ($1, $2, $3, $4, $5, $6, $7, $8)',
        [bidId, row['returnValues']['token'], row['returnValues']['id'], id, price.toString(), row['returnValues']['timestamp'], row['returnValues']['buyer'], row['transactionHash']]);

    console.log(`[BID PLACED] tx: ${row['transactionHash']}; token: ${row['returnValues']['id']}; collection: ${row['returnValues']['token']}; price: ${price}}`)

}

async function handleBidCancelled(row) {
    const id = `${row['returnValues']['token']}-${row['returnValues']['id']}`;

    // REMOVE CURRENT BID
    let bid = await db.oneOrNone('SELECT * FROM "bids" WHERE "tokenId" = $1 AND "buyer" = $2 AND "value" = $3 ORDER BY "timestamp" ASC LIMIT 1', [id, row['returnValues']['buyer'], web3.utils.toBN(row['returnValues']['price']).toString()]);
    if (bid !== null) {
        await db.any('DELETE FROM "bids" WHERE "id" = $1', [bid['id']]);

        // UPDATE TOKEN
        let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [id]);
        if (token === null) {
            console.log("Token not in database");
        }

        let [lowestBid, heighestBid] = await getTokenPrices(id);
        await db.any('UPDATE "tokens" SET "lowestBid" = $1, "heighestBid" = $2 WHERE "id" = $3', [lowestBid, heighestBid, id]);

        console.log(`[BID CANCELLED] tx: ${row['transactionHash']}; token: ${row['returnValues']['id']}; collection: ${row['returnValues']['token']}; from: ${row['returnValues']['buyer']}; price: ${row['returnValues']['price']}`);
    } else {
        console.log("Bid not in database");
    }
}

async function handleTransaction(row) {
    await db.any('INSERT INTO "transactions" ("id", "blockNumber", "timestamp") VALUES ($1, $2, $3)',
        [row['transactionEventHash'], row['blockNumber'], row['timestamp']]);
}

async function getTokenPrices(id) {
    let floorPrice = null;
    let ceilingPrice = null;

    let bids = await db.manyOrNone('SELECT * FROM "bids" WHERE "tokenId" = $1', [id]);
    if (bids.length > 0) {
        for (let bid of bids) {
            if (floorPrice === null || (web3.utils.toBN(bid['value'])).lte(web3.utils.toBN(floorPrice))) {
                floorPrice = web3.utils.toBN(bid['value']);
            }
            if (ceilingPrice === null || (web3.utils.toBN(bid['value'])).gte(web3.utils.toBN(ceilingPrice))) {
                ceilingPrice = web3.utils.toBN(bid['value']);
            }
        }
    }

    if (floorPrice === null) {
        floorPrice = web3.utils.toBN(0);
    }
    if (ceilingPrice === null) {
        ceilingPrice = web3.utils.toBN(0);
    }

    return [floorPrice.toString(), ceilingPrice.toString()];
}

async function getCollectionPrices(collectionId) {
    let floorPrice = null;
    let ceilingPrice = null;

    let asks = await db.manyOrNone('SELECT * FROM "asks" WHERE "collectionId" = $1', [collectionId]);
    if (asks.length > 0) {
        for (let ask of asks) {
            if (floorPrice === null || (web3.utils.toBN(ask['value'])).lte(web3.utils.toBN(floorPrice))) {
                floorPrice = web3.utils.toBN(ask['value']);
            }
            if (ceilingPrice === null || (web3.utils.toBN(ask['value'])).gte(web3.utils.toBN(ceilingPrice))) {
                ceilingPrice = web3.utils.toBN(ask['value']);
            }
        }
    }

    if (floorPrice === null) {
        floorPrice = web3.utils.toBN(0);
    }
    if (ceilingPrice === null) {
        ceilingPrice = web3.utils.toBN(0);
    }

    return [floorPrice.toString(), ceilingPrice.toString()];
}

function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

async function startListeningHolders() {
    collections = JSON.parse(await fs.readFile('collections.json'));

    lastBlock = await web3.eth.getBlockNumber();

    for (let key in collections) {
        let startBlockQuery = await db.oneOrNone('SELECT "value" FROM "meta" WHERE "name" = $1', ['last_block_' + key]);
        let startBlock = 0;
        if (startBlockQuery === null) {
            await db.any('INSERT INTO "meta" ("name", "value", "timestamp") VALUES ($1, $2, $3)', ['last_block_' + key, 0, Math.floor(Date.now() / 1000)]);      
        } else {
            startBlock = parseInt(startBlockQuery['value']);
        }

        let endBlock = startBlock + blockBatch;
        if (endBlock > lastBlock) {
            endBlock = lastBlock;
        }

        handleCollectionTransfers(key, startBlock, endBlock);
    }
}

async function handleCollectionTransfers(key, startBlock, endBlock) {
    let collection = collections[key];

    try {
        while (true) {
            console.log('Getting Transfer events for ' + collection['title'] + ' (' + collection['contractAddress'] + ') ' + startBlock + '/' + endBlock);

            let contract = new web3.eth.Contract(collection['abi'], collection['contractAddress']);

            let events = await contract.getPastEvents("Transfer", { 'fromBlock': startBlock, 'toBlock': endBlock });

            let sortedEvents = events.reverse().sort(function (x, y) {
                return x.blockNumber - y.blockNumber || x.transactionIndex - y.transactionIndex;
            });

            for (let row of sortedEvents) {
                if (row.removed) {
                    continue;
                }

                if (row['blockNumber'] in blockTimestamps) {
                    row['timestamp'] = blockTimestamps[row['blockNumber']]
                } else {
                    let block = await web3.eth.getBlock(row['blockNumber']);
                    row['timestamp'] = block['timestamp'];
                    blockTimestamps[row['blockNumber']] = block['timestamp'];
                }

                let id = collection['contractAddress'] + '-' + row['returnValues']['2'];

                let token = await db.oneOrNone('SELECT * FROM "holders" WHERE "id" = $1', [id]);
                if (token === null) {
                    await db.any('INSERT INTO "holders" ("id", "tokenNumber", "collectionId", "currentOwner", "lastTransfer") VALUES ($1, $2, $3, $4, $5)', [id, row['returnValues']['2'], collection['contractAddress'], row['returnValues']['1'], row['timestamp']]);
                } else {
                    await db.any('UPDATE "holders" SET "currentOwner" = $1, "lastTransfer" = $2 WHERE "id" = $3', [row['returnValues']['1'], row['timestamp'], id]);
                }
            }

            startBlock = endBlock;
            await db.any('UPDATE "meta" SET "value" = $1, "timestamp" = $2 WHERE "name" = $3', [startBlock, Math.floor(Date.now() / 1000), 'last_block_' + key]);
                    
            if (startBlock >= lastBlock) {
                endBlock = await web3.eth.getBlockNumber();
                await sleep(120000);
            } else {
                endBlock += blockBatch;
                if (endBlock > lastBlock) {
                    endBlock = lastBlock;
                }
                await sleep(800);
            }
        }
    } catch (e) {
        console.log(e);
        handleCollectionTransfers(key, startBlock, endBlock);
    }
}

function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

startListening();