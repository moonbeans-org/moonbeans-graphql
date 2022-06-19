const ConnectionFilterPlugin = require("postgraphile-plugin-connection-filter");
const Web3 = require("web3");
const fs = require("fs").promises;
const http = require("http");
const { postgraphile } = require("postgraphile");
const pgp = require("pg-promise")({});
const cn = 'postgres://postgres:<DBPASS>@<DBHOST>:5432/moonbeanstwochainz';
const db = pgp(cn);

const marketPlaceAbi = [{ "type": "event", "name": "BidCancelled", "inputs": [{ "type": "address", "name": "token", "internalType": "address", "indexed": true }, { "type": "uint256", "name": "id", "internalType": "uint256", "indexed": true }, { "type": "uint256", "name": "price", "internalType": "uint256", "indexed": true }, { "type": "address", "name": "buyer", "internalType": "address", "indexed": false }, { "type": "bool", "name": "escrowed", "internalType": "bool", "indexed": false }, { "type": "uint256", "name": "timestamp", "internalType": "uint256", "indexed": false }], "anonymous": false }, { "type": "event", "name": "BidPlaced", "inputs": [{ "type": "address", "name": "token", "internalType": "address", "indexed": true }, { "type": "uint256", "name": "id", "internalType": "uint256", "indexed": true }, { "type": "uint256", "name": "price", "internalType": "uint256", "indexed": true }, { "type": "address", "name": "buyer", "internalType": "address", "indexed": false }, { "type": "uint256", "name": "timestamp", "internalType": "uint256", "indexed": false }, { "type": "bool", "name": "escrowed", "internalType": "bool", "indexed": false }], "anonymous": false }, { "type": "event", "name": "EscrowReturned", "inputs": [{ "type": "address", "name": "user", "internalType": "address", "indexed": true }, { "type": "uint256", "name": "price", "internalType": "uint256", "indexed": true }], "anonymous": false }, { "type": "event", "name": "OwnershipTransferred", "inputs": [{ "type": "address", "name": "previousOwner", "internalType": "address", "indexed": true }, { "type": "address", "name": "newOwner", "internalType": "address", "indexed": true }], "anonymous": false }, { "type": "event", "name": "TokenDelisted", "inputs": [{ "type": "address", "name": "token", "internalType": "address", "indexed": true }, { "type": "uint256", "name": "id", "internalType": "uint256", "indexed": true }, { "type": "uint256", "name": "timestamp", "internalType": "uint256", "indexed": false }], "anonymous": false }, { "type": "event", "name": "TokenListed", "inputs": [{ "type": "address", "name": "token", "internalType": "address", "indexed": true }, { "type": "uint256", "name": "id", "internalType": "uint256", "indexed": true }, { "type": "uint256", "name": "price", "internalType": "uint256", "indexed": true }, { "type": "uint256", "name": "timestamp", "internalType": "uint256", "indexed": false }], "anonymous": false }, { "type": "event", "name": "TokenPurchased", "inputs": [{ "type": "address", "name": "oldOwner", "internalType": "address", "indexed": true }, { "type": "address", "name": "newOwner", "internalType": "address", "indexed": true }, { "type": "uint256", "name": "price", "internalType": "uint256", "indexed": true }, { "type": "address", "name": "collection", "internalType": "address", "indexed": false }, { "type": "uint256", "name": "tokenId", "internalType": "uint256", "indexed": false }], "anonymous": false }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "RecoverMOVR", "inputs": [{ "type": "address", "name": "to", "internalType": "address" }, { "type": "uint256", "name": "amount", "internalType": "uint256" }] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "address", "name": "", "internalType": "address" }], "name": "TOKEN", "inputs": [] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "acceptOffer", "inputs": [{ "type": "address", "name": "ca", "internalType": "address" }, { "type": "uint256", "name": "tokenId", "internalType": "uint256" }, { "type": "uint256", "name": "price", "internalType": "uint256" }, { "type": "address", "name": "from", "internalType": "address" }, { "type": "bool", "name": "escrowedBid", "internalType": "bool" }] }, { "type": "function", "stateMutability": "payable", "outputs": [], "name": "addMoneyToEscrow", "inputs": [] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "address", "name": "", "internalType": "address" }], "name": "beanBuybackAddress", "inputs": [] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "uint256", "name": "", "internalType": "uint256" }], "name": "beanBuybackFee", "inputs": [] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "address", "name": "", "internalType": "address" }], "name": "beanieHolderAddress", "inputs": [] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "uint256", "name": "", "internalType": "uint256" }], "name": "beanieHolderFee", "inputs": [] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "cancelOffer", "inputs": [{ "type": "address", "name": "ca", "internalType": "address" }, { "type": "uint256", "name": "tokenId", "internalType": "uint256" }, { "type": "uint256", "name": "price", "internalType": "uint256" }, { "type": "bool", "name": "escrowed", "internalType": "bool" }] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "uint256", "name": "", "internalType": "uint256" }], "name": "checkEscrowAmount", "inputs": [{ "type": "address", "name": "user", "internalType": "address" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "clearAllBids", "inputs": [{ "type": "address", "name": "ca", "internalType": "address" }, { "type": "uint256", "name": "tokenId", "internalType": "uint256" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "clearAllListings", "inputs": [{ "type": "address", "name": "ca", "internalType": "address" }, { "type": "uint256", "name": "tokenId", "internalType": "uint256" }] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "bool", "name": "", "internalType": "bool" }], "name": "clearBidsAfterAcceptingOffer", "inputs": [] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "uint256", "name": "", "internalType": "uint256" }], "name": "defaultCollectionOwnerFee", "inputs": [] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "bool", "name": "", "internalType": "bool" }], "name": "delistAfterAcceptingOffer", "inputs": [] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "delistToken", "inputs": [{ "type": "address", "name": "ca", "internalType": "address" }, { "type": "uint256", "name": "tokenId", "internalType": "uint256" }] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "address", "name": "", "internalType": "address" }], "name": "devAddress", "inputs": [] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "uint256", "name": "", "internalType": "uint256" }], "name": "devFee", "inputs": [] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "address", "name": "", "internalType": "address" }], "name": "featuredCollection", "inputs": [] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "bool", "name": "", "internalType": "bool" }], "name": "feesOn", "inputs": [] }, { "type": "function", "stateMutability": "payable", "outputs": [], "name": "fulfillListing", "inputs": [{ "type": "address", "name": "ca", "internalType": "address" }, { "type": "uint256", "name": "tokenId", "internalType": "uint256" }] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "uint256", "name": "", "internalType": "uint256" }], "name": "getCollectionFee", "inputs": [{ "type": "address", "name": "ca", "internalType": "address" }] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "address", "name": "", "internalType": "address" }], "name": "getCollectionOwner", "inputs": [{ "type": "address", "name": "ca", "internalType": "address" }] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "tuple", "name": "", "internalType": "struct MarketPlace.Listing", "components": [{ "type": "uint256", "name": "price", "internalType": "uint256" }, { "type": "uint256", "name": "timestamp", "internalType": "uint256" }, { "type": "uint256", "name": "tokenId", "internalType": "uint256" }, { "type": "bool", "name": "accepted", "internalType": "bool" }] }], "name": "getCurrentListing", "inputs": [{ "type": "address", "name": "ca", "internalType": "address" }, { "type": "uint256", "name": "tokenId", "internalType": "uint256" }] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "uint256", "name": "", "internalType": "uint256" }], "name": "getCurrentListingPrice", "inputs": [{ "type": "address", "name": "ca", "internalType": "address" }, { "type": "uint256", "name": "tokenId", "internalType": "uint256" }] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "uint256", "name": "", "internalType": "uint256" }], "name": "getEscrowedAmount", "inputs": [{ "type": "address", "name": "user", "internalType": "address" }] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "tuple[]", "name": "", "internalType": "struct MarketPlace.Offer[]", "components": [{ "type": "uint256", "name": "price", "internalType": "uint256" }, { "type": "uint256", "name": "timestamp", "internalType": "uint256" }, { "type": "bool", "name": "accepted", "internalType": "bool" }, { "type": "address", "name": "buyer", "internalType": "address" }, { "type": "bool", "name": "escrowed", "internalType": "bool" }] }], "name": "getOffers", "inputs": [{ "type": "address", "name": "ca", "internalType": "address" }, { "type": "uint256", "name": "tokenId", "internalType": "uint256" }] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "tuple[]", "name": "", "internalType": "struct MarketPlace.Listing[]", "components": [{ "type": "uint256", "name": "price", "internalType": "uint256" }, { "type": "uint256", "name": "timestamp", "internalType": "uint256" }, { "type": "uint256", "name": "tokenId", "internalType": "uint256" }, { "type": "bool", "name": "accepted", "internalType": "bool" }] }], "name": "getTokenListingHistory", "inputs": [{ "type": "address", "name": "ca", "internalType": "address" }, { "type": "uint256", "name": "tokenId", "internalType": "uint256" }] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "bool", "name": "", "internalType": "bool" }], "name": "isCollectionTrading", "inputs": [{ "type": "address", "name": "ca", "internalType": "address" }] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "bool", "name": "", "internalType": "bool" }], "name": "isListed", "inputs": [{ "type": "address", "name": "ca", "internalType": "address" }, { "type": "uint256", "name": "tokenId", "internalType": "uint256" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "listToken", "inputs": [{ "type": "address", "name": "ca", "internalType": "address" }, { "type": "uint256", "name": "tokenId", "internalType": "uint256" }, { "type": "uint256", "name": "price", "internalType": "uint256" }] }, { "type": "function", "stateMutability": "payable", "outputs": [], "name": "makeEscrowedOffer", "inputs": [{ "type": "address", "name": "ca", "internalType": "address" }, { "type": "uint256", "name": "tokenId", "internalType": "uint256" }, { "type": "uint256", "name": "price", "internalType": "uint256" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "makeOffer", "inputs": [{ "type": "address", "name": "ca", "internalType": "address" }, { "type": "uint256", "name": "tokenId", "internalType": "uint256" }, { "type": "uint256", "name": "price", "internalType": "uint256" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [{ "type": "bytes4", "name": "", "internalType": "bytes4" }], "name": "onERC721Received", "inputs": [{ "type": "address", "name": "", "internalType": "address" }, { "type": "address", "name": "", "internalType": "address" }, { "type": "uint256", "name": "", "internalType": "uint256" }, { "type": "bytes", "name": "", "internalType": "bytes" }] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "address", "name": "", "internalType": "address" }], "name": "owner", "inputs": [] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "recoverNFT", "inputs": [{ "type": "address", "name": "_token", "internalType": "address" }, { "type": "uint256", "name": "tokenId", "internalType": "uint256" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "recoverToken", "inputs": [{ "type": "address", "name": "_token", "internalType": "address" }, { "type": "uint256", "name": "amount", "internalType": "uint256" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "renounceOwnership", "inputs": [] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "setBeanBuyBackFee", "inputs": [{ "type": "uint256", "name": "fee", "internalType": "uint256" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "setBeanBuybackAddress", "inputs": [{ "type": "address", "name": "_address", "internalType": "address" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "setBeanieHolderAddress", "inputs": [{ "type": "address", "name": "_address", "internalType": "address" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "setBeanieHolderFee", "inputs": [{ "type": "uint256", "name": "fee", "internalType": "uint256" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "setClearBidsAfterAcceptingOffer", "inputs": [{ "type": "bool", "name": "_value", "internalType": "bool" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "setCollectionOwner", "inputs": [{ "type": "address", "name": "ca", "internalType": "address" }, { "type": "address", "name": "owner", "internalType": "address" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "setCollectionOwnerFee", "inputs": [{ "type": "address", "name": "ca", "internalType": "address" }, { "type": "uint256", "name": "fee", "internalType": "uint256" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "setCollectionTrading", "inputs": [{ "type": "address", "name": "ca", "internalType": "address" }, { "type": "bool", "name": "value", "internalType": "bool" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "setDelistAfterAcceptingOffer", "inputs": [{ "type": "bool", "name": "_value", "internalType": "bool" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "setDevAddress", "inputs": [{ "type": "address", "name": "_address", "internalType": "address" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "setDevFee", "inputs": [{ "type": "uint256", "name": "fee", "internalType": "uint256" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "setFeaturedCollection", "inputs": [{ "type": "address", "name": "_collection", "internalType": "address" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "setFeesOn", "inputs": [{ "type": "bool", "name": "_value", "internalType": "bool" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "setSpecialGasTax", "inputs": [{ "type": "uint256", "name": "gasAmount", "internalType": "uint256" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "setSuperGasTaxes", "inputs": [{ "type": "bool", "name": "value", "internalType": "bool" }] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "setTrading", "inputs": [{ "type": "bool", "name": "value", "internalType": "bool" }] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "uint256", "name": "", "internalType": "uint256" }], "name": "specialTaxGas", "inputs": [] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "uint256", "name": "", "internalType": "uint256" }], "name": "totalEscrowedAmount", "inputs": [] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "uint256", "name": "", "internalType": "uint256" }], "name": "totalFees", "inputs": [] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "bool", "name": "", "internalType": "bool" }], "name": "tradingPaused", "inputs": [] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "transferOwnership", "inputs": [{ "type": "address", "name": "newOwner", "internalType": "address" }] }, { "type": "function", "stateMutability": "view", "outputs": [{ "type": "bool", "name": "", "internalType": "bool" }], "name": "useSuperGasTaxes", "inputs": [] }, { "type": "function", "stateMutability": "nonpayable", "outputs": [], "name": "withdrawMoneyFromEscrow", "inputs": [{ "type": "uint256", "name": "amount", "internalType": "uint256" }] }, { "type": "receive", "stateMutability": "payable" }];

const blockBatch = 200;

let methodSignatures = [];
let collections = [];
let blockTimestamps = {};

const web3 = new Web3(new Web3.providers.WebsocketProvider("wss://moonriver.api.onfinality.io/public-ws", {
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

const marketPlaceContract = new web3.eth.Contract(marketPlaceAbi, "0x16d7Edd3A562BB60aA0B3Af357A2c195cE2AA974");

function _typeToString(input) {
    if (input.type === "tuple") {
        return "(" + input.components.map(_typeToString).join(",") + ")";
    }
    return input.type;
}

async function startListening() {
    startListeningMarketplace();
    startListeningHolders();
}

async function startListeningMarketplace() {
    let startBlock = parseInt(await fs.readFile("last_block.txt"));

    if (startBlock == 0) {
        startBlock = 817352;
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
            await fs.writeFile("./last_block.txt", "" + startBlock);
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

http.createServer(async function (req, res) {
    res.writeHead(200, {'Content-Type': 'application/json'});

    let responseData = {};
    responseData['last_block_movr'] = parseInt(await fs.readFile("last_block.txt"));
    responseData['last_block_glmr'] = parseInt(await fs.readFile("last_block_glmr.txt"));
    
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