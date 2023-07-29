require('dotenv').config()
const Web3 = require("web3");
const fs = require("fs");
const timers = require('timers/promises');
const pgp = require("pg-promise")({});
const cn = `postgres://${process.env.DBUSER}:${process.env.DBPASS}@${process.env.DBHOST}:5432/${process.env.DBNAME}${process.env.USE_SSL === "true" ? "?ssl=true" : ""}`;
const db = pgp(cn);
const ABIS = require("./utils/abis.js");
const { program, Option } = require('commander');
const { CHAINS, CHAIN_LIST } = require("./utils/chains.js");
const { prependIpfs, convertIpfstoHttp, addJustCors } = require("./utils/urlModifiers.js");
const fetch = require('node-fetch');

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
const blockBatch = chainObject?.blockBatch ?? 2000;
let blockTimestamps = {};

console.log("Starting Moonbeans Indexer for " + CHAIN_NAME);

let collections = JSON.parse(fs.readFileSync(__dirname + '/utils/collections.json'));
let noIndexList = ['0xc433f820467107bc5176b95f3a58248C4332F8DE', '0x7B2e778453AB3a0D946c4620fB38A0530A434e15', '0x08716e418e68564C96b68192E985762740728018', '0x7BCbA68680a1178A84a816B6B2a6Aa191eAA4734'];

/*****************
    WEB3 SETUP
******************/

// Get our web3 provider setup
const web3 = new Web3(new Web3.providers.WebsocketProvider(chainObject.rpc, {
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


/*****************
     ENTRYPOINT
******************/
startListening();

async function startListening() {
    chainObject = await setupChain(chainObject);
    startListeningHolders();
}


/*****************
    HOLDERS
******************/
async function startListeningHolders() {

    lastBlock = await web3.eth.getBlockNumber();

    for (let index in collections) {
        let collection = collections[index];
        if (collection['chain'] !== CHAIN_NAME || collection['contractAddress'] === '0xfEd9e29b276C333b2F11cb1427142701d0D9f7bf') continue;

        // if (index < 0 || index > 20) continue; // TODO: ?? handle this

        let key = collection?.collectionId;
        let startBlockQuery = await db.oneOrNone('SELECT "value" FROM "meta" WHERE "name" = $1', ['last_block_' + key]);
        let startBlock = collection?.startBlock ?? 0;
        if (startBlockQuery === null) {
            await db.any('INSERT INTO "meta" ("name", "value", "timestamp") VALUES ($1, $2, $3)', ['last_block_' + key, startBlock, Math.floor(Date.now() / 1000)]);
        } else {
            startBlock = parseInt(startBlockQuery['value']);
        }

        let endBlock = startBlock + blockBatch;

        if (endBlock > lastBlock) {
            endBlock = lastBlock;
        }

        handleCollectionTransfers(index, key, startBlock, endBlock, lastBlock, collection); // TODO: REMOVE await
    }
}


async function handleCollectionTransfers(index, key, startBlock, endBlock, lastBlock, collection) {
    try {
        while (true) {
            console.log('Getting Transfer events for ' + collection['title'] + ' (' + collection['contractAddress'] + ') ' + startBlock + '-' + endBlock + '/' + lastBlock);

            if (collection?.['isERC1155'] ?? false) {
                await handle1155Transfers(collection, startBlock, endBlock);
            } else {
                await handle721Transfers(collection,startBlock, endBlock)
            }

            startBlock = endBlock;
            await db.any('UPDATE "meta" SET "value" = $1, "timestamp" = $2 WHERE "name" = $3', [startBlock, Math.floor(Date.now() / 1000), 'last_block_' + key]);

            if (startBlock >= lastBlock) {
                // return; // TODO: REMOVE ME
                endBlock = await web3.eth.getBlockNumber();
                await sleep(120000);
            } else {
                endBlock = startBlock + blockBatch;
                if (endBlock > lastBlock) {
                    endBlock = lastBlock;
                }
                await sleep(200); // TODO: UNCOMMENT ME (800 default)
            }
        }
    } catch (e) {
        console.log(e?.message, `error collecting transfers for ${collection['title']}, sleeping for 2 secs and retrying. ${startBlock} ${endBlock} ${lastBlock}`);
        await sleep(2000);
        handleCollectionTransfers(index, key, startBlock, endBlock, lastBlock, collection);
    }
}


async function handle721Transfers(collection, startBlock, endBlock) {
    let contract = new web3.eth.Contract(ABIS.NFT, collection['contractAddress']);

    let events = await contract.getPastEvents("Transfer", { 'fromBlock': startBlock, 'toBlock': endBlock });

    let sortedEvents = events.reverse().sort(function (x, y) {
        return x.blockNumber - y.blockNumber || x.transactionIndex - y.transactionIndex || x.logIndex - y.logIndex || x.transactionHash - y.transactionHash;
    });

    for (let row of sortedEvents) {
        if (row.removed) {
            continue;
        }

        row['transactionEventHash'] = row['transactionHash'] + "-" + row['transactionIndex'] + "-" + row['logIndex'] + "-" + row['blockNumber'];

        const txrow = await db.oneOrNone('SELECT * FROM "transfers" WHERE "id" = $1', [row['transactionEventHash']]);

        if (txrow !== null) {
            continue;
        }

        if (row['blockNumber'] in blockTimestamps) {
            row['timestamp'] = blockTimestamps[row['blockNumber']]
        } else {
            let block = await web3.eth.getBlock(row['blockNumber']);
            row['timestamp'] = block['timestamp'];
            blockTimestamps[row['blockNumber']] = block['timestamp'];
        }

        const { from, to, tokenId } = row['returnValues'];
        const oldId = `${collection['contractAddress']}-${tokenId}-${from}`;
        const newId = `${collection['contractAddress']}-${tokenId}-${to}`;

        //Update Holders
        let newOwner = await db.oneOrNone('SELECT * FROM "holders" WHERE "id" = $1', [newId]);
        let oldOwner = await db.oneOrNone('SELECT * FROM "holders" WHERE "id" = $1', [oldId]);

        if (newOwner === null) {
            await db.any('INSERT INTO "holders" ("id", "collectionId", "tokenNumber", "currentOwner", "lastTransfer", "chainName", "balance") VALUES ($1, $2, $3, $4, $5, $6, $7)', 
                [newId, collection['contractAddress'], tokenId, to, row['timestamp'], CHAIN_NAME, 1]);
        } else {
            await db.any('UPDATE "holders" SET "currentOwner" = $1, "lastTransfer" = $2, "balance" = $3 WHERE "id" = $4 ', 
                [to, row['timestamp'], 1, newId]);
        }

        //old owner should never be null.
        if (oldOwner === null) {
            await db.any('INSERT INTO "holders" ("id", "collectionId", "tokenNumber", "currentOwner", "lastTransfer", "chainName", "balance") VALUES ($1, $2, $3, $4, $5, $6, $7)', 
                [oldId, collection['contractAddress'], tokenId, from, row['timestamp'], CHAIN_NAME, 0]);
        } else {
            await db.any('UPDATE "holders" SET "balance" = $1 WHERE "id" = $2', [0, oldId]);
        }

        //Update token?
        try {
            let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [`${collection['contractAddress']}-${tokenId}`]);
            if (!token?.['tokenURI'] && !noIndexList.includes(collection['contractAddress'])) {
                await fetchAndStoreTokenMetadata(collection, contract, collection['contractAddress'], tokenId, row['timestamp']);
            }
        } catch {
            console.log(`Failed attempting to fetch and store token metadata for ${collection['contractAddress']}-${tokenId}. Continuing...`);
        }

        await markTransfer(row);


    }
}


async function handle1155Transfers(collection, startBlock, endBlock) {
    let contract = new web3.eth.Contract(ABIS.NFT1155, collection['contractAddress']);
    let singleEvents = await contract.getPastEvents("TransferSingle", { 'fromBlock': startBlock, 'toBlock': endBlock });
    let batchEvents = await contract.getPastEvents("TransferBatch", { 'fromBlock': startBlock, 'toBlock': endBlock });
    batchEvents = batchEvents.map(event => ({ ...event, batchTransfer: true}));

    let sortedEvents = singleEvents.concat(batchEvents).reverse().sort(function (x, y) {
        return x.blockNumber - y.blockNumber || x.transactionIndex - y.transactionIndex || x.logIndex - y.logIndex || x.transactionHash - y.transactionHash;
    });

    for (let row of sortedEvents) {
        if (row.removed) {
            continue;
        }

        row['transactionEventHash'] = row['transactionHash'] + "-" + row['transactionIndex'] + "-" + row['logIndex'] + "-" + row['blockNumber'];

        const txrow = await db.oneOrNone('SELECT * FROM "transfers" WHERE "id" = $1', [row['transactionEventHash']]);

        if (txrow !== null) {
            continue;
        }

        if (row['blockNumber'] in blockTimestamps) {
            row['timestamp'] = blockTimestamps[row['blockNumber']]
        } else {
            let block = await web3.eth.getBlock(row['blockNumber']);
            row['timestamp'] = block['timestamp'];
            blockTimestamps[row['blockNumber']] = block['timestamp'];
        }

        if (row?.batchTransfer) {
            const { operator, from, to, ids, values } = row['returnValues'];
            for (let i = 0; i < ids.length; i++) {
                await handleSingleTransfer(row, collection, contract, from, to, ids[i], values[i]);
            }
        } else {
            const { operator, from, to, id, value } = row['returnValues'];
            await handleSingleTransfer(row, collection, contract, from, to, id, value);
        }

        await markTransfer(row);
    }
}


async function handleSingleTransfer(row, collection, contract, from, to, tokenId, value) {

    const oldId = `${collection['contractAddress']}-${tokenId}-${from}`;
    const newId = `${collection['contractAddress']}-${tokenId}-${to}`;

    //Update Holders
    let newOwner = await db.oneOrNone('SELECT * FROM "holders" WHERE "id" = $1', [newId]);
    let oldOwner = await db.oneOrNone('SELECT * FROM "holders" WHERE "id" = $1', [oldId]);

    if (newOwner === null) {
        await db.any('INSERT INTO "holders" ("id", "collectionId", "tokenNumber", "currentOwner", "lastTransfer", "chainName", "balance") VALUES ($1, $2, $3, $4, $5, $6, $7)', 
            [newId, collection['contractAddress'], tokenId, to, row['timestamp'], CHAIN_NAME, value]);
    } else {
        await db.any('UPDATE "holders" SET "currentOwner" = $1, "lastTransfer" = $2, "balance" = "balance" + $3 WHERE "id" = $4 ', 
            [to, row['timestamp'], value, newId]);
        await db.any('UPDATE "holders" SET "balance" = "balance" - $1 WHERE "id" = $2', 
            [value, oldId]);
    }

    //old owner should never be null.
    if (oldOwner === null) {
        await db.any('INSERT INTO "holders" ("id", "collectionId", "tokenNumber", "currentOwner", "lastTransfer", "chainName", "balance") VALUES ($1, $2, $3, $4, $5, $6, $7)', 
            [oldId, collection['contractAddress'], tokenId, from, row['timestamp'], CHAIN_NAME, 0]);
    } else {
        await db.any('UPDATE "holders" SET "balance" = "balance" - $1 WHERE "id" = $2', 
            [value, oldId]);
    }

    //Update token?
    try {
        let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [`${collection['contractAddress']}-${tokenId}`]);
        if (!token?.['tokenURI']) {
            await fetchAndStoreTokenMetadata(collection, contract, collection['contractAddress'], tokenId, row['timestamp']);
        }
    } catch {
        console.log(`Failed attempting to fetch and store token metadata for ${collection['contractAddress']}-${tokenId}. Continuing...`);
    }
}


async function markTransfer(row) {

    if (row['timestamp'] === undefined) {
        let block = await web3.eth.getBlock(row['blockNumber']);
        row['timestamp'] = block['timestamp'];
    }

    try {
        await db.any('INSERT INTO "transfers" ("id", "blockNumber", "timestamp", "chainName") VALUES ($1, $2, $3, $4)',
            [row['transactionEventHash'], row['blockNumber'], row['timestamp'], CHAIN_NAME]);

    } catch (e) {
        console.log(`Error updating transfers table: [${row['transactionEventHash']}].`);
        console.log(e?.message);
        console.debug(e);
    }

}



/*****************
     HELPERS
******************/

async function fetchAndStoreTokenMetadata(collection, contract, ca, tokenId, timestamp) {
    console.log(`attempting to fetch and store metadata for ${ca}-${tokenId}`);
    const uri = collection?.isERC1155 ? await contract.methods.uri(tokenId).call() : await contract.methods.tokenURI(tokenId).call();

    const ipfsMetadataUrl = ((collection?.uriFromChainGalleryPage ?? false) && uri !== undefined) 
        ? convertIpfstoHttp(collection?.prependIPFS ? prependIpfs(uri) : uri)
        : convertIpfstoHttp(`${collection?.links.MetadataBase}${tokenId}${collection?.metadataExtension ?? ""}`);

    const metadataUrl = collection?.convertCORS ? addJustCors(ipfsMetadataUrl) : ipfsMetadataUrl;

    if (collection?.metadataStoredOnChain && collection?.uriFromChainGalleryPage && uri !== undefined) {
        let unparsed_metadata = collection?.decodeOnChainMetadata ? Buffer.from(uri?.slice(29), 'base64').toString('ascii') : uri;
        const response = JSON.parse(unparsed_metadata);
        const image = response?.image;
        await db.any('INSERT INTO "tokens" ("id", "collectionId", "tokenNumber", "chainName", "tokenURI", "metadataBlob", "imageURI") VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT ("id") DO UPDATE SET "tokenURI" = EXCLUDED."tokenURI", "metadataBlob" = EXCLUDED."metadataBlob", "imageURI" = EXCLUDED."imageURI"',
            [`${ca}-${tokenId}`, ca, tokenId, CHAIN_NAME, metadataUrl, response, image]);

    } else {
        fetch(metadataUrl)
            .then(res => res.json())
            .then(async response => {
                const image = convertIpfstoHttp(collection?.useThumbnailField ? response?.thumbnail || response?.imageThumbnail : response?.image);
                await db.any('INSERT INTO "tokens" ("id", "collectionId", "tokenNumber", "chainName", "tokenURI", "metadataBlob", "imageURI") VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT ("id") DO UPDATE SET "tokenURI" = EXCLUDED."tokenURI", "metadataBlob" = EXCLUDED."metadataBlob", "imageURI" = EXCLUDED."imageURI"',
                [`${ca}-${tokenId}`, ca, tokenId, CHAIN_NAME, metadataUrl, response, image]);
            })
            .catch(error => {
                console.log(`failed with ${error.message}, trying again with cors`);
                fetch(addJustCors(metadataUrl))
                .then(res => res.json())
                .then(async response => {
                    const image = convertIpfstoHttp(collection?.useThumbnailField ? response?.thumbnail || response?.imageThumbnail : response?.image);
                    await db.any('INSERT INTO "tokens" ("id", "collectionId", "tokenNumber", "chainName", "tokenURI", "metadataBlob", "imageURI") VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT ("id") DO UPDATE SET "tokenURI" = EXCLUDED."tokenURI", "metadataBlob" = EXCLUDED."metadataBlob", "imageURI" = EXCLUDED."imageURI"',
                    [`${ca}-${tokenId}`, ca, tokenId, CHAIN_NAME, metadataUrl, response, image]);
                })
                .catch(error_dos => {
                    console.log(`failed again with ${error.message}`);
                })
                
            });
    }

}


function _typeToString(input) {
    if (input.type === "tuple") {
        return "(" + input.components.map(_typeToString).join(",") + ")";
    }
    return input.type;
}


function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

async function setupChain(chainObject) {
    // UPDATE/PULL CHAIN DATA IN DB
    let chain = await db.oneOrNone('SELECT * FROM "chains" WHERE "name" = $1', [CHAIN_NAME]);
    if (chain === null) {
        await db.any('INSERT INTO "chains" ("name", "chainId", "startBlock", "lastIndexedBlock") VALUES ($1, $2, $3, $4)', 
            [CHAIN_NAME, chainObject?.chain_id, chainObject?.startBlock ?? 0, chainObject?.startBlock ?? 0]);
    } else {
        chainObject.startBlock = parseInt(chain['lastIndexedBlock']);
    }
    return chainObject;
}