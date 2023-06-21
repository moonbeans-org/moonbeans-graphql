require('dotenv').config();
const { ethers } = require('ethers');
const { Pool } = require('pg');
const { CHAINS, CHAIN_LIST } = require("./utils/chains.js");
const { program, Option } = require('commander');
const ABIS = require("./utils/abis.js");
const pool = new Pool({
    connectionString: `postgres://${process.env.DBUSER}:${process.env.DBPASS}@${process.env.DBHOST}:5432/${process.env.DBNAME}${process.env.USE_SSL === "true" ? "?ssl=true" : ""}`
});

program
  .version('1.0.0', '-v, --version')
  .addOption(new Option('-c, --chain <value>', 'chain name ~ should be present in chains.js').choices(CHAIN_LIST))
  .parse();

const options = program.opts();
const CHAIN_NAME = options.chain;
let chainObject = CHAINS[CHAIN_NAME];
const BLOCKS_PER_BATCH = chainObject?.blockBatch ?? 500;

const provider = new ethers.providers.WebSocketProvider(chainObject?.rpc);

const contractAddresses = [
    // List your thousands of contract addresses here
];

async function main() {
    await pool.connect();

    const startBlock = await getStartBlock();
    const endBlock = await provider.getBlockNumber();

    for (let fromBlock = startBlock; fromBlock < endBlock; fromBlock += BLOCKS_PER_BATCH) {
        const toBlock = Math.min(fromBlock + BLOCKS_PER_BATCH - 1, endBlock);

        console.log(`Processing blocks ${fromBlock} to ${toBlock}`);

        for (const address of contractAddresses) {
            const contractType = await getContractType(address);
            
            if (contractType === 'ERC721') {
                await processErc721(address, fromBlock, toBlock);
            } else if (contractType === 'ERC1155') {
                await processErc1155(address, fromBlock, toBlock);
            }
        }
    }
}

async function main() {
    await pool.connect();

    const endBlock = await provider.getBlockNumber();

    for (const address of contractAddresses) {
        const startBlock = await getStartBlock(address);

        for (let fromBlock = startBlock; fromBlock < endBlock; fromBlock += BLOCKS_PER_BATCH) {
            const toBlock = Math.min(fromBlock + BLOCKS_PER_BATCH - 1, endBlock);

            console.log(`Processing blocks ${fromBlock} to ${toBlock} for contract ${address}`);

            const code = await provider.getCode(address);
            const isErc721 = code.includes(ethers.utils.id('Transfer(address,address,uint256)').slice(2, 10));
            const isErc1155TransferSingle = code.includes(ethers.utils.id('TransferSingle(address,address,address,uint256,uint256)').slice(2, 10));
            const isErc1155TransferBatch = code.includes(ethers.utils.id('TransferBatch(address,address,address,uint256[],uint256[])').slice(2, 10));
            const isErc1155 = isErc1155TransferSingle || isErc1155TransferBatch;

            if (isErc721) {
                await processErc721(address, fromBlock, toBlock);
            } else if (isErc1155) {
                await processErc1155(address, fromBlock, toBlock);
            }
        }

        // Store the latest processed block number for the current contract
        await storeLatestProcessedBlock(address, endBlock - 1);
    }
}

async function getStartBlock(contractAddress) {
    const query = `
        SELECT "value"::integer
        FROM public.meta
        WHERE "name" = $1;
    `;
    const values = [`lastIndexedBlock-${contractAddress}`];
    const { rows } = await pool.query(query, values);

    const DEFAULT_START_BLOCK = 1;
    return rows.length > 0 ? rows[0].value : DEFAULT_START_BLOCK;
}

async function storeLatestProcessedBlock(contractAddress, blockNumber) {
    const query = `
        INSERT INTO public.meta ("name", "value", "timestamp")
        VALUES ($1, $2, NOW())
        ON CONFLICT ("name") DO UPDATE
        SET "value" = EXCLUDED.value, "timestamp" = EXCLUDED.timestamp;
    `;
    const values = [`lastIndexedBlock-${contractAddress}`, blockNumber.toString()];
    await pool.query(query, values);
}


async function processErc721(contractAddress, fromBlock, toBlock) {
    const contract = new ethers.Contract(contractAddress, ABIS.NFT, provider);

    const filter = contract.filters.Transfer(null, null, null);
    const events = await contract.queryFilter(filter, fromBlock, toBlock);

    for (const event of events) {
        const { from, to, tokenId } = event.args;

        await upsertHolder(contractAddress, tokenId.toString(), to, CHAIN_NAME, 1);
    }
}


async function processErc1155(contractAddress, fromBlock, toBlock) {
    const contract = new ethers.Contract(contractAddress, ABIS.NFT1155, provider);

    const transferSingleFilter = contract.filters.TransferSingle(null, null, null, null, null);
    const transferBatchFilter = contract.filters.TransferBatch(null, null, null, null, null);

    const transferSingleEvents = await contract.queryFilter(transferSingleFilter, fromBlock, toBlock);
    const transferBatchEvents = await contract.queryFilter(transferBatchFilter, fromBlock, toBlock);

    for (const event of transferSingleEvents) {
        const { operator, from, to, id, value } = event.args;

        if (value.gt(0)) {
            await upsertHolder(contractAddress, id.toString(), to, CHAIN_NAME, value);
        }
    }

    for (const event of transferBatchEvents) {
        const { operator, from, to, ids, values } = event.args;

        for (let i = 0; i < ids.length; i++) {
            const id = ids[i];
            const value = values[i];

            if (value.gt(0)) {
                await upsertHolder(contractAddress, id.toString(), to, CHAIN_NAME, value);
            }
        }
    }
}


async function upsertHolder(contractAddress, tokenId, currentOwner, chainName, balance = null) {
    const query = `
      INSERT INTO public.holders (collectionId, tokenNumber, currentOwner, lastTransfer, chainName, balance)
      VALUES ($1, $2, $3, NOW(), $4, $5)
      ON CONFLICT (collectionId, tokenNumber, currentOwner) DO UPDATE
      SET balance = EXCLUDED.balance, lastTransfer = EXCLUDED.lastTransfer;
    `;
    const values = [contractAddress, tokenId, currentOwner, chainName, balance];
  
    await pool.query(query, values);
  }


  async function getContractType(contractAddress) {
    // Try to get the contract type from database
    const query = `
        SELECT isERC1155 FROM public.collections
        WHERE contractAddress = $1
        LIMIT 1
    `;
    const { rows } = await pool.query(query, [contractAddress]);

    if (rows.length > 0) {
        return rows[0].isERC1155 ? 'ERC1155' : 'ERC721';
    }

    // If not in database, check using supportsInterface
    const contract = new ethers.Contract(contractAddress, ABIS.IERC165, provider);
    const isErc721 = await contract.supportsInterface('0x80ac58cd');
    const isErc1155 = await contract.supportsInterface('0xd9b67a26');

    // Store the contract type in database
    if (isErc721 || isErc1155) {
        const updateQuery = `
            UPDATE public.collections
            SET isERC1155 = $1
            WHERE contractAddress = $2
        `;
        await pool.query(updateQuery, [isErc1155, contractAddress]);
    } else {
        console.log("Contract not recognized as 721 or 1155 - guessing 721.");
    }

    return isErc1155 ? 'ERC1155' : 'ERC721';
}


main().catch((error) => {
    console.error('Error:', error);
    process.exit(1);
});
