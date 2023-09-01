require('dotenv').config()

const CHAINS = {
    "moonriver": {
        "chain_name": "moonriver",
        "chain_id": 1285,
        "rpc": `wss://moonriver.blastapi.io/${process.env.BLAST_KEY}`,
        "old_marketplace_contract_address": "0x16d7Edd3A562BB60aA0B3Af357A2c195cE2AA974",
        "marketplace_contract_address": "0x11C0f9d0fc3E30E546B6C19C4a2da5caaaC19E5f",
        "fungible_marketplace_contract_address": "0x3686d82A54b39f3136A3B2eb12b77F5d577855D5",
        "testnet": false,
        "startBlock": 817352
    },
    "moonbeam": {
        "chain_name": "moonbeam",
        "chain_id": 1284,
        "rpc": `wss://moonbeam.blastapi.io/${process.env.BLAST_KEY}`,
        "old_marketplace_contract_address": "0x683724817a7d526d6256Aec0D6f8ddF541b924de",
        "marketplace_contract_address": "0x3e59684d7806aD0BEDeaB9fb4c2D277F7300DbAe",
        "fungible_marketplace_contract_address": "0x9Ff0cF19F66ab00774DE20b311825b7F65f23972",
        "testnet": false,
        "startBlock": 975368
    },
    "arbitrum_one": {
        "chain_name": "arbitrum_one",
        "chain_id": 42161,
        "rpc": `wss://arbitrum-one.blastapi.io/${process.env.BLAST_KEY}`,
        "marketplace_contract_address": "0x3A6dffd314818A37FA67c23465cf7992DdF229d6",
        "fungible_marketplace_contract_address": "0xC2392DD3e3fED2c8Ed9f7f0bDf6026fcd1348453",
        "testnet": false
    },
    "arbitrum_nova": {
        "chain_name": "arbitrum_nova",
        "chain_id": 42170,
        "rpc": `wss://arbitrum-nova.blastapi.io/${process.env.BLAST_KEY}`,
        "marketplace_contract_address": "0x56892CBf4BFf9FCDE4E65076b135440F315a91f2",
        "fungible_marketplace_contract_address": "0x0d45F008283943E7F91Cf8af0e9e7Ea85d1ab01d",
        "testnet": false
    }
};

const CHAIN_LIST = ['moonriver', 'moonbeam', 'arbitrum_one', 'arbitrum_nova'];

module.exports = { CHAINS, CHAIN_LIST };