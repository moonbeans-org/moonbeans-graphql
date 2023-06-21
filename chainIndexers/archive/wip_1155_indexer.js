async function handleCollectionTransfers(key, startBlock, endBlock, collections) {
    let collection = collections[key];
    if (collection['isERC1155']) {
        try {
            while (true) {
                console.log('Getting Transfer events for ' + collection['title'] + ' (' + collection['contractAddress'] + ') ' + startBlock + '/' + endBlock);

                let contract = new web3.eth.Contract(ABIS.NFT1155, collection['contractAddress']);

                let batchEvents = await contract.getPastEvents("TransferBatch", { 'fromBlock': startBlock, 'toBlock': endBlock });
                let singleEvents = await contract.getPastEvents("TransferSingle", { 'fromBlock': startBlock, 'toBlock': endBlock });
                let events = batchEvents.concat(singleEvents);

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

                    const token_id_or_ids = row['returnValues']['3'];
                    let ids = [];
                    let quants = [];
                    //if this is a single transfer, we have id
                    if (typeof token_id_or_ids !== "array") {
                        ids.push(`${collection['contractAddress'] + '-' + token_id_or_ids}`);
                        quants.push("0");
                    }
                    //if this is a batch transfer, we have a list of ids.
                    else {
                        token_id_or_ids.forEach((id, index) => {
                            ids.push(`${collection['contractAddress'] + '-' + id}`);
                            quants.push(row['returnValues']['4'][index]);
                        })
                    }

                    //TODO: Rejigger DB to handle 1155's, and add logic to handle multiple holders of the same token, since we have quantities.
                    for (const id in ids) {
                        let token = await db.oneOrNone('SELECT * FROM "holders" WHERE "id" = $1', [id]);
                        if (token === null) {
                            await db.any('INSERT INTO "holders" ("id", "tokenNumber", "collectionId", "currentOwner", "lastTransfer", "chainName") VALUES ($1, $2, $3, $4, $5, $6)', 
                                [id, row['returnValues']['2'], collection['contractAddress'], row['returnValues']['1'], row['timestamp'], CHAIN_NAME]
                            );
                        } else {
                            await db.any('UPDATE "holders" SET "currentOwner" = $1, "lastTransfer" = $2 WHERE "id" = $3', [row['returnValues']['1'], row['timestamp'], id]);
                        }
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
            handleCollectionTransfers(key, startBlock, endBlock, collections);
        }
    }
}