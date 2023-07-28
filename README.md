# Moonbeans Backend

Sorry, this one's a mess. Still working on it. Check the `startup` directory to read the startup scripts and maybe you can piece together what I was going for.
Currently supported chains: `moonbeam`, `moonriver`, `arbitrum_one`, `arbitrum_nova`. Or just check `chainIndexers/chains.js`.

Includes:
- Indexer (`chainIndexers/v2-indexer.js` - specify a chain to index like so `node v2-indexer.js --chain moonbeam`)
- GraphQL Server (Sits on top of chain-agnostic Postgres DB - contained in top-level `watcher.js`)
- Listing Cleanup Script/Cron (Scans for transfer events on listed tokens - peep `cleanup/listingCleanup.sh` directory)
- Get Collections from API (`collectionRefresh`)
- Collection List (`chainIndexers/utils/collections.json`)
- Marketplace & NFT ABIs (`chainIndexers/utils/abis.js`)
- Chain Data (`chainIndexers/utils/chains.js`)
- Archived indexers and db dumps and such are in the appropriate `archive` folders. 
- WIP - holders indexer for 1155's and 721's (`chainIndexers/ai-holder-indexer.js`)
- Other stuff, surely. maybe. probably.


Container Deployment Information:
-  `.github/workflows/aws.yml` contains the automatic deployment script for the watcher and indexer container services.
- `ecs-task-definition.json` and `Dockerfile` contain the watcher ECS service task and container definitions.
- `chainIndexers/indexer-ecs-task-definition.json` and `chainIndexers/Dockerfile` contain the indexer ECS service task and container definitions.
- `chainIndexers/holder-ecs-task-definition.json` and `chainIndexers/Dockerfile` contain the holder ECS service task and container definitions.
- `chainIndexers/beanzilla` contains lambda function code (node.js 18.x) for automatically restarting the indexer service to prevent out-of-memory errors.