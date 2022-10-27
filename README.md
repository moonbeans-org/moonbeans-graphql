# Moonbeans Backend

Sorry, this one's a mess. Still working on it. Check the `startup` directory to read the startup scripts and maybe you can piece together what I was going for.

Includes:
- Indexer (`chainIndexers/moonriver.js`, `chainIndexers/moonbeam.js`)
- GraphQL Server (Sits on top of chain-agnostic Postgres DB - contained in `chainIndexers/moonriver.js`. Server only with no indexer is in `watcher.js`)
- Listing Cleanup Script/Cron (Scans for transfer events on listed tokens - peep `cleanup/listingCleanup.sh` directory)
- Collection List (`chainIndexers/utils/collections.json`)
- Other stuff, surely. maybe. probably.