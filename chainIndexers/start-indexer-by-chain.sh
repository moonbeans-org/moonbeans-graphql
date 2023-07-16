#!/bin/bash

if [ "$CHAIN" = "invalid" ]; then
    echo "Chain not set."
else
    # Start the indexer using the chain type set in the environment variable
    node v2-indexer.js -c "$CHAIN"
fi
