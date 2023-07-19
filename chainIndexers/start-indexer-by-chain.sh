#!/bin/bash

if [[ ($CHAIN != "moonbeam" && $CHAIN != "moonriver") || ($SCRIPT != "holder" && $SCRIPT != "indexer") ]]; then
    echo "Invalid chain or script"
else
    if [ "$SCRIPT" = "indexer" ]; then
        node v2-indexer.js -c "$CHAIN"
    else
        node holders-indexer.js -c "$CHAIN"
    fi
fi