//****************************************************************//
// This script indexes all of the existing token data in Postgres //
//            in the Weaviate vector search database              //
//****************************************************************//

const pgp = require("pg-promise")({});
const cn = 'postgres://postgres:<DBPASS>@<DBHOST>:5432/moonbeanstwochainz';
const db = pgp(cn);
const fs = require("fs").promises;
const { default: weaviate } = require('weaviate-ts-client');
const client = weaviate.client({
  scheme: 'http',
  host: 'localhost:8080',
  headers: {"X-OpenAI-Api-Key": "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"}
});

client
  .schema
  .getter()
  .do()
  .then(res => {
    console.log(res);
  })
  .catch(err => {
    console.error(err)
  });


