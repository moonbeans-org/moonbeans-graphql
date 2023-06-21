const fs = require("fs").promises;
const http = require("http");
const fetch = require('node-fetch');


fetch("https://staging.api.moonbeans.io/collection", {
  "method": "GET"
}).then(res => {return res.json()}).then(async response => {
    await fs.writeFile('./utils/test_collections.json', JSON.stringify(response, null, 3));
});

fetch("https://api.moonbeans.io/collection", {
  "method": "GET"
}).then(res => {return res.json()}).then(async response => {
    await fs.writeFile('./utils/collections.json', JSON.stringify(response, null, 3));
});