const fs = require("fs").promises;
const fetch = require('node-fetch');

const data = JSON.stringify({
    query: `{
      allCollections {
        nodes {
          isErc1155
          chainName
          id
        }
      }
    }`,
  });


const caller = () => {
  fetch("https://graph.api.yesports.gg/graphql", {
    method: "POST",
    headers: {'Content-Type': 'application/json'},
    body: data
  }).then(res => {return res.json()}).then(async response => {
      console.log("Collections Pulled:", response);
      await fs.writeFile('./test.json', JSON.stringify(response?.data?.allCollections?.nodes, null, 3));
      return response;
  });
}

module.exports = { caller }