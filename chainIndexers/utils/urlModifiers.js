require('dotenv').config()

function prependIpfs(ipfsUrl) {
    if (typeof ipfsUrl === "string" && !ipfsUrl.startsWith("ipfs://") && !ipfsUrl.startsWith("https://") && !ipfsUrl.startsWith("http://")) {
        return "ipfs://" + ipfsUrl;
    } else {
        return ipfsUrl;
    }
}

function convertIpfstoHttp(ipfsUrl) {
    if (typeof ipfsUrl === "string") {
        let temp = ipfsUrl
            .replace("ipfs://ipfs/", "https://ipfs.moonbeans.io/ipfs/")
            .replace("ipfs://", "https://ipfs.moonbeans.io/ipfs/")
            .replace("https://ipfs.io/ipfs/", "https://ipfs.moonbeans.io/ipfs/")
            .replace('https://gateway.pinata.cloud/ipfs/', 'https://ipfs.moonbeans.io/ipfs/')
            .replace('https://moonbeans.mypinata.cloud/ipfs/', 'https://ipfs.moonbeans.io/ipfs/')

        if (temp.includes('ipfs.moonbeans.io') && !temp.includes('pinataGatewayToken')) return temp.concat(`?pinataGatewayToken=${process.env.PINATA_TOKEN}`)
        else return temp;
    } else {
        return ipfsUrl;
    }
}

function addJustCors(ipfsUrl) {
    return `https://justcors.com/${process.env.JUSTCORS_TOKEN}/` + ipfsUrl
}



module.exports = { prependIpfs, convertIpfstoHttp, addJustCors };