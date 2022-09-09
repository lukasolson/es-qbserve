const {Client} = require('@elastic/elasticsearch');
const client = new Client({
    node: 'http://elastic:changeme@localhost:9200'
    // node: 'https://elastic:DPfsF4ZQTE4EQVcvVRPpZdJ6@askullsoon.es.eastus2.staging.azure.foundit.no:9243'
});

function getSession() {
    const now = new Date().toISOString();
    const sessionId = Math.random();
    const index = '.kibana_8.0.0_001'; // '.kibana_7.12.0_001'
    const id = `search-session:${sessionId}`;
    const body = {
        "search-session": {
            sessionId,
            "status": "in_progress",
            "expires": new Date(Date.now() + 7 * 24 * 60 * 60 * 1000),
            "created": now,
            "touched": now,
            "idMapping": getIdMapping(),
            "persisted": false,
            "realmType": "file",
            "realmName": "reserved",
            "username": "elastic"
        },
        "type" : "search-session",
        "references" : [],
        "coreMigrationVersion" : "7.13.0", // '7.12.0'
        "updated_at" : now
    };
    return { index, body, id };
}

function getIdMapping(size = 1) {
    return Array(size).fill(null).reduce(acc => {
        return {
            ...acc,
            [`${Math.random()}`]: {
                "id": Math.random(),
                "strategy": "ese",
                "status": "in_progress"
            }
        }
    }, {});
}

const docs = Array(10001).fill(null).map(getSession);
bulkInsert(docs);

async function bulkInsert(docs) {
    if (docs.length <= 0) return;
    const batch = docs.splice(0, 5000);
    console.log(`Indexing ${batch.length} docs...`);

    const body = batch.reduce((actions, {index, body, id}) => actions.concat([
        {index: {_index: index, _id: id }},
        body
    ]), []);

    try {
        await client.bulk({body});
    } catch (e) {
        console.log(e);
    }

    if (docs.length) bulkInsert(docs);
}
