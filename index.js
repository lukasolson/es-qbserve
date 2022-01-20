const {resolve} = require('path');
const {readdir, watch} = require('fs');
const {Client} = require('@elastic/elasticsearch');

const nodes = [
    'http://elastic:changeme@localhost:9200'
];

const clients = nodes.map(node => new Client({node}));
const getClient = () => clients[Math.floor(Math.random() * clients.length)];

const TWO_WEEKS_AGO = parseFloat(`${new Date(Date.now() - 24 * 60 * 60 * 1000).getTime()}`.substr(0, 10));
const dataFolder = '/Users/lukas/Documents/Qbserve';

readdir(dataFolder, (err, filenames) => {
  filenames.filter(filename => {
    return parseFloat(filename.substr(0, 10)) >= TWO_WEEKS_AGO;
  }).forEach(indexFile);
});

watch(dataFolder, (eventType, filename) => {
  indexFile(filename);
});

let queuedTimeout = 0;
let queuedDocs = [];

async function indexFile(filename) {
  console.log(`reading ${filename}...`)
  const {history} = require(resolve(dataFolder, filename));
  const {log, activities, apps, categories} = history;

  const docs = await Promise.all(log.map(async entry => {
    const activity = activities[entry.activity_id];
    const app = apps[activity.app_id];
    const category = categories[activity.category_id];
    const timestamp = new Date(entry.start_time * 1000).toISOString();
    const index = `qbserve-${timestamp.substr(0, 10)}`;
    const body = {...entry, activity, app, category, '@timestamp': timestamp};
    return {index, body};
  }));

  clearTimeout(queuedTimeout);
  queuedDocs = queuedDocs.concat(docs);
  queuedTimeout = setTimeout(() => {
    bulkInsert(queuedDocs);
    queuedDocs = [];
  }, 10);
}

async function bulkInsert(docs) {
  if (docs.length <= 0) return;
  const batch = docs.splice(0, 5000);
  console.log(`Indexing ${batch.length} docs...`);

  const body = batch.reduce((actions, {index, body, id}) => actions.concat([
    {index: {_index: index, _id: id }},
    body
  ]), []);

  try {
    await getClient().bulk({body});
  } catch (e) {
    console.log(e);
  }

  if (docs.length) bulkInsert(docs);
}
