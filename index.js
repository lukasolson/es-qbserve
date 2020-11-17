const {resolve} = require('path');
const {readdir, watch} = require('fs');
const {Client} = require('@elastic/elasticsearch');
const client = new Client({
  node: 'http://elastic:changeme@localhost:9200'
});

const TWO_WEEKS_AGO = parseFloat(`${new Date(Date.now() - 2 * 7 * 24 * 60 * 60 * 1000).getTime()}`.substr(0, 10));
const dataFolder = '/Users/lukas/Documents/data/productivity';

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

function indexFile(filename) {
  console.log(`reading ${filename}...`)
  const {history} = require(resolve(dataFolder, filename));
  const {log, activities, apps, categories} = history;

  const docs = log.map(entry => {
    const activity = activities[entry.activity_id];
    const app = apps[activity.app_id];
    const category = categories[activity.category_id];
    const timestamp = new Date(entry.start_time * 1000).toISOString();
    const index = `qbserve-${timestamp.substr(0, 10)}`;
    const body = {...entry, activity, app, category, '@timestamp': timestamp};
    return {index, body};
  });

  clearTimeout(queuedTimeout);
  queuedDocs = queuedDocs.concat(docs);
  queuedTimeout = setTimeout(() => {
    bulkInsert(queuedDocs);
    queuedDocs = [];
  }, 10);
}

function bulkInsert(docs) {
  if (docs.length <= 0) return;
  const batch = docs.splice(0, 5000);
  console.log(`Indexing ${batch.length} docs...`);

  const body = batch.reduce((actions, {index, body}) => actions.concat([
    {index: {_index: index }},
    body
  ]), []);

  try {
    client.bulk({body});
  } catch (e) {
    console.log(e);
  }

  if (docs.length) bulkInsert(docs);
}