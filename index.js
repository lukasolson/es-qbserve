const {resolve} = require('path');
const {readdir, watch} = require('fs');
const {Client} = require('@elastic/elasticsearch');

const nodes = [
    'http://elastic:changeme@localhost:9200'
];

const clients = nodes.map(node => new Client({node}));
const getClient = () => clients[Math.floor(Math.random() * clients.length)];

const TWO_WEEKS_AGO = parseFloat(`${new Date(Date.now() - 2 * 7 * 24 * 60 * 60 * 1000).getTime()}`.substr(0, 10));
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
  const {log, activities, apps, categories, projects} = history;

  const docs = await Promise.all(log.map(async entry => {
    const activity = activities[entry.activity_id];
    const app = apps[activity.app_id];
    const category = categories[activity.category_id];
    const project = projects[activity.project_id];
    const timestamp = new Date(entry.start_time * 1000).toISOString();
    const index = `qbserve-${timestamp.substring(0, 10)}`;
    const body = {...entry, activity, app, category, project, '@timestamp': timestamp};
    return {index, body};
  }));

  clearTimeout(queuedTimeout);
  queuedDocs = queuedDocs.concat(docs);
  queuedTimeout = setTimeout(() => {
    bulkInsert(queuedDocs);
    queuedDocs = [];
  }, 10);
}

async function bulkInsert(docs, size = 5000) {
  if (docs.length <= 0) return;
  const batch = docs.slice(0, size)
  console.log(`Indexing ${batch.length} docs...`);

  const body = batch.reduce((actions, {index, body, id}) => actions.concat([
    {index: {_index: index, _id: id }},
    body
  ]), []);

  try {
    await getClient().bulk({body});
  } catch (e) {
    if (e.name === 'ConnectionError') {
      // Retry in 5 seconds
      console.log('Connection error, retrying in 5 seconds...');
      return setTimeout(() => bulkInsert(docs, size), 5000);
    } else {
      console.log(e);
    }
  }

  if (docs.length > size) bulkInsert(docs.slice(size), size);
}
