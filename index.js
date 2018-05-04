const {resolve} = require('path');
const {readdir, watch} = require('fs');
const elasticsearch = require('elasticsearch');
const client = new elasticsearch.Client({
  host: 'elastic:changeme@localhost:9200'
});

const index = 'qbserve';
const type = 'log';
const dataFolder = '/Users/lukas/Documents/data/productivity';

readdir(dataFolder, (err, filenames) => {
  filenames.reduce(serially(indexFile), Promise.resolve());
});

watch(dataFolder, (eventType, filename) => {
  indexFile(filename);
});

function indexFile(filename) {
  const {history} = require(resolve(dataFolder, filename));
  const {log, activities, apps, categories} = history;

  const bodies = log.map(entry => {
    const activity = activities[entry.activity_id];
    const app = apps[activity.app_id];
    const category = categories[activity.category_id];
    const timestamp = new Date(entry.start_time * 1000).toISOString();
    return {...entry, activity, app, category, '@timestamp': timestamp};
  });

  const docs = bodies.map(body => ({index, type, body}));
  return docs.reduce(serially(indexDoc), Promise.resolve());
}

function indexDoc(doc) {
  return client.index(doc);
}

function serially(fn) {
  return (accumulator, value) => {
    return accumulator.then(() => fn(value));
  };
}
