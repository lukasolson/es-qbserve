const {resolve} = require('path');
const {readdir, watch} = require('fs');
const enqueue = require('../es-queue')({host: 'elastic:changeme@localhost:9200'});

const index = 'qbserve';
const type = 'log';
const dataFolder = '/Users/lukas/Documents/data/productivity';

readdir(dataFolder, (err, filenames) => {
  filenames.reverse().forEach(indexFile);
});

watch(dataFolder, (eventType, filename) => {
  indexFile(filename);
});

function indexFile(filename, attempts = 0) {
  try {
    const {history} = require(resolve(dataFolder, filename));
    const {log, activities, apps, categories} = history;
    if (!log.length) return;

    const docs = log.map(entry => {
      const activity = activities[entry.activity_id];
      const app = apps[activity.app_id];
      const category = categories[activity.category_id];
      const timestamp = new Date(entry.start_time * 1000).toISOString();
      return {...entry, activity, app, category, '@timestamp': timestamp};
    });

    enqueue(`${index}-${docs[0]['@timestamp'].substr(0, 10)}`, type, docs);
  } catch (e) {
    console.log(e);
    console.log(`Failed, retrying for the ${++attempts} time...`);
    setTimeout(() => indexFile(filename, attempts), 1000);
  }
}
