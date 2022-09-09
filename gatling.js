const {resolve} = require('path');
const {readdir, readFile, lstatSync} = require('fs');
const {promisify} = require('util');
const {Client} = require('@elastic/elasticsearch');
const client = new Client({
  node: 'http://elastic:changeme@localhost:9200'
});

const asyncReadFile = promisify(readFile);

const dataFolder = '/Users/lukas/Developer/kibana-load-testing/target/gatling';
readdir(dataFolder, (err, files) => {
  const folders = files.filter(file => lstatSync(resolve(dataFolder, file)).isDirectory());
  serially(folders, indexFolder);

  // const logFiles = folders.map(folder => resolve(dataFolder, folder, 'simulation.log'));
  // logFiles.forEach(indexFile);

  // const esMonitoringFiles = folders.map(folder => resolve(dataFolder, folder, 'monitoring/es.json'));
  // esMonitoringFiles.forEach(indexMonitoringFile.bind(this, 'monitoring-es'));
  //
  // const kibanaMonitoringFiles = folders.map(folder => resolve(dataFolder, folder, 'monitoring/es.json'));
  // kibanaMonitoringFiles.forEach(indexMonitoringFile.bind(this, 'monitoring-kibana'));
});

async function indexFolder(folder) {
  const logFile = resolve(dataFolder, folder, 'simulation.log');
  const logFileData = await asyncReadFile(logFile, 'utf8');
  const [header, ...logs] = logFileData.split('\n').map(line => line.split('\t'));
  const [, scenario,, timestampMs] = header;
  const timestamp = new Date(parseFloat(timestampMs)).toISOString();

  // Ingest request logs
  const requestLogs = logs.filter(([type]) => type === 'REQUEST');
  const entries = requestLogs.map((log) => {
    const [,, name, start, end, status] = log;
    const requestSendStartTime = new Date(parseFloat(start)).toISOString();
    const responseReceiveEndTime = new Date(parseFloat(end)).toISOString();
    const relativeStartTime = new Date(parseFloat(start - timestampMs)).toISOString();
    const relativeEndTime = new Date(parseFloat(end - timestampMs)).toISOString();
    const requestTime = end - start;
    return {
      scenario,
      timestamp,
      name,
      status,
      requestSendStartTime,
      responseReceiveEndTime,
      relativeStartTime,
      relativeEndTime,
      requestTime,
    };
  });
  await bulkInsert(entries.map(body => ({index: 'gatling-request', body})));

  // Ingest user logs
  const userLogs = logs.filter(([type]) => type === 'USER');
  const userEntries = userLogs.map((log) => {
    const [,, event, time] = log;
    const eventTime = new Date(parseFloat(time)).toISOString();
    const relativeEventTime = new Date(time - timestampMs).toISOString();
    const counter = event === 'START' ? 1 : -1;
    return {
      scenario,
      timestamp,
      event,
      counter,
      eventTime,
      relativeEventTime
    };
  });
  await bulkInsert(userEntries.map(body => ({index: 'gatling-user', body})));

  await indexMonitoringFile(resolve(dataFolder, folder, 'monitoring/es.json'), scenario, timestampMs, 'monitoring-es');
  await indexMonitoringFile(resolve(dataFolder, folder, 'monitoring/kibana.json'), scenario, timestampMs, 'monitoring-kibana');
}

function indexMonitoringFile(file, scenario, startTime, index) {
  const response = require(file);
  const entries = response.hits.hits.map(hit => {
    const timestamp = new Date(hit._source.timestamp);
    const relativeTimestamp = new Date(timestamp - startTime).toISOString();
    return { ...hit._source, scenario, timestamp: relativeTimestamp, startTime };
  });
  const docs = entries.map(body => ({index, body}));
  return bulkInsert(docs);
}

function serially(array, fn) {
  return array.reduce((promise, value, i) => {
    return promise.then(() => fn(value, i));
  }, Promise.resolve());
}

async function bulkInsert(docs) {
  if (docs.length <= 0) return;
  const batch = docs.splice(0, 5000);
  console.log(`Indexing ${batch.length} docs...`);

  const body = batch.reduce((actions, {index, body}) => actions.concat([
    {index: {_index: index }},
    body
  ]), []);

  try {
    await client.bulk({body});
  } catch (e) {
    console.log(e);
  }

  return bulkInsert(docs);
}
