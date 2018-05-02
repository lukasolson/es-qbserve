# es-qbserve

Index your Qbserve exports into Elasticsearch (for the pure fun of it).

1. Open Qbserve and click the advanced tab
1. Click the export sub-tab
1. Under scheduled export, make sure to select JSON min, totals and timesheet and click Start
1. Change the value for `dataFolder` in index.js to the path you've selected for Qbserve exports

After you've done this, it's as easy as `node index`. This will read all of the existing files and then watch for additional files.
