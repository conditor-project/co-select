'use strict';

const ElasticsearchStream = require('./lib/elasticsearch-stream');
const redis = require('redis');
const path = require('path');
const fs = require('fs');
const fse = require('fs-extra');
const uuid = require('uuid');
const esConf = require('co-config/es.js');

const coSelect = {
  init () {
    this.redisHost = process.env.REDIS_HOST || 'localhost';
    this.redisPort = process.env.REDIS_PORT || 6379;
    this.pubClient = redis.createClient({
      'host': this.redisHost,
      'port': this.redisPort
    });
    this.CONDITOR_SESSION = process.env.ISTEX_SESSION || 'TEST_1970-01-01-00-00-00';
    this.MODULEROOT = process.env.MODULEROOT || __dirname;
    this.redisKey = this.CONDITOR_SESSION + ':co-select';
    const elasticUrl = esConf.host;
    this.elasticsearchStream = new ElasticsearchStream({ elasticUrl });
    return this;
  },

  doTheJob (docObject, next) {
    this.elasticsearchStream.elasticIndex = docObject.elasticIndex || esConf.index;
    this.elasticsearchStream.elasticReq = docObject.elasticReq || '*';
    let count = 0;
    let myDocObjectFilePath = this.getWhereIWriteMyFiles(uuid.v4() + '.json', 'out');
    let directoryOfMyFile = myDocObjectFilePath.substr(0, myDocObjectFilePath.lastIndexOf('/'));
    fse.ensureDirSync(directoryOfMyFile);
    let writableStream = fs.createWriteStream(myDocObjectFilePath);
    this.elasticsearchStream.on('data', (data) => {
      count++;
      writableStream.write(JSON.stringify(data) + '\n');
      this.pubClient.hincrby('Module:' + this.redisKey, 'outDocObject', 1);
      if (count === 100) {
        count = 0;
        writableStream.end();
        this.pubClient.hincrby('Module:' + this.redisKey, 'out', 1);
        this.pubClient.publish(this.redisKey + ':out', path.basename(myDocObjectFilePath));
        myDocObjectFilePath = this.getWhereIWriteMyFiles(uuid.v4() + '.json', 'out');
        directoryOfMyFile = myDocObjectFilePath.substr(0, myDocObjectFilePath.lastIndexOf('/'));
        fse.ensureDirSync(directoryOfMyFile);
        writableStream = fs.createWriteStream(myDocObjectFilePath);
      }
    });
    this.elasticsearchStream.on('end', () => {
      if (count !== 0) {
        this.pubClient.hincrby('Module:' + this.redisKey, 'out', 1);
        this.pubClient.publish(this.redisKey + ':out', path.basename(myDocObjectFilePath));
      }
      const error = new Error('The first docObject goes into error so as not to pollute the processing chain.');
      next(error, docObject);
    });
    this.elasticsearchStream.on('error', (error) => {
      next(error, docObject);
    });
  },

  finalJob (docObjects, done) {
    this.pubClient.quit();
    done();
  },

  getWhereIWriteMyFiles (file, dirOutOrErr) {
    return path.join(
      this.MODULEROOT,
      dirOutOrErr,
      this.CONDITOR_SESSION,
      file[0],
      file[1],
      file[2],
      file
    );
  }
};

module.exports = coSelect.init();
