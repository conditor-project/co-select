'use strict';

const ElasticsearchStream = require('./lib/elasticsearch-stream');
const redis = require('redis');
const elasticsearch = require('elasticsearch');
const path = require('path');
const fs = require('fs');
const fse = require('fs-extra');
const uuid = require('uuid');
const esConf = require('co-config/es.js');

class CoSelect {
  constructor () {
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
    this.elasticseachClient = new elasticsearch.Client({ host: elasticUrl });
  }

  beforeAnyJob (next) {
    const conditorSession = process.env.CONDITOR_SESSION || esConf.index;
    this.elasticseachClient.indices.exists({ index: conditorSession }).then((exist) => {
      if (!exist) return next(new Error(`index ${conditorSession} doesn't exist`));
      next();
    }).catch(error => {
      next(error);
    });
  }

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
  }

  finalJob (docObjects, done) {
    this.pubClient.quit();
    done();
  }

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
}

module.exports = new CoSelect();
