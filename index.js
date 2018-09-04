'use strict';

const _ = require('lodash');
const path = require('path');
const fse = require('fs-extra');
const uuid = require('uuid');
const async = require('async');
const Promise = require('bluebird');
const Redis = require('ioredis');
const es = require('elasticsearch');
const esConf = require('co-config/es.js');
const ElasticSearchScrollStream = require('elasticsearch-scroll-stream');

class CoSelect {
  constructor () {
    this.redisHost = process.env.REDIS_HOST || 'localhost';
    this.redisPort = process.env.REDIS_PORT || 6379;
    this.pubClient = new Redis({
      'host': this.redisHost,
      'port': this.redisPort
    });
    this.redisClient = new Redis({
      'host': this.redisHost,
      'port': this.redisPort
    });
    this.CONDITOR_SESSION = process.env.ISTEX_SESSION || 'TEST_1970-01-01-00-00-00';
    this.MODULEROOT = process.env.MODULEROOT || __dirname;
    this.redisKey = this.CONDITOR_SESSION + ':co-select';
    this.id = 0;
    this.endFlag = false;
    this.drain = false;

    this.esClient = new es.Client({
      host: esConf.host,
      log: {
        type: 'file',
        level: ['error']
      }
    });
  }

  checkIndex (conditorSession, options, indexCallback) {
    let reqParams = {
      index: conditorSession
    };

    let error;

    this.esClient.indices.exists(reqParams, (err, response, status) => {
      if (err) {
        error = new Error('Erreur de communication avec elasticSearch :' + err);
        indexCallback(error);
      } else if (status !== 200) {
        error = new Error('Mapping et index introuvables : ' + status);
        indexCallback(error);
      } else {
        indexCallback();
      }
    });
  }

  disconnect () {
    return Promise.try(() => {
      return this.pubClient.disconnect();
    })
      .then(() => {
        return Promise.try(() => {
          return this.redisClient.disconnect();
        });
      })
      .catch(() => {
        throw (new Error('Erreur de fermeture ioredis.'));
      });
  }

  streamInit (docObject, next) {
    return Promise.try(() => {
      let bloc;
      let esStream;
      let listing = [];
      let doc;

      esStream = new ElasticSearchScrollStream(this.esClient, {
        index: esConf.index,
        type: esConf.type,
        scroll: '100s',
        size: '100',
        q: '*'
      });

      esStream.on('data', (chunk) => {
        let blocContainer = {};
        doc = JSON.parse(chunk);
        listing.push(doc);
        if (listing.length >= 100) {
          bloc = listing.splice(0, 100);
          _.shuffle(bloc);
          blocContainer.bloc = bloc;
          this.blocFormate.push(blocContainer);
        }
      });

      esStream.on('error', (chunk) => {
        let err = new Error('Erreur stderr esStream(co-select): ' + chunk);
        next(err);
      });

      esStream.on('end', (chunk) => {
        let blocContainer = {};
        this.endFlag = true;
        if (chunk) {
          doc = JSON.parse(chunk);
          listing.push(doc);
        }
        if (listing.length > 0) {
          bloc = listing.splice(0, listing.length);
          blocContainer.bloc = bloc;
          this.blocFormate.push(blocContainer);
        }
      });
    });
  }

  doTheJob (docObject, next) {
    this.blocFormate = async.queue(this.sendFlux.bind(this), 8);

    this.blocFormate.drain = () => {
      this.drain = true;
      if (this.endFlag) {
        let error = new Error('Le premier docObject passe en erreur afin de ne pas polluer la chaine.');
        docObject.error = 'Le premier docObject passe en erreur afin de ne pas polluer la chaine.';
        next(error, docObject);
      }
    };

    this.streamInit(docObject, next)
      .catch(function (error) {
        let err = new Error('Erreur de génération du flux : ' + error);
        next(err);
      });
  }

  sendFlux (blocContainer, callback) {
    return Promise.try(() => {
      let fileName = uuid.v4() + '.json';
      let myDocObjectFilePath = this.getWhereIWriteMyFiles(fileName, 'out');
      let directoryOfMyFile = myDocObjectFilePath.substr(0, myDocObjectFilePath.lastIndexOf('/'));

      return fse.ensureDir(directoryOfMyFile)
        .catch(err => {
          console.log(err);
        })
        .then(() => {
          return Promise.try(() => {
            let constructedString = '';
            _.each(blocContainer.bloc, (docObject) => {
              constructedString += JSON.stringify(docObject) + '\n';
            });
            try {
              fse.writeFileSync(myDocObjectFilePath, constructedString);
            } catch (err) {
              callback(new Error('Erreur de flux d\'ecriture : ' + err));
            }
          });
        })
        .catch(err => {
          console.error(err);
        })
        .then(this.sendRedis.bind(this, myDocObjectFilePath, blocContainer, callback));
    });
  }

  sendRedis (myDocObjectFilePath, blocContainer, callback) {
    return Promise.try(() => {
      let pipelineClient = this.redisClient.pipeline();
      let pipelinePublish = this.pubClient.pipeline();
      pipelineClient.hincrby('Module:' + this.redisKey, 'outDocObject', blocContainer.bloc.length)
        .hincrby('Module:' + this.redisKey, 'out', 1).exec();
      pipelinePublish.publish(this.redisKey + ':out', path.basename(myDocObjectFilePath)).exec();
      callback();
    });
  }

  finalJob (docObjects, done) {
    this.disconnect()
      .catch(err => {
        done(err);
      })
      .then(() => {
        done();
      });
  }

  beforeAnyJob (cbBefore) {
    let options = {
      processLogs: [],
      errLogs: []
    };

    let conditorSession = process.env.CONDITOR_SESSION || esConf.index;
    this.checkIndex(conditorSession, options, function (err) {
      options.errLogs.push('callback checkIndex, err=' + err);
      return cbBefore(err, options);
    });
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
