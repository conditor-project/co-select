'use strict';
/* eslint-env mocha */
/* eslint-disable no-unused-expressions */
const fse = require('fs-extra');
const path = require('path');
const pkg = require('../package.json');
const coSelect = require('../index.js');
const expect = require('chai').expect;
const elasticsearch = require('elasticsearch');
const fs = require('fs');
const esConf = require('co-config/es.js');
esConf.index = `tests-co-select-${Date.now()}`;
const esMapping = require('co-config/mapping.json');
const readline = require('readline');

describe(pkg.name + '/index.js', function () {
  describe('doTheJob', function () {
    before((done) => {
      const esClient = elasticsearch.Client();
      const instream = fs.createReadStream(path.join(__dirname, 'dataset', 'in', 'doc100.json'));
      const rl = readline.createInterface(instream);
      const docs = [];
      const options = {
        index: {
          _index: esConf.index,
          _type: esConf.type
        }
      };

      rl.on('line', (doc) => {
        docs.push(options);
        const jsonObject = JSON.parse(doc);
        docs.push({ jsonObject });
      });

      rl.on('close', () => {
        esClient.indices.create({ index: esConf.index, body: esMapping })
          .then(() => esClient.bulk({ body: docs }))
          .then(() => done())
          .catch(error => done(error));
      });
    });

    it('should do the job and do it well', function (done) {
      const docObjectInput = {
        elasticReq: '*',
        elasticIndex: 'tests-select'
      };

      coSelect.doTheJob(docObjectInput, (error, docObject) => {
        expect(error).to.be.an('Error');
        expect(error.message).to.be.equal('The first docObject goes into error so as not to pollute the processing chain.');
        expect(docObject).to.include.keys('elasticReq');
        expect(docObject).to.include.keys('elasticIndex');
        done();
      });
    });

    after((done) => {
      const esClient = elasticsearch.Client();
      esClient.indices.delete({ index: esConf.index })
        .then(() => fse.removeSync(path.join(__dirname, '/../out')))
        .then(() => {
          coSelect.finalJob(null, done);
        });
    });
  });

  describe('getWhereIWriteMyFiles', function () {
    it('should return the destination path of files containing docObjects', function () {
      const myFile = '123456-thisIsMyFile.json';
      const myPath = coSelect.getWhereIWriteMyFiles(myFile, 'out');
      const expectedPath = path.join(__dirname, '/../out/TEST_1970-01-01-00-00-00/1/2/3/123456-thisIsMyFile.json');
      expect(myPath).to.equal(path.normalize(expectedPath));
    });
  });
});
