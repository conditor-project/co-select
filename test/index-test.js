'use strict';
/* eslint-env mocha */
/* eslint-disable no-unused-expressions */
const fse = require('fs-extra');
const path = require('path');
const pkg = require('../package.json');
const coSelect = require('../index.js');
const expect = require('chai').expect;
const sinon = require('sinon');
const responseElasticsearch = require('./responseElasticsearch.json');

const stubElasticsearch = sinon.stub(coSelect.elasticsearchStream.client, 'search').callsFake((options, callback) => {
  callback(null, responseElasticsearch);
});
const stubRedis = sinon.stub(coSelect.pubClient, 'hincrby');

describe(pkg.name + '/index.js', function () {
  describe('doTheJob', function () {
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
        expect(stubElasticsearch.called).to.be.true;
        expect(stubRedis.called).to.be.true;
        done();
      });
    });
    after((done) => {
      fse.removeSync(path.join(__dirname, '/../out'));
      coSelect.finalJob(null, done);
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
