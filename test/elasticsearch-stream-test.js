'use strict';
/* eslint-env mocha */
/* eslint-disable no-unused-expressions */
const pkg = require('../package.json');
const ElasticsearchStream = require('../lib/elasticsearch-stream');
const chai = require('chai');
const expect = chai.expect;
const sinon = require('sinon');
const responseElasticsearch = require('./responseElasticsearch.json');

describe(pkg.name + '/lib/elasticsearch-stream.js', function () {
  it('should stream the answers of a search elasticsearch', function (done) {
    const elasticsearchStream = new ElasticsearchStream({
      elasticIndex: 'something-in-the-wind'
    });
    // Wrap the method search in object oiSelect._client
    sinon.stub(elasticsearchStream.client, 'search').callsFake((options, callback) => {
      callback(null, responseElasticsearch);
    });

    elasticsearchStream.on('data', (data) => {
      expect(data).to.include.keys('idConditor');
      expect(data).to.include.keys('typeConditor');
      expect(data).to.include.keys('source');
      expect(data).to.include.keys('isDuplicate');
    });
    elasticsearchStream.on('error', (error) => done(error));
    elasticsearchStream.on('end', () => done());
  });
});
