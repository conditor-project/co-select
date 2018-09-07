'use strict';

const Readable = require('stream').Readable;
const elasticsearch = require('elasticsearch');

class ElasticsearchStream extends Readable {
  constructor (options) {
    super({objectMode: true});
    options = options || {};
    this.reading = false;
    this.counter = 0;
    this.elasticUrl = options.elasticUrl || 'localhost:9200';
    this.elasticIndex = options.elasticIndex || '*';
    this.elasticReq = options.elasticReq || '*';
    this.client = new elasticsearch.Client({
      host: this.elasticUrl
    });
  }

  _read () {
    if (this.reading) return false;
    this.reading = true;
    const self = this;
    this.client.search({
      scroll: '10m',
      index: this.elasticIndex,
      q: this.elasticReq
    }, function getMoreUntilDone (error, response) {
      if (error) return self.emit('error', error);
      if (response.hasOwnProperty('hits') && response.hits.hasOwnProperty('hits')) {
        response.hits.hits.forEach((hit) => {
          self.counter++;
          const objToSend = hit._source.jsonObject;
          self.push(objToSend);
        });
        if (self.counter < response.hits.total) {
          self.client.scroll({
            scrollId: response._scroll_id,
            scroll: '10m'
          }, getMoreUntilDone);
        } else {
          self.push(null);
          self.counter = 0;
          self.reading = false;
        }
      }
    });
  }
}

module.exports = ElasticsearchStream;
