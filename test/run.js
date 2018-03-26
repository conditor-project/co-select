'use strict';

const
  fs = require('fs'),
  rewire = require('rewire'),
  pkg = require('../package.json'),
  business = rewire('../index.js'),
  testData = require('./dataset/in/test.json'),
  badData = require('./dataset/in/badDocs.json'),
  baseRequest = require('co-config/base_request.json'),
  chai = require('chai'),
  expect = chai.expect,
  _ = require('lodash'),
  es = require('elasticsearch');

var esConf = require('co-config/es.js');
//esConf.index = 'tests-deduplicate';
//business.__set__('esConf.index','tests-deduplicate');

const esClient = new es.Client({
  host: esConf.host,
  log: {
    type: 'file',
    level: ['error']
  }
});


//fonction de vérification et suppression de l'index pour les tests
let checkIndex = function (cbCheck) {
  esClient.indices.exists({index: esConf.index}, function (errorExists, exists) {
    if (errorExists) {
      console.error(`Problème dans la vérification de l'index ${esConf.index}\n${errorExists.message}`);
      process.exit(1);
    }
    if (!exists) {return cbCheck()};
  });
};


describe(pkg.name + '/index.js', function () {

  this.timeout(10000);

  // Méthde d'initialisation s'exécutant en tout premier
  before(function (done) {

    checkIndex(function (errCheck) {

      if (errCheck) {
        console.log('Erreur checkAndDelete() : ' + errCheck.errMessage);
        process.exit(1);
      }

      business.beforeAnyJob(function (errBeforeAnyJob) {
        if (errBeforeAnyJob) {
          console.log('Erreur beforeAnyJob(), code ' + errBeforeAnyJob.errCode);
          console.log(errBeforeAnyJob.errMessage);
          process.exit(1);
        }
        console.log('before OK');
        done();
      });

    });

  });

});

  

// Méthde finale sensée faire du nettoyage après les tests
  
  after(function (done) {
    esClient.indices.delete({index: esConf.index}).then(
      function () {
        console.log('nettoyage index de test OK');
        done();
      });
    done();
  });


});
