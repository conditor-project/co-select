'use strict';

const
  fse = require('fs-extra'),
  rewire = require('rewire'),
  pkg = require('../package.json'),
  business = rewire('../index.js'),
  testData = require('./dataset/in/test.json'),
  badData = require('./dataset/in/badDocs.json'),
  baseRequest = require('co-config/base_request.json'),
  chai = require('chai'),
  expect = chai.expect,
  _ = require('lodash'),
  glob = require('glob'),
  path = require('path'),
  es = require('elasticsearch');

var esConf = require('co-config/es.js');
esConf.index = 'tests-select';
business.__set__('esConf.index','tests-select');
const sessionName = "TEST_1970-01-01-00-00-00";

const esClient = new es.Client({
  host: esConf.host,
  log: {
    type: 'file',
    level: ['error']
  }
});

const outDir = path.join(__dirname, '/..', 'out', sessionName);

//fonction de vérification et suppression de l'index pour les tests
let checkAndDeleteIndex = function (cbCheck) {
  esClient.indices.delete({index: esConf.index}, function (errorExists, exists) {
    if (errorExists && errorExists.status===404) {
      console.log(`l index ${esConf.index} n existe pas et est créé.`);
    }
    esClient.indices.create({index: esConf.index}, function (errorCreate, responseCreate) {
      if (errorCreate) {
        console.error(`Problème dans la creation de l'index ${esConf.index}\n${errorCreate.message}`);
      }
      return cbCheck();
    });
  });
};


let insertTestCorpus = function(done){
  
  let pathCorpus =path.join(__dirname,'dataset','in','doc100.json');
  let jsonObjects;
  let nbDocsFound;
  let jsonObject;
  let body = [];
  let options={index:{_index:esConf.index,_type:esConf.type}};
  let doc;

  jsonObjects = (fse.readFileSync(pathCorpus, {
      encoding: 'utf8'
  }).trim()).split('\n');
  _.compact(jsonObjects);
  nbDocsFound = jsonObjects.length;
  for (let i = 0; i < jsonObjects.length; i++) {
    jsonObject = JSON.parse(jsonObjects[i]);  
    body.push(options);
    body.push({jsonObject,refresh:true});
  }

  esClient.bulk({body:body})
  .catch(err=>{
    if (err) { done(err)}
  })
  .then(()=>{
    done();
  });

}

describe(pkg.name + '/index.js', function () {

  
  // Méthde d'initialisation s'exécutant en tout premier
  before(function (done) {

    fse.mkdirsSync(outDir);

    checkAndDeleteIndex(function (errCheck) {

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
        done();
      });
    });
  });
  

  it("La vérification si l'index existe est bien faite",function(done) {
    
    business.checkIndex(esConf.index,{},(err)=>{
      expect(err).to.be.undefined;
      done();
    });
  });

  it("Mauvais nom d'index génère bien une erreur",function(done){
    business.checkIndex(esConf.index+'s',{},(err)=>{
      expect(err).to.be.not.undefined;
      //console.log(err);
      done();
    });
  });


  it("Insertion du corpus de test",function(done){
    insertTestCorpus((err)=>{
      expect(err).to.be.undefined;
      done();
    });
  });


  it("test de l'extraction et de la génération du flux",(done)=>{
    setTimeout(()=>{
      business.doTheJob({},(err)=>{
        expect(err).to.be.not.undefined;
        done();
      });
    },1000)
  });

  it('devrait générer les docObjects correspondant aux docObjects insérés en base', function(done) {
    const nbExpectedDocs = 97;

    // vérifie qu'en out, les fichiers JSON contenant les docObjects ont bien été générés
    glob(outDir + '/**/*.json', (err, files)=> {
        //if (err) { return done(err);}
        expect(files.length,
            'les ' + nbExpectedDocs + ' documents du jeu de test doivent être dans un seul fichier, non pas ' + files.length).to.equal(1);

        // parcours des fichiers trouvés (fichiers pouvant contenir 100 docs
        let nbDocsFound = 0;
        let jsonObjects;
        let jsonObject;
        files.forEach(function(file) {
            jsonObjects = (fse.readFileSync(file, {
                encoding: 'utf8'
            }).trim()).split('\n');
            nbDocsFound += jsonObjects.length;
        }); //fin forEach

        expect(nbDocsFound,
            'le jeu de test ' + sessionName + ' devrait contenir ' + nbExpectedDocs + ' documents, et non pas ' + nbDocsFound).to.equal(nbExpectedDocs);

        done();

    }); //fin glob
});



  // Méthode finale sensée faire du nettoyage après les tests
    
  after((done)=> {
    
    esClient.indices.delete({index: esConf.index}).then(
      function () {
        console.log('nettoyage index de test OK');
        done();
      }); 
    fse.removeSync(outDir);
    business.disconnect()
    done();
  }); 
});


