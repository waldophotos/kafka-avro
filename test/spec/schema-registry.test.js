/**
 * @fileOverview Test initialization of the KafkaAvro lib, SR related.
 */
var chai = require('chai');
var expect = chai.expect;

const testLib = require('../lib/test.lib');
var KafkaAvro = require('../..');

// var srUrl = 'http://schema-registry-confluent.internal.dev.waldo.photos';
var srUrl = 'http://localhost:8081';

describe('Initialization of SR', function() {
  testLib.init();
  it('should initialize properly', function() {
    var kafkaAvro = new KafkaAvro({
      schemaRegistry: srUrl,
    });

    return kafkaAvro.init()
      .map((res) => {
        expect(res).to.have.keys([
          'responseRaw',
          'schemaType',
          'topic',
          'schemaRaw',
          'schemaTopicRaw',
          'type',
        ]);

        expect(res.responseRaw).to.have.keys([
          'subject',
          'version',
          'id',
          'schema',
        ]);
      })
      .then((all) => {
        expect(all).to.have.length.of.at.least(1);
      });
  });
  it('kafkaAvro instance should contain expected values after init', function() {
    var kafkaAvro = new KafkaAvro({
      schemaRegistry: srUrl,
    });

    return kafkaAvro.init()
      .map((res) => {
        if (res.schemaType.toLowerCase() === 'value') {
          expect(kafkaAvro.valueSchemas[res.topic]).to.be.an('object');
          expect(kafkaAvro.schemaMeta[res.topic]).to.be.an('object');

          expect(kafkaAvro.schemaMeta[res.topic]).to.have.keys([
            'subject',
            'version',
            'id',
            'schema',
          ]);

        } else {
          expect(kafkaAvro.keySchemas[res.topic]).to.be.an('object');
        }
      })
      .then((all) => {
        expect(all).to.have.length.of.at.least(1);
      });
  });

});
