/**
 * @fileOverview Test initialization of the KafkaAvro lib, SR related.
 */
const chai = require('chai');
const expect = chai.expect;

const testLib = require('../lib/test.lib');
const SchemaRegistry = require('../../lib/schema-registry');

const srUrl = 'http://localhost:8081';

describe('Initialization of SR', function() {
  testLib.init();
  it('should initialize properly', function() {
    const sr = new SchemaRegistry({schemaRegistryUrl: srUrl});

    return sr.init()
      .map((res) => {
        expect(res).to.have.keys([
          'version',
          'responseRaw',
          'schemaType',
          'topic',
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
  it('SR instance should contain expected values after init', function() {
    const sr = new SchemaRegistry({schemaRegistryUrl: srUrl});

    return sr.init()
      .map((res) => {
        if (res.schemaType.toLowerCase() === 'value') {
          expect(sr.schemaTypeById['schema-' + res.responseRaw.id]).to.be.an('object');
          expect(sr.valueSchemas[res.topic]).to.be.an('object');
        } else {
          expect(sr.keySchemas[res.topic]).to.be.an('object');
        }
        expect(sr.schemaMeta[res.topic]).to.be.an('object');
        expect(sr.schemaMeta[res.topic]).to.have.keys([
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

});
