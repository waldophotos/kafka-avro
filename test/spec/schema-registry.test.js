/**
 * @fileOverview Test initialization of the KafkaAvro lib, SR related.
 */
var chai = require('chai');
var expect = chai.expect;

var KafkaAvro = require('../..');

var srUrl = 'http://schema-registry-confluent.internal.dev.waldo.photos';

describe('Initialization of RS', function() {
  it('should initialize properly', function() {
    var kafkaAvro = new KafkaAvro({
      schemaRegistry: srUrl,
    });

    return kafkaAvro.init()
      .map((res) => {
        expect(res).to.have.keys([
          'schemaType',
          'topic',
          'schemaRaw',
          'schemaTopicRaw',
          'type',
        ]);
      })
      .then((all) => {
        expect(all).to.have.length.of.at.least(1);
      });
  });
});
