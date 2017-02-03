/**
 * @fileOverview Base API Surface tests.
 */
const chai = require('chai');
const expect = chai.expect;
const sinon = require('sinon');

const KafkaAvro = require('../..');

const testLib = require('../lib/test.lib');

describe('Base API Surface', function() {
  testLib.init();

  it('should expose expected methods', function(){
    expect(KafkaAvro).to.be.a('function');
  });

  describe('Edge cases', function() {
    beforeEach(function() {
      this.initSpy = sinon.spy(KafkaAvro.prototype, 'init');
    });
    afterEach(function() {
      this.initSpy.restore();
    });
    it('Should instantiate multiple instances', function() {
      let kafkaAvro = new KafkaAvro({
        kafkaBroker: testLib.KAFKA_BROKER_URL,
        schemaRegistry: testLib.KAFKA_SCHEMA_REGISTRY_URL,
      });

      return kafkaAvro.init()
        .bind(this)
        .then(function() {
          kafkaAvro = new KafkaAvro({
            kafkaBroker: testLib.KAFKA_BROKER_URL,
            schemaRegistry: testLib.KAFKA_SCHEMA_REGISTRY_URL,
          });
          return kafkaAvro.init();
        })
        .then(function() {
          expect(this.initSpy.callCount).to.equal(2);
        });
    });
  });
});
