/**
 * @fileOverview Test produce and consume messages using kafka-avro.
 */
var chai = require('chai');
var expect = chai.expect;

var KafkaAvro = require('../..');
var testLib = require('../lib/test.lib');

describe('Produce', function() {
  testLib.init();

  beforeEach(function() {
    this.kafkaAvro = new KafkaAvro({
      kafkaBroker: testLib.KAFKA_BROKER_URL,
      schemaRegistry: testLib.KAFKA_SCHEMA_REGISTRY_URL,
    });

    return this.kafkaAvro.init();
  });

  beforeEach(function() {
    this.consumer = new this.kafkaAvro.Consumer({
      'group.id': 'kafka-avro-test',
      'socket.keepalive.enable': true,
      'enable.auto.commit': true,
    });
  });

  beforeEach(function() {
    this.producer = new this.kafkaAvro.Producer();
  });

  it.only('should produce and consume a message', function(done) {
    var message = {
      name: 'Thanasis',
      long: 540,
    };

    var stream = this.consumer.getReadStream(testLib.topic, {
      waitInterval: 0
    });

    stream.on('error', function(err) {
      console.error('FATAL Stream error:', err);
      done(err);
    });

    this.consumer.on('error', function(err) {
      console.error('Consumer Error:', err);
    });

    stream
      .pipe((data) => {
        console.log('GOT:', data);
        expect(data).to.deep.equal(message);
        this.consumer.disconnect();
        done();
      });

    // Produce message
    this.producer.produce(testLib.topic, -1, message, 'key');
  });
});
