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
    return this.kafkaAvro.getProducer({
      'dr_cb': true,
    })
      .then(function (producer) {
        this.producer = producer;

        producer.on('event.log', function(log) {
          console.log('producer log:', log);
        });

        //logging all errors
        producer.on('error', function(err) {
          console.error('Error from producer:', err);
        });

        producer.on('delivery-report', function() {
          this.gotReceipt = true;
        }.bind(this));

        this.producerTopic = producer.Topic(testLib.topic, {
          // Make the Kafka broker acknowledge our message (optional)
          'request.required.acks': 1,
        });
      }.bind(this));
  });
  afterEach(function(done) {
    this.producer.disconnect(function(err) {
      done(err);
    });
  });

  it('should produce a message', function(done) {
    var message = {
      name: 'Thanasis',
      long: 540,
    };
    this.producer.produce(this.producerTopic, -1, message, 'key');

    //need to keep polling for a while to ensure the delivery reports are received
    var pollLoop = setInterval(() => {
      this.producer.poll();
      if (this.gotReceipt) {
        clearInterval(pollLoop);
        done();
      }
    }, 1000);
  });
  it('should not allow invalid type', function() {
    var message = {
      name: 'Thanasis',
      long: '540',
    };

    var binded = this.producer.produce.bind(this.producer, this.producerTopic,
      -1, message, 'key');

    expect(binded).to.throw(Error);
  });
  it('should not allow less attributes', function() {
    var message = {
      name: 'Thanasis',
    };

    var binded = this.producer.produce.bind(this.producer, this.producerTopic, -1, message, 'key');

    expect(binded).to.throw(Error);
  });

});
