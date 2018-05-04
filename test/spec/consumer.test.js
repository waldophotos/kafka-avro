/**
 * @fileOverview Test produce and consume messages using kafka-avro.
 */
var crypto = require('crypto');

var Promise = require('bluebird');
var chai = require('chai');
var expect = chai.expect;

var testLib = require('../lib/test.lib');

function noop () {}

describe('Consume', function() {
  testLib.init();

  beforeEach(function() {
    this.consOpts = {
      'group.id': 'testKafkaAvro' + crypto.randomBytes(20).toString('hex'),
      'enable.auto.commit': true,
    };

    testLib.log.info('beforeEach 1 on Consume');
    return this.kafkaAvro.getConsumer(this.consOpts)
      .bind(this)
      .then(function (consumer) {
        testLib.log.info('beforeEach 1 on Consume: Got consumer');
        this.consumer = consumer;
      });
  });

  beforeEach(function() {
    testLib.log.info('beforeEach 2 on Consume');
    return this.kafkaAvro.getProducer({
      // 'debug': 'all',
      'dr_cb': true,
    })
      .bind(this)
      .then(function (producer) {
        testLib.log.info('beforeEach 2 on Consume: Got producer');
        this.producer = producer;

        producer.on('event.log', function(log) {
          testLib.log.info('producer log:', log);
        });

        //logging all errors
        producer.on('error', function(err) {
          testLib.log.error('Error from producer:', err);
        });

        producer.on('delivery-report', function(err, report) {
          testLib.log.info('delivery-report:' + JSON.stringify(report));
          this.gotReceipt = true;
        }.bind(this));

        testLib.log.info('beforeEach 2 on Consume: Done');

      });
  });

  afterEach(function() {
    testLib.log.info('afterEach 1 on Consume: Disposing...');
    return this.kafkaAvro.dispose()
      .then(function() {
        testLib.log.info('afterEach 1 on Consume: Disposed');
      });
  });

  describe('Consume using Streams', function() {
    it('should produce and consume a message using streams on two topics', function(done) {
      var produceTime = 0;

      var isDone = false;

      var message = {
        name: 'Thanasis',
        long: 540,
      };

      this.kafkaAvro.getConsumerStream(this.consOpts, { 'enable.auto.commit': true }, { topics: [ testLib.topic, testLib.topicTwo ] })
        .then(function (consumerStream) {
          consumerStream.on('error', noop);

          consumerStream.on('data', function(dataRaw) {
            var data = dataRaw.parsed;
            var diff = Date.now() - produceTime;
            testLib.log.info('Produce to consume time in ms:', diff);
            expect(data).to.have.keys([
              'name',
              'long',
            ]);

            expect(data.name).to.equal(message.name);
            expect(data.long).to.equal(message.long);
            if (!isDone) done();
            isDone = true;
        });
      });

      setTimeout(() => {
        produceTime = Date.now();
        this.producer.produce(testLib.topicTwo, -1, message, 'key');
        this.producer.produce(testLib.topic, -1, message, 'key');
      }, 5000);
    });

    it('should produce and consume a message using streams on a not SR topic', function(done) {
      var produceTime = 0;

      var topicName = 'testKafkaAvro' + crypto.randomBytes(20).toString('hex');

      var message = {
        name: 'Thanasis',
        long: 540,
      };

      this.kafkaAvro.getConsumerStream(this.consOpts, { 'enable.auto.commit': true }, { topics: topicName })
        .then(function (consumerStream) {
          consumerStream.on('error', noop);

          consumerStream.on('data', function(dataRaw) {
            var data = dataRaw.parsed;
            var diff = Date.now() - produceTime;
            testLib.log.info('Produce to consume time in ms:', diff);
            expect(data).to.have.keys([
              'name',
              'long',
            ]);

            expect(data.name).to.equal(message.name);
            expect(data.long).to.equal(message.long);

            done();
          });
      });

      setTimeout(() => {
        produceTime = Date.now();
        this.producer.produce(topicName, -1, message, 'key');
      }, 5000);
    });
  });
});
