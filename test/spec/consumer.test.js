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

  describe('Consumer direct "on"', function() {

    beforeEach(function() {
      return new Promise(function (resolve, reject) {
        this.consumer.on('ready', function() {
          testLib.log.debug('getConsumer() :: Got "ready" event.');
          resolve();
        });

        this.consumer.connect({}, function(err) {
          if (err) {
            testLib.log.error('getConsumer() :: Connect failed:', err);
            reject(err);
            return;
          }
          testLib.log.debug('getConsumer() :: Got "connect()" callback.');
          resolve(); // depend on Promises' single resolve contract.
        });
      }.bind(this));
    });

    it('should produce and consume a message using consume "on"', function(done) {
      var produceTime = 0;

      var message = {
        name: 'Thanasis',
        long: 540,
      };

      var key = 'test-key';

      // //start consuming messages
      this.consumer.subscribe([testLib.topic]);
      this.consumer.consume();

      this.consumer.on('data', function(rawData) {
        var dataValue = rawData.parsed;
        var dataKey = rawData.parsedKey;
        var diff = Date.now() - produceTime;
        testLib.log.info('Produce to consume time in ms:', diff);
        expect(dataValue).to.have.keys([
          'name',
          'long',
        ]);
        expect(dataValue.name).to.equal(message.name);
        expect(dataValue.long).to.equal(message.long);

        expect(dataKey).to.equal(key);

        done();
      }.bind(this));

      setTimeout(() => {
        produceTime = Date.now();
        this.producer.produce(testLib.topic, -1, message, key);
      }, 10000);
    });

    it('should produce and consume a message using consume "on" with timestamp when provided', function(done) {
      var produceTime = Date.parse('04 Dec 2015 00:12:00 GMT'); //use date in the past to guarantee we don't get Date.now()

      var message = {
        name: 'Thanasis',
        long: 540,
      };

      // //start consuming messages
      this.consumer.subscribe([testLib.topic]);
      this.consumer.consume();

      this.consumer.on('data', function(rawData) {
        expect(rawData.timestamp).to.equal(produceTime);
        done();
      }.bind(this));

      setTimeout(() => {
        produceTime = Date.now();
        this.producer.produce(testLib.topic, -1, message, 'key', produceTime);
      }, 10000);
    });

    it('should produce and consume a message using consume "on", on a non Schema Registry topic', function(done) {
      var produceTime = 0;

      var topicName = 'testKafkaAvro' + crypto.randomBytes(20).toString('hex');

      var message = {
        name: 'Thanasis',
        long: 540,
      };

      var key = 'no-schema-key';

      // //start consuming messages
      this.consumer.subscribe([topicName]);
      this.consumer.consume();

      this.consumer.on('data', function(rawData) {
        var dataValue = rawData.parsed;
        var dataKey = rawData.parsedKey;
        var diff = Date.now() - produceTime;
        testLib.log.info('Produce to consume time in ms:', diff);
        expect(dataValue).to.have.keys([
          'name',
          'long',
        ]);
        expect(dataValue.name).to.equal(message.name);
        expect(dataValue.long).to.equal(message.long);

        expect(dataKey).to.equal(key);

        done();
      }.bind(this));

      setTimeout(() => {
        testLib.log.info('Producing on non SR topic...');
        produceTime = Date.now();
        this.producer.produce(topicName, -1, message, key);
      }, 10000);
    });

    it('should produce and consume on two topics using a single consumer', function(done) {
      var produceTime = 0;

      var message = {
        name: 'Thanasis',
        long: 540,
      };

      var key = 'two-topics';

      // //start consuming messages
      this.consumer.subscribe([
        testLib.topic,
        testLib.topicTwo,
      ]);
      this.consumer.consume();

      var receivedOne = false;
      var receivedTwo = false;

      this.consumer.on('data', function(rawData) {
        if (rawData.topic === testLib.topic) {
          receivedOne = true;
        } else {
          receivedTwo = true;
        }

        var dataValue = rawData.parsed;
        var dataKey = rawData.parsedKey;
        var diff = Date.now() - produceTime;
        testLib.log.info('Produce to consume time in ms:', diff);
        expect(dataValue).to.have.keys([
          'name',
          'long',
        ]);
        expect(dataValue.name).to.equal(message.name);
        expect(dataValue.long).to.equal(message.long);
        expect(dataKey).to.equal(key);

        if (receivedOne && receivedTwo) {
          done();
        }
      }.bind(this));

      setTimeout(() => {
        produceTime = Date.now();
        this.producer.produce(testLib.topicTwo, -1, message, key);
        this.producer.produce(testLib.topic, -1, message, key);
      }, 10000);
    });
  });

  describe('Consume using Streams', function() {
    it('should produce and consume a message using streams on two topics', function(done) {
      var produceTime = 0;

      var message = {
        name: 'Thanasis',
        long: 540,
      };

      var key = 'key-stream';

      var isDone = false;

      this.kafkaAvro.getConsumerStream(this.consOpts, { 'enable.auto.commit': true }, { topics: [ testLib.topic, testLib.topicTwo ] })
      .then(function (consumerStream) {
        consumerStream.on('error', noop);

        consumerStream.on('data', function(dataRaw) {
          var dataValue = dataRaw.parsed;
          var dataKey = dataRaw.parsedKey;
          var diff = Date.now() - produceTime;
          testLib.log.info('Produce to consume time in ms:', diff);
          expect(dataValue).to.have.keys([
            'name',
            'long',
          ]);

          expect(dataValue.name).to.equal(message.name);
          expect(dataValue.long).to.equal(message.long);
          expect(dataKey).to.equal(key);

          if (!isDone) {
            consumerStream.consumer.disconnect();
            done();
          }
          isDone = true;
        });
      });

      setTimeout(() => {
        produceTime = Date.now();
        this.producer.produce(testLib.topicTwo, -1, message, key);
        this.producer.produce(testLib.topic, -1, message, key);
      }, 10000);
    });

    it('should produce and consume a message using streams on a not SR topic', function(done) {
      var produceTime = 0;

      var topicName = 'testKafkaAvro' + crypto.randomBytes(20).toString('hex');

      var message = {
        name: 'Thanasis',
        long: 540,
      };

      var key = 'not-sr-key';

      this.kafkaAvro.getConsumerStream(this.consOpts, { 'enable.auto.commit': true }, { topics: topicName })
        .then(function (consumerStream) {
          consumerStream.on('error', noop);

          consumerStream.on('data', function(dataRaw) {
            var dataValue = dataRaw.parsed;
            var dataKey = dataRaw.parsedKey;
            var diff = Date.now() - produceTime;
            testLib.log.info('Produce to consume time in ms:', diff);
            expect(dataValue).to.have.keys([
              'name',
              'long',
            ]);

            expect(dataValue.name).to.equal(message.name);
            expect(dataValue.long).to.equal(message.long);

            expect(dataKey).to.equal(key);

            consumerStream.consumer.disconnect();
            done();
          });
        });

      setTimeout(() => {
        produceTime = Date.now();
        this.producer.produce(topicName, -1, message, key);
      }, 10000);
    });
  });
});
