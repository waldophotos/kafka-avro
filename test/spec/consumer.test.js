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
      // 'debug': 'all',
      'group.id': 'testKafkaAvro' + crypto.randomBytes(20).toString('hex'),
      'enable.auto.commit': true,
      // 'auto.offset.reset': 'earliest',
      // 'session.timeout.ms': 1000,
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

        this.producerTopic = producer.Topic(testLib.topic, {
          // Make the Kafka broker acknowledge our message (optional)
          'request.required.acks': 1,
        });
        this.producerTopicTwo = producer.Topic(testLib.topicTwo, {
          // Make the Kafka broker acknowledge our message (optional)
          'request.required.acks': 1,
        });
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

      // //start consuming messages
      this.consumer.subscribe([testLib.topic]);
      this.consumer.consume();

      this.consumer.on('data', function(rawData) {
        var data = rawData.parsed;
        var diff = Date.now() - produceTime;
        testLib.log.info('Produce to consume time in ms:', diff);
        expect(data).to.have.keys([
          'name',
          'long',
        ]);
        expect(data.name).to.equal(message.name);
        expect(data.long).to.equal(message.long);

        done();
      }.bind(this));

      setTimeout(() => {
        produceTime = Date.now();
        this.producer.produce(this.producerTopic, -1, message, 'key');
      }, 4000);

      // //need to keep polling for a while to ensure the delivery reports are received
      // var pollLoop = setInterval(function () {
      //   this.producer.poll();
      //   if (this.gotReceipt) {
      //     clearInterval(pollLoop);
      //     this.producer.disconnect();
      //   }
      // }.bind(this), 1000);
    });

    it('should produce and consume a message using consume "on", on a non Schema Registry topic', function(done) {
      var produceTime = 0;

      var topicName = 'testKafkaAvro' + crypto.randomBytes(20).toString('hex');
      var producerTopic = this.producer.Topic(topicName, {});

      var message = {
        name: 'Thanasis',
        long: 540,
      };

      // //start consuming messages
      this.consumer.subscribe([topicName]);
      this.consumer.consume();

      this.consumer.on('data', function(rawData) {
        var data = rawData.parsed;
        var diff = Date.now() - produceTime;
        testLib.log.info('Produce to consume time in ms:', diff);
        expect(data).to.have.keys([
          'name',
          'long',
        ]);
        expect(data.name).to.equal(message.name);
        expect(data.long).to.equal(message.long);

        done();
      }.bind(this));

      setTimeout(() => {
        testLib.log.info('Producing on non SR topic...');
        produceTime = Date.now();
        this.producer.produce(producerTopic, -1, message, 'key');
      }, 4000);
    });

    it('should produce and consume on two topics using a single consumer', function(done) {
      var produceTime = 0;

      var message = {
        name: 'Thanasis',
        long: 540,
      };

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

        var data = rawData.parsed;
        var diff = Date.now() - produceTime;
        testLib.log.info('Produce to consume time in ms:', diff);
        expect(data).to.have.keys([
          'name',
          'long',
        ]);
        expect(data.name).to.equal(message.name);
        expect(data.long).to.equal(message.long);

        if (receivedOne && receivedTwo) {
          done();
        }
      }.bind(this));

      setTimeout(() => {
        produceTime = Date.now();
        this.producer.produce(this.producerTopicTwo, -1, message, 'key');
        this.producer.produce(this.producerTopic, -1, message, 'key');
      }, 2000);
    });
  });

  describe('Consume using Streams', function() {
    it('should produce and consume a message using streams', function(done) {
      var produceTime = 0;

      var message = {
        name: 'Thanasis',
        long: 540,
      };

      var stream = this.consumer.getReadStream(testLib.topic, {
        waitInterval: 0
      });
      stream.on('error', noop);

      stream.on('data', function(dataRaw) {
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
      }.bind(this));

      setTimeout(() => {
        produceTime = Date.now();
        this.producer.produce(this.producerTopic, -1, message, 'key');
      }, 2000);
    });
    it('should produce and consume a message using streams on two topics', function(done) {
      var produceTime = 0;

      var message = {
        name: 'Thanasis',
        long: 540,
      };

      var stream = this.consumer.getReadStream([testLib.topic, testLib.topicTwo], {
        waitInterval: 0
      });
      stream.on('error', noop);

      stream.on('data', function(dataRaw) {
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
      }.bind(this));

      setTimeout(() => {
        produceTime = Date.now();
        this.producer.produce(this.producerTopic, -1, message, 'key');
      }, 2000);
    });

    it('should produce and consume a message using streams on a not SR topic', function(done) {
      var produceTime = 0;

      var topicName = 'testKafkaAvro' + crypto.randomBytes(20).toString('hex');
      var producerTopic = this.producer.Topic(topicName, {});

      var message = {
        name: 'Thanasis',
        long: 540,
      };

      var stream = this.consumer.getReadStream(topicName, {
        waitInterval: 0
      });
      stream.on('error', noop);

      stream.on('data', function(dataRaw) {
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
      }.bind(this));

      setTimeout(() => {
        produceTime = Date.now();
        this.producer.produce(producerTopic, -1, message, 'key');
      }, 2000);
    });

  });
});
