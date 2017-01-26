/**
 * @fileOverview Test produce and consume messages using kafka-avro.
 */

var chai = require('chai');
var expect = chai.expect;

var KafkaAvro = require('../..');
var testLib = require('../lib/test.lib');


describe('Consume', function() {
  testLib.init();

  beforeEach(function() {
    this.kafkaAvro = new KafkaAvro({
      kafkaBroker: testLib.KAFKA_BROKER_URL,
      schemaRegistry: testLib.KAFKA_SCHEMA_REGISTRY_URL,
    });

    return this.kafkaAvro.init();
  });

  beforeEach(function() {
    this.consOpts = {
      'group.id': 'testKafkaAvro',
      'enable.auto.commit': true,
      // 'session.timeout.ms': 1000,
    };
    return this.kafkaAvro.getConsumer(testLib.topic, this.consOpts)
      .bind(this)
      .then(function (consumer) {
        this.consumer = consumer;
      });
  });

  beforeEach(function() {
    return this.kafkaAvro.getProducer({
      'dr_cb': true,
    })
      .bind(this)
      .then(function (producer) {
        this.producer = producer;

        producer.on('event.log', function(log) {
          console.log('producer log:', log);
        });

        //logging all errors
        producer.on('error', function(err) {
          console.error('Error from producer:', err);
        });

        producer.on('delivery-report', function(err, report) {
          console.log('delivery-report:' + JSON.stringify(report));
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

      });
  });

  describe('Consumer direct "on"', function() {
    afterEach(function(done) {
      this.consumer.disconnect(function() {
        done();
      });
    });

    it('should produce and consume a message using consume "on"', function(done) {
      var produceTime = 0;

      var message = {
        name: 'Thanasis',
        long: 540,
      };

      // //start consuming messages
      this.consumer.consume([testLib.topic]);

      this.consumer.on('data', function(rawData) {
        var data = rawData.parsed;
        var diff = Date.now() - produceTime;
        console.log('Produce to consume time in ms:', diff);
        expect(data).to.have.keys([
          'name',
          'long',
        ]);
        expect(data.name).to.equal(message.name);
        expect(data.long).to.equal(message.long);

        done();
      }.bind(this));

      produceTime = Date.now();
      this.producer.produce(this.producerTopic, -1, message, 'key');

      //need to keep polling for a while to ensure the delivery reports are received
      var pollLoop = setInterval(function () {
        this.producer.poll();
        if (this.gotReceipt) {
          clearInterval(pollLoop);
          this.producer.disconnect();
        }
      }.bind(this), 1000);
    });

    it('should produce and consume on two topics using a single consumer', function(done) {
      var produceTime = 0;

      var message = {
        name: 'Thanasis',
        long: 540,
      };

      // //start consuming messages
      this.consumer.consume([
        testLib.topic,
        testLib.topicTwo,
      ]);

      var receivedOne = false;
      var receivedTwo = false;

      this.consumer.on('data', function(rawData) {
        console.log('GOT:', rawData);
        if (rawData.topic === testLib.topic) {
          receivedOne = true;
        } else {
          receivedTwo = true;
        }

        var data = rawData.parsed;
        var diff = Date.now() - produceTime;
        console.log('Produce to consume time in ms:', diff);
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

      produceTime = Date.now();
      this.producer.produce(this.producerTopicTwo, -1, message, 'key');
      this.producer.produce(this.producerTopic, -1, message, 'key');

      //need to keep polling for a while to ensure the delivery reports are received
      var pollLoop = setInterval(function () {
        this.producer.poll();
        if (this.gotReceipt) {
          clearInterval(pollLoop);
          this.producer.disconnect();
        }
      }.bind(this), 1000);
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

      stream.on('data', function(dataRaw) {
        var data = dataRaw.parsed;
        var diff = Date.now() - produceTime;
        console.log('Produce to consume time in ms:', diff);
        expect(data).to.have.keys([
          'name',
          'long',
        ]);

        expect(data.name).to.equal(message.name);
        expect(data.long).to.equal(message.long);

        done();
      }.bind(this));

      produceTime = Date.now();
      this.producer.produce(this.producerTopic, -1, message, 'key');

      //need to keep polling for a while to ensure the delivery reports are received
      var pollLoop = setInterval(function () {
        this.producer.poll();
        if (this.gotReceipt) {
          clearInterval(pollLoop);
          this.producer.disconnect();
        }
      }.bind(this), 1000);
    });
  });
});
