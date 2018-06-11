/**
 * @fileOverview Test produce and consume messages using kafka-avro.
 */
var crypto = require('crypto');

var Promise = require('bluebird');
var chai = require('chai');
var expect = chai.expect;

var testLib = require('../lib/test.lib');

var Avro = require('avsc');
var schemaFixV2 = require('../fixtures/schema-default-value.fix');

function noop () {}

describe('Consume with reader schema with extra field with a default', function() {
  var readerSchemas = {};
  readerSchemas[schemaFixV2.name] = Avro.parse(schemaFixV2);
  testLib.init(readerSchemas);

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

    it('should produce and consume a message using consume "on" without value in new field using default', function(done) {
      var produceTime = 0;

      // message doesn't contain the new field, default value will be deserialized instead
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
          'anotherString'
        ]);
        expect(data.name).to.equal(message.name);
        expect(data.long).to.equal(message.long);
        expect(data.anotherString).to.equal('defaultValue');

        done();
      }.bind(this));

      setTimeout(() => {
        produceTime = Date.now();
        this.producer.produce(testLib.topic, -1, message, 'key');
      }, 10000);
    });

    it('should produce and consume a message using consume "on" overriding default value in new field', function(done) {
      var produceTime = 0;

      // Use the evolved schema with the default value as writer schema for the topic
      testLib.registerSchema(testLib.topic, schemaFixV2).then(
        () => {

          var message = {
            name: 'Thanasis',
            long: 540,
            anotherString: 'not-your-default' // field whose default will be ignored
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
              'anotherString'
            ]);
            expect(data.name).to.equal(message.name);
            expect(data.long).to.equal(message.long);
            expect(data.anotherString).to.equal('not-your-default');

            done();
          }.bind(this));

          setTimeout(() => {
            produceTime = Date.now();
            this.producer.produce(testLib.topic, -1, message, 'key');
          }, 10000);
      });
    });
  });
});
