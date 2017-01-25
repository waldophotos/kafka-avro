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
    return this.kafkaAvro.getConsumer({
      'group.id': 'kafka-avro-test',
      'socket.keepalive.enable': true,
      'enable.auto.commit': true,
    })
      .then((consumer) => {
        this.consumer = consumer;
      });
  });

  beforeEach(function() {
    return this.kafkaAvro.getProducer({
      'dr_cb': true,
    })
      .then((producer) => {
        this.producer = producer;

        producer.on('event.log', function(log) {
          console.log('producer log:', log);
        });

        //logging all errors
        producer.on('error', function(err) {
          console.error('Error from producer:', err);
        });

        producer.on('delivery-report', function(report) {
          console.log('delivery-report:' + JSON.stringify(report));
          this.gotReceipt = true;
        });

        this.producerTopic = producer.Topic(testLib.topic, {
          // Make the Kafka broker acknowledge our message (optional)
          'request.required.acks': 1,
        });
      });
  });

  it.only('should produce and consume a message', function(done) {
    console.log('test start');
    var message = {
      name: 'Thanasis',
      long: 540,
    };

    var stream = this.consumer.getReadStream(testLib.topic, {
      waitInterval: 0
    });
    console.log('stream:', typeof stream.pipe);

    stream.on('error', function(err) {
      console.error('FATAL Stream error:', err);
      done(err);
    });

    this.consumer.on('error', function(err) {
      console.error('Consumer Error:', err);
    });

    // stream
    //   .pipe((data) => {
    //     console.log('GOT:', data);
    //     expect(data).to.deep.equal(message);
    //     this.consumer.disconnect();
    //     done();
    //   });

    stream.on('data', (data) => {
      console.log('GOT:', data);
      expect(data).to.deep.equal(message);
      this.consumer.disconnect();
      done();
    });

    console.log('producing...');
    // Produce message
    this.producer.produce(testLib.topic, this.producerTopic, -1, message, 'key');

    //need to keep polling for a while to ensure the delivery reports are received
    var pollLoop = setInterval(() => {
      this.producer.poll();
      if (this.gotReceipt) {
        clearInterval(pollLoop);
        this.producer.disconnect();
      }
    }, 1000);

  });
});
