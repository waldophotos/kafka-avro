/**
 * @fileOverview Test produce and consume messages using kafka-avro.
 */
var Transform = require('stream').Transform;

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
    this.consOpts = {
      'metadata.broker.list': 'broker-1.service.consul:9092',
      'group.id': 'kafka-avro-test',
      'socket.keepalive.enable': true,
      'enable.auto.commit': false,
    };
    return this.kafkaAvro.getConsumer(testLib.topic, this.consOpts)
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

  beforeEach(function(done) {
    console.log('POS:', this.consumer.position());
    this.consumer.committed(1000, function(err, committed) {
      console.log('GOT COMMITTED:', committed);
      done();
    });
  });

  it('should produce and consume a message', function(done) {
    var produceTime = 0;

    var message = {
      name: 'Thanasis',
      long: 540,
    };

    var stream = this.consumer.getReadStream(testLib.topic, {
      waitInterval: 0
    });

    stream.on('data', function(data) {
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
