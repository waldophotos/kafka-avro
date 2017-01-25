/**
 * @fileOverview Wrapper for node-rdkafka Producer Ctor, a mixin.
 */
var Promise = require('bluebird');
var cip = require('cip');
var kafka = require('node-rdkafka');

/**
 * Wrapper for node-rdkafka Produce Ctor, a mixin.
 *
 * @constructor
 */
var Producer = module.exports = cip.extend();

/**
 * The wrapper of the node-rdkafka package Producer Ctor.
 *
 * @param {Object} opts Producer general options.
 * @see https://github.com/edenhill/librdkafka/blob/2213fb29f98a7a73f22da21ef85e0783f6fd67c4/CONFIGURATION.md
 * @return {Promise(kafka.Producer)} A Promise.
 */
Producer.prototype.getProducer = Promise.method(function (opts) {
  if (!opts) {
    opts = {};
  }

  if (!opts['metadata.broker.list']) {
    opts['metadata.broker.list'] = this.kafkaBrokerUrl;
  }
  console.log('KafkaAvro :: Starting producer with options:', opts);
  var producer = new kafka.Producer(opts);

  // hack node-rdkafka
  producer.__kafkaAvro_produce = producer.produce;
  producer.produce = this._produceWrapper.bind(this, producer);

  producer.connect();

  return new Promise(function(resolve) {
    producer.on('ready', resolve);
  })
    .return(producer);
});

/**
 * The node-rdkafka produce method wrapper, will validate and serialize
 * the message against the existing schemas.
 *
 * @param {kafka.Producer} producerInstance node-rdkafka instance.
 * @param {string} topic Topic to produce on.
 * @param {kafka.Producer.Topic} kafkaTopic node-rdkafka Topic instance.
 * @param {number} partition The partition to produce on.
 * @param {Object} value The message.
 * @param {string|number} key The partioning key.
 */
Producer.prototype._produceWrapper = function (producerInstance, topic, kafkaTopic,
  partition, value, key) {

  if (!this.valueSchemas[topic]) {
    // topic not found in schemas, bail early
    console.log('KafkaAvro :: Warning, did not find topic on SR:', topic);
    var bufVal = new Buffer(JSON.stringify(value));
    return producerInstance.__kafkaAvro_produce(topic, partition, bufVal, key);
  }

  var bufValue = this.valueSchemas[topic].toBuffer(value);
  return producerInstance.__kafkaAvro_produce(topic, partition, bufValue, key);
};
