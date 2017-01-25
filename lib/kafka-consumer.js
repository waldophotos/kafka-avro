/**
 * @fileOverview Wrapper for node-rdkafka Consumer Ctor, a mixin.
 */
var Transform = require('stream').Transform;

var cip = require('cip');
var kafka = require('node-rdkafka');

/**
 * Wrapper for node-rdkafka Consumer Ctor, a mixin.
 *
 * @constructor
 */
var Consumer = module.exports = cip.extend();

/**
 * The wrapper of the node-rdkafka package Consumer Ctor.
 *
 * @param {Object} opts Consumer general options.
 * @see https://github.com/edenhill/librdkafka/blob/2213fb29f98a7a73f22da21ef85e0783f6fd67c4/CONFIGURATION.md
 */
Consumer.prototype.Consumer = function (opts) {
  if (!opts['metadata.broker.list']) {
    opts['metadata.broker.list'] = this.kafkaBrokerUrl;
  }

  var consumer = new kafka.Consumer(opts);

  // hack node-rdkafka
  consumer.__kafkaAvro_getReadStream = consumer.getReadStream;
  consumer.getReadStream = this._getReadStreamWrapper.bind(this, consumer);

  return consumer;
};

/**
 * The node-rdkafka getReadStream method wrapper, will deserialize
 * the incoming message using the existing schemas.
 *
 * @param {kafka.KafkaConsumer} consumerInstance node-rdkafka instance.
 * @param {string} topic Topic to produce on.
 * @param {Object=} opts Stream options.
 * @return {Stream} A Stream.
 */
Consumer.prototype._getReadStreamWrapper = function (consumerInstance, topic,
  opts) {

  if (!this.valueSchemas[topic]) {
    // topic not found in schemas, bail early
    return consumerInstance.__kafkaAvro_getReadStream(topic, opts);
  }

  var stream = consumerInstance.__kafkaAvro_getReadStream(topic, opts);

  return stream
    .pipe(new Transform({
      objectMode: true,
      transform: function(data, encoding, callback) {
        var value = this.valueSchemas[topic].fromBuffer(data.message);

        // We want to force it to run async
        callback(null, value);
      }
    }));
};
