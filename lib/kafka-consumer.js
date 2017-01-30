/**
 * @fileOverview Wrapper for node-rdkafka Consumer Ctor, a mixin.
 */
var Transform = require('stream').Transform;

var Promise = require('bluebird');
var cip = require('cip');
var kafka = require('node-rdkafka');

var magicByte = require('./magic-byte');

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
 * @return {Promise(kafka.Consumer)} A Promise with the consumer.
 */
Consumer.prototype.getConsumer = Promise.method(function (opts) {
  if (!opts['metadata.broker.list']) {
    opts['metadata.broker.list'] = this.kafkaBrokerUrl;
  }

  console.log('KafkaAvro :: Starting Consumer with opts:', opts);

  var consumer = new kafka.KafkaConsumer(opts);

  this._consumers.push(consumer);

  consumer.on('event.log', function(log) {
    console.log('node-rdkafka log:', log);
  });

  // hack node-rdkafka
  consumer.__kafkaAvro_getReadStream = consumer.getReadStream;
  consumer.getReadStream = this._getReadStreamWrapper.bind(this, consumer);

  consumer.__kafkaAvro_on = consumer.on;
  consumer.on = this._onWrapper.bind(this, consumer);

  return new Promise(function(resolve) {
    consumer.on('ready', function() {
      resolve(consumer);
    });

    consumer.connect();
  });
});

/**
 * The node-rdkafka getReadStream method wrapper, will deserialize
 * the incoming message using the existing schemas.
 *
 * @param {kafka.KafkaConsumer} consumerInstance node-rdkafka instance.
 * @param {string} topic Topic to consume on.
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

  stream.on('error', function(err) {
    console.error('KafkaAvro.Consumer :: Read Stream Error:', err);
  });

  return stream
    .pipe(new Transform({
      objectMode: true,
      transform: this._transformAvro.bind(this, topic),
    }));
};

/**
 * The node-rdkafka on method wrapper, will intercept "data" events and
 * deserialize the incoming message using the existing schemas.
 *
 * @param {kafka.KafkaConsumer} consumerInstance node-rdkafka instance.
 * @param {string} eventName the name to listen for events on.
 * @param {Function} cb Event callback.
 * @private
 */
Consumer.prototype._onWrapper = function (consumerInstance, eventName, cb) {
  if (eventName !== 'data') {
    return consumerInstance.__kafkaAvro_on(eventName, cb);
  }

  consumerInstance.__kafkaAvro_on('data', function(message) {
    if (!this.valueSchemas[message.topic]) {
      console.log('KafkaAvro :: Warning, consumer did not find topic on SR:',
        message.topic);
      message.parsed = JSON.parse(message.toString('utf-8'));

      cb(message);
      return;
    }

    var type = this.valueSchemas[message.topic];
    if (this.hasMagicByte) {
      message.parsed = magicByte.fromMessageBuffer(type, message.value).value;
    } else {
      message.parsed = type.fromBuffer(message.value);
    }

    cb(message);
  }.bind(this));
};

/**
 * Callback for Stream Transform, will deserialize the message properly.
 *
 * @param {string} topicName The topic name.
 * @param {*} data The message to encode.
 * @param {string=} encoding Encoding of the message.
 * @param {Function} callback Callback to call when done.
 * @private
 */
Consumer.prototype._transformAvro = function (topicName, data, encoding, callback) {
  if (!this.valueSchemas[topicName]) {
    console.log('KafkaAvro :: Warning, consumer did not find topic on SR:',
      topicName);
    data.parsed = JSON.parse(data.toString('utf-8'));

    callback(null, data);
    return;
  }

  var type = this.valueSchemas[topicName];
  console.log('consuming STREAM topic:', data.topic, 'has byte:', this.hasMagicByte);

  if (this.hasMagicByte) {
    data.parsed = magicByte.fromMessageBuffer(type, data.value).value;
  } else {
    data.parsed = type.fromBuffer(data.value);
  }
  callback(null, data);
};
