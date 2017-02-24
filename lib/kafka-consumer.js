/**
 * @fileOverview Wrapper for node-rdkafka Consumer Ctor, a mixin.
 */
var Transform = require('stream').Transform;

var Promise = require('bluebird');
var cip = require('cip');
var kafka = require('node-rdkafka');

var magicByte = require('./magic-byte');
var log = require('./log.lib').getChild(__filename);

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
 * @param {Object} topts Topic specific options.
 * @see https://github.com/edenhill/librdkafka/blob/2213fb29f98a7a73f22da21ef85e0783f6fd67c4/CONFIGURATION.md
 * @return {Promise(kafka.Consumer)} A Promise with the consumer.
 */
Consumer.prototype.getConsumer = Promise.method(function (opts, topts) {
  if (!opts['metadata.broker.list']) {
    opts['metadata.broker.list'] = this.kafkaBrokerUrl;
  }

  log.info('getConsumer() :: Starting Consumer with opts:', opts);

  var consumer = new kafka.KafkaConsumer(opts, topts);

  this._consumers.push(consumer);

  consumer.on('disconnect', function(arg) {
    log.warn('getConsumer() :: Consumer disconnected. Args:', arg);
  });

  consumer.on('error', function(err) {
    log.error('getConsumer() :: Consumer Error event fired:', err);
  });

  // hack node-rdkafka
  consumer.__kafkaAvro_getReadStream = consumer.getReadStream;
  consumer.getReadStream = this._getReadStreamWrapper.bind(this, consumer);

  consumer.__kafkaAvro_on = consumer.on;
  consumer.on = this._onWrapper.bind(this, consumer);

  return consumer;
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

  var stream = consumerInstance.__kafkaAvro_getReadStream(topic, opts);

  stream.on('error', function(err) {
    log.error('_getReadStreamWrapper() :: Read Stream Error:', err);
  });

  return stream
    .pipe(new Transform({
      objectMode: true,
      transform: this._transformAvro.bind(this),
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

  return consumerInstance.__kafkaAvro_on('data', function(message) {
    if (!this.sr.valueSchemas[message.topic]) {
      log.warn('_onWrapper() :: Warning, consumer did not find topic on SR:',
        message.topic);

      message.parsed = JSON.parse(message.value.toString('utf-8'));

      cb(message);
      return;
    }

    var type = this.sr.valueSchemas[message.topic];
    var decoded = this.deserialize(type, message);

    if (!decoded) {
      message.parsed = null;
      message.schemaId = null;
    } else {
      message.parsed = decoded.value;
      message.schemaId = decoded.schemaId;
    }

    cb(message);
  }.bind(this));
};

/**
 * Deserialize an avro message.
 *
 * @param {avsc.Type} type Avro type instance.
 * @param {Object} message The raw message.
 * @return {Object} The deserialized object.
 */
Consumer.prototype.deserialize = function (type, message) {
  try {
    var parsed = magicByte.fromMessageBuffer(type, message.value, this.sr);
  } catch(ex) {
    log.warn(`deserialize() :: Error deserializing on topic ${ message.topic }`,
      'Raw value:', message.value, `Partition: ${ message.partition } Offset:`,
      `${ message.offset } Key: ${ message.key } Exception:`, ex);
    return null;
  }

  return parsed;
};

/**
 * Callback for Stream Transform, will deserialize the message properly.
 *
 * @param {*} data The message to decode.
 * @param {string=} encoding Encoding of the message.
 * @param {Function} callback Callback to call when done.
 * @private
 */
Consumer.prototype._transformAvro = function (data, encoding, callback) {
  const topicName = data.topic;

  if (!this.sr.valueSchemas[topicName]) {
    log.warn('_transformAvro() :: Warning, consumer did not find topic on SR:',
      topicName);

    try {
      data.parsed = JSON.parse(data.value.toString('utf-8'));
    } catch(ex) {
      log.warn('_transformAvro() :: Error parsing value:', data.value,
        'Error:', ex);
    }

    callback(null, data);
    return;
  }

  var type = this.sr.valueSchemas[topicName];

  var decoded = this.deserialize(type, data);

  if (!decoded) {
    data.parsed = null;
    data.schemaId = null;
  } else {
    data.parsed = decoded.value;
    data.schemaId = decoded.schemaId;
  }


  callback(null, data);
};
