/**
 * @fileOverview Wrapper for node-rdkafka Consumer Ctor, a mixin.
 */
const Promise = require('bluebird');
const cip = require('cip');
const kafka = require('node-rdkafka');

const magicByte = require('./magic-byte');
const log = require('./log.lib').getChild(__filename);

/**
 * Wrapper for node-rdkafka Consumer Ctor, a mixin.
 *
 * @constructor
 */
const Consumer = module.exports = cip.extend();

/**
 * The wrapper of the node-rdkafka package Consumer Ctor.
 *
 * @param {Object} opts Consumer general options.
 * @param {Object} topicOpts Topic specific options.
 * @see https://github.com/edenhill/librdkafka/blob/2213fb29f98a7a73f22da21ef85e0783f6fd67c4/CONFIGURATION.md
 * @return {Promise(kafka.Consumer)} A Promise with the consumer.
 */
Consumer.prototype.getConsumer = Promise.method(function (opts, topicOpts) {
  if (!opts['metadata.broker.list']) {
    opts['metadata.broker.list'] = this.kafkaBrokerUrl;
  }

  log.info('getConsumer() :: Starting Consumer with opts', {opts});

  const consumer = new kafka.KafkaConsumer(opts, topicOpts);

  this._consumers.push(consumer);

  consumer.on('disconnect', function (args) {
    log.warn('getConsumer() :: Consumer disconnected. args', {args});
  });

  consumer.on('error', function (err) {
    log.error(err, 'getConsumer() :: Consumer Error event fired');
  });

  // hack node-rdkafka
  consumer.__kafkaAvro_on = consumer.on;
  consumer.on = this._onWrapper.bind(this, consumer);

  return consumer;
});

/**
 * The wrapper of the node-rdkafka package KafkaConsumerStream.
 *
 * @param {Object} opts Consumer general options.
 * @param {Object} topicOpts Topic specific options.
 * @param {Object} streamOpts node-rdkafka ConsumerStream options
 * @see https://github.com/edenhill/librdkafka/blob/2213fb29f98a7a73f22da21ef85e0783f6fd67c4/CONFIGURATION.md
 * @see https://blizzard.github.io/node-rdkafka/current/KafkaConsumerStream.html
 * @return {Promise(kafka.ConsumerStream)} A Promise with the consumer stream.
 */
Consumer.prototype.getConsumerStream = Promise.method(function (opts, topicOpts, streamOpts) {
  if (!opts['metadata.broker.list']) {
    opts['metadata.broker.list'] = this.kafkaBrokerUrl;
  }

  log.info('getConsumerStream() :: Starting Consumer Stream with opts', {opts});

  const consumer = new kafka.KafkaConsumer.createReadStream(opts, topicOpts, streamOpts);

  this._consumersStream.push(consumer);

  consumer.on('disconnect', function (arg) {
    log.warn('getConsumerStream() :: Consumer disconnected. arg', {arg});
  });

  consumer.on('error', function (err) {
    log.error(err, 'getConsumerStream() :: Consumer Error event fired');
  });

  // hack node-rdkafka

  consumer.__kafkaAvro_on = consumer.on;
  consumer.on = this._onWrapper.bind(this, consumer);

  return consumer;
});

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

  return consumerInstance.__kafkaAvro_on('data', function (message) {
    try {
      const keyDecoded = magicByte.fromMessageBuffer(message.key, this.sr);
      message.parsedKey = keyDecoded.value;
      message.schemaIdKey = keyDecoded.schemaId;
    } catch (err) {
      log.warn(err, '_onWrapper() :: Warning, consumer did not find topic on SR for key schema',
        {topic: message.topic});

      const {key} = message;
      if (key) {
        try {
          message.parsedKey = Buffer.isBuffer(key)
            ? JSON.parse(key.toString())
            : key;
        } catch (err) {
          if (err instanceof SyntaxError) {
            message.parsedKey = key.toString();
          } else {
            log.warn(err, '_onWrapper() :: Error parsing key', {key});
          }
        }
      }
    }

    try {
      const valueDecoded = magicByte.fromMessageBuffer(message.value, this.sr);
      message.parsed = valueDecoded.value;
      message.schemaId = valueDecoded.schemaId;
      cb(message);
    } catch (err) {
      log.warn(err, '_onWrapper() :: Warning, consumer did not find topic on SR for value schema',
        {topic: message.topic});

      if (message.value) {
        try {
          message.parsed = Buffer.isBuffer(message.value)
            ? JSON.parse(message.value.toString())
            : message.value;
        } catch (err) {
          if (err instanceof SyntaxError) {
            message.parsed = message.value.toString();
          } else {
            log.warn(err, '_onWrapper() :: Error parsing value', {value: message.value});
          }
        }
      }
      cb(message);
    }

  }.bind(this));
};

/**
 * Deserialize an avro message.
 *
 * @param {avsc.Type} type Avro type instance.
 * @param {Object} message The raw message.
 * @param {boolean} isKey Whether the data is the key or value of the schema.
 * @return {Object} The deserialized object.
 */
Consumer.prototype.deserialize = function (type, message, isKey) {
  try {
    const deserializeType = isKey === true
      ? message.key
      : message.value;

    return magicByte.fromMessageBuffer(
      type,
      deserializeType,
      this.sr
    );
  } catch (err) {
    log.warn(`deserialize() :: Error deserializing on topic ${message.topic}`,
      'Raw value:', message.value, `Partition: ${message.partition} Offset:`,
      `${message.offset} Key: ${message.key} Exception:`, err);
    return null;
  }
};

/**
 * Avro deserialization helper.
 *
 * @param {avsc.Type} type Avro type instance.
 * @param {Object} data The raw data to deserialize.
 * @param {boolean} isKey Whether the data is the key or value of the schema.
 * @return {Object} The deserialized object's value and schemaId.
 * @private
 */
Consumer.prototype._deserializeType = function (type, data, isKey) {
  const decoded = this.deserialize(type, data, isKey);

  return {
    value: !decoded ? null : decoded.value,
    schemaId: !decoded ? null : decoded.schemaId
  };
};
