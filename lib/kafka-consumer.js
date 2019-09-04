/**
 * @fileOverview Wrapper for node-rdkafka Consumer Ctor, a mixin.
 */

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

  consumer.on('disconnect', function (arg) {
    log.warn('getConsumer() :: Consumer disconnected. Args:', arg);
  });

  consumer.on('error', function (err) {
    log.error('getConsumer() :: Consumer Error event fired:', err);
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
 * @param {Object} topts Topic specific options.
 * @param {Object} sopts node-rdkafka ConsumerStream options
 * @see https://github.com/edenhill/librdkafka/blob/2213fb29f98a7a73f22da21ef85e0783f6fd67c4/CONFIGURATION.md
 * @see https://blizzard.github.io/node-rdkafka/current/KafkaConsumerStream.html
 * @return {Promise(kafka.ConsumerStream)} A Promise with the consumer stream.
 */
Consumer.prototype.getConsumerStream = Promise.method(function (opts, topts, sopts) {
  if (!opts['metadata.broker.list']) {
    opts['metadata.broker.list'] = this.kafkaBrokerUrl;
  }

  log.info('getConsumerStream() :: Starting Consumer Stream with opts:', opts);

  const consumer = new kafka.KafkaConsumer.createReadStream(opts, topts, sopts);

  this._consumersStream.push(consumer);

  consumer.on('disconnect', function (arg) {
    log.warn('getConsumerStream() :: Consumer disconnected. Args:', arg);
  });

  consumer.on('error', function (err) {
    log.error('getConsumerStream() :: Consumer Error event fired:', err);
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
    const keyDecoded = magicByte.fromMessageBuffer(message.key, this.sr);
    const valueDecoded = magicByte.fromMessageBuffer(message.value, this.sr);


    if (!keyDecoded) {
      log.warn('_onWrapper() :: Warning, consumer did not find topic on SR for key schema:',
        message.topic);

      message.parsedKey = null;

      if (message.key) {
        try {
          message.parsedKey = Buffer.isBuffer(message.key)
            ? JSON.parse(message.key.toString('utf-8'))
            : message.key;
        } catch (ex) {
          if (ex instanceof SyntaxError) {
            message.parsedKey = message.key.toString('utf-8');
          } else {
            log.warn('_onWrapper() :: Error parsing key:', message.key,
              'Error:', ex);
          }
        }
      }
    } else {
      message.parsedKey = keyDecoded.value;
      message.schemaIdKey = keyDecoded.schemaId;
    }

    if (!valueDecoded) {
      log.warn('_onWrapper() :: Warning, consumer did not find topic on SR for value schema:',
        message.topic);

      message.parsed = null;

      if (message.value) {
        try {
          message.parsed = Buffer.isBuffer(message.value)
            ? JSON.parse(message.value.toString('utf-8'))
            : message.value;
        } catch (ex) {
          if (ex instanceof SyntaxError) {
            message.parsed = message.value.toString('utf-8');
          } else {
            log.warn('_onWrapper() :: Error parsing value:', message.value,
              'Error:', ex);
          }
        }
      }
      cb(message);
      return;
    }

    message.parsed = valueDecoded.value;
    message.schemaId = valueDecoded.schemaId;

    cb(message);
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
    var deserializeType = isKey === true
      ? message.key
      : message.value;

    var parsed = magicByte.fromMessageBuffer(
      type,
      deserializeType,
      this.sr
    );
  } catch (ex) {
    log.warn(`deserialize() :: Error deserializing on topic ${message.topic}`,
      'Raw value:', message.value, `Partition: ${message.partition} Offset:`,
      `${message.offset} Key: ${message.key} Exception:`, ex);
    return null;
  }

  return parsed;
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
  var decoded = this.deserialize(type, data, isKey);

  return {
    value: !decoded ? null : decoded.value,
    schemaId: !decoded ? null : decoded.schemaId
  };
};
