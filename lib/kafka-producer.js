/**
 * @fileOverview Wrapper for node-rdkafka Producer Ctor, a mixin.
 */
const Promise = require('bluebird');
const cip = require('cip');
const kafka = require('node-rdkafka');

const magicByte = require('./magic-byte');
const log = require('./log.lib').getChild(__filename);

/**
 * Wrapper for node-rdkafka Produce Ctor, a mixin.
 *
 * @constructor
 */
const Producer = module.exports = cip.extend();

/**
 * The wrapper of the node-rdkafka package Producer Ctor.
 *
 * @param {Object} opts Producer general options.
 * @param {Object=} topicOptions Producer topic options.
 * @see https://github.com/edenhill/librdkafka/blob/2213fb29f98a7a73f22da21ef85e0783f6fd67c4/CONFIGURATION.md
 * @return {Promise(kafka.Producer)} A Promise.
 */
Producer.prototype.getProducer = Promise.method(function (opts, topicOptions) {
  if (!opts) {
    opts = {};
  }

  if (!opts['metadata.broker.list']) {
    opts['metadata.broker.list'] = this.kafkaBrokerUrl;
  }

  log.info('getProducer() :: Starting producer with options:', opts);

  const producer = new kafka.Producer(opts, topicOptions);

  this._producers.push(producer);

  // hack node-rdkafka
  producer.__kafkaAvro_produce = producer.produce;
  producer.produce = this._produceWrapper.bind(this, producer);

  return new Promise(function (resolve, reject) {
    producer.on('ready', function () {
      log.debug('getProducer() :: Got "ready" event.');
      resolve(producer);
    });

    producer.connect({}, function (err) {
      if (err) {
        log.error({err}, 'getProducer() :: Connect failed:');
        reject(err);
        return;
      }
      log.debug('getProducer() :: Got "connect()" callback.');
      resolve(producer); // depend on Promises' single resolve contract.
    });
  })
    .return(producer);
});

function prepareSubject(isKey, subject, topicName, data) {
  if (isKey) {
    return this.sr.keySubjectStrategy.prepareSubjectName(topicName, data, isKey);
  } else {
    return this.sr.valueSubjectStrategy.prepareSubjectName(topicName, data, isKey);
  }
}

/**
 * Avro serialization helper.
 *
 * @param {string=} topicName Name of the topic.
 * @param {boolean} isKey Whether the data is the key or value of the schema.
 * @param {*} data Data to be serialized.
 * @private
 */
Producer.prototype._serializeType = function (topicName, isKey, data) {
  let schema = null;
  let subject = null;

  if (data.__id != null) {
    schema = this.sr.schemaTypeById[`schema-${data.__id}`];
  } else {
    subject = prepareSubject.call(this, isKey, subject, topicName, data);
    schema = this.sr.schemas[subject];
  }

  if (!schema) {
    log.warn('_produceWrapper() :: Warning, did not find topic on SR for subject schema:',
      {
        topic: topicName,
        subject: subject
      });

    if (this.__shouldFailWhenSchemaIsMissing) {
      throw new Error(
        `Unable to serialize message for topic ${topicName} : schema not found`);
    }

    if (typeof data === 'object') {
      // Making sure __schemaName and __id is not serialized in case avro schema is missing and json is used as a fallback.
      delete data.__schemaName;
      delete data.__id;
      return new Buffer(JSON.stringify(data));
    } else {
      return new Buffer(data);
    }
  } else {
    const schemaId = this.sr.schemaIds[subject];

    return this.serialize(schema, schemaId, data);
  }
};

/**
 * The node-rdkafka produce method wrapper, will validate and serialize
 * the message against the existing schemas.
 *
 * @param {kafka.Producer} producerInstance node-rdkafka instance.
 * @param {string} topicName The topic name.
 * @param {number} partition The partition to produce on.
 * @param {Object} value The message.
 * @param {string|number} key The partitioning key.
 * @param {number} timestamp The create time value.
 * @param {*=} optOpaque Pass vars to receipt handler.
 */
Producer.prototype._produceWrapper = function (producerInstance, topicName,
                                               partition, value, key, timestamp, optOpaque) {
  var sendKey = key
    ? this._serializeType(topicName, true, key)
    : null;

  const sendValue = value
    ? this._serializeType(topicName, false, value)
    : null;

  return producerInstance.__kafkaAvro_produce(topicName, partition, sendValue,
    sendKey, timestamp, optOpaque);
};

/**
 * Serialize the message using avro.
 *
 * @param {avsc.Type} type The avro type instance.
 * @param {number} schemaId The schema id.
 * @param {*} value The value to serialize.
 * @return {Buffer} Serialized buffer.
 * @private
 */
Producer.prototype.serialize = function (type, schemaId, value) {
  return magicByte.toMessageBuffer(value, type, schemaId);
};
