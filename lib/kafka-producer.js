/**
 * @fileOverview Wrapper for node-rdkafka Producer Ctor, a mixin.
 */
var Promise = require('bluebird');
var cip = require('cip');
var kafka = require('node-rdkafka');

var magicByte = require('./magic-byte');
var log = require('./log.lib').getChild(__filename);

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
 * @param {Object=} topts Producer topic options.
 * @see https://github.com/edenhill/librdkafka/blob/2213fb29f98a7a73f22da21ef85e0783f6fd67c4/CONFIGURATION.md
 * @return {Promise(kafka.Producer)} A Promise.
 */
Producer.prototype.getProducer = Promise.method(function (opts, topts) {
  if (!opts) {
    opts = {};
  }

  if (!opts['metadata.broker.list']) {
    opts['metadata.broker.list'] = this.kafkaBrokerUrl;
  }

  log.info('getProducer() :: Starting producer with options:', opts);

  var producer = new kafka.Producer(opts, topts);

  this._producers.push(producer);

  // hack node-rdkafka
  producer.__kafkaAvro_produce = producer.produce;
  producer.produce = this._produceWrapper.bind(this, producer);

  return new Promise(function(resolve, reject) {
    producer.on('ready', function() {
      log.debug('getProducer() :: Got "ready" event.');
      resolve(producer);
    });

    producer.connect({}, function(err) {
      if (err) {
        log.error('getProducer() :: Connect failed:', err);
        reject(err);
        return;
      }
      log.debug('getProducer() :: Got "connect()" callback.');
      resolve(producer); // depend on Promises' single resolve contract.
    });
  })
    .return(producer);
});

/**
 * The node-rdkafka produce method wrapper, will validate and serialize
 * the message against the existing schemas.
 *
 * @param {kafka.Producer} producerInstance node-rdkafka instance.
 * @param {kafka.Producer.Topic} kafkaTopic node-rdkafka Topic instance.
 * @param {number} partition The partition to produce on.
 * @param {Object} value The message.
 * @param {string|number} key The partioning key.
 * @param {*=} optOpaque Pass vars to receipt handler.
 */
Producer.prototype._produceWrapper = function (producerInstance, kafkaTopic,
  partition, value, key, optOpaque) {

  var topicName = kafkaTopic.name();

  if (!this.sr.valueSchemas[topicName]) {
    // topic not found in schemas, bail early

    log.warn('_produceWrapper() :: Warning, did not find topic on SR:',
      topicName);

    var bufVal = new Buffer(JSON.stringify(value));
    return producerInstance.__kafkaAvro_produce(kafkaTopic, partition, bufVal,
      key, optOpaque);
  }

  var type = this.sr.valueSchemas[topicName];
  var schemaId = this.sr.schemaMeta[topicName].id;

  var bufValue = this.serialize(type, schemaId, value);

  return producerInstance.__kafkaAvro_produce(kafkaTopic, partition, bufValue,
    key, optOpaque);
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
  var bufValue = magicByte.toMessageBuffer(value, type, schemaId);

  return bufValue;
};
