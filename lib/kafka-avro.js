/*
 * kafka-avro
 * Node.js bindings for librdkafka with Avro schema serialization.
 * https://github.com/waldophotos/kafka-avro
 *
 * Copyright © Waldo, Inc.
 * Licensed under the MIT license.
 */
const EventEmitter = require('events').EventEmitter;

const Promise = require('bluebird');
const cip = require('cip');
const Kafka = require('node-rdkafka');

const rootLog = require('./log.lib');
const log = rootLog.getChild(__filename);
const SchemaRegistry = require('./schema-registry');
const { SubjectNameStrategy } = require('./subject-strategy');

//
// Mixins
//
const Producer = require('./kafka-producer');
const Consumer = require('./kafka-consumer');

const CeventEmitter = cip.cast(EventEmitter);

function noop() {
}

/**
 * @fileOverview bootstrap and master exporting module.
 */

/**
 * The master module.
 *
 * @param {Object} opts The options.
 * @constructor
 */
const KafkaAvro = module.exports = CeventEmitter.extend(function (opts) {
  /** @type {string} The SR url */
  this.kafkaBrokerUrl = opts.kafkaBroker;

  const srOpts = {
    schemaRegistryUrl: opts.schemaRegistry,
    auth: opts.schemaRegistryAuth || null,
    selectedTopics: opts.topics || null,
    fetchAllVersions: opts.fetchAllVersions || false,
    fetchRefreshRate: opts.fetchRefreshRate || 0,
    parseOptions: opts.parseOptions,
    httpsAgent: opts.httpsAgent,
    keySubjectStrategy: new SubjectNameStrategy(opts.keySubjectStrategy),
    valueSubjectStrategy: new SubjectNameStrategy(opts.valueSubjectStrategy),
  };

  /** @type {kafka-avro.SchemaRegistry} Instantiated SR. */
  this.sr = new SchemaRegistry(srOpts);

  /** @type {boolean} Whether the producer should fail when no schema was found. */
  this.__shouldFailWhenSchemaIsMissing = opts.shouldFailWhenSchemaIsMissing === true;

  /** @type {Array.<node-rdkafka.Producer>} Instanciated producers. */
  this._producers = [];
  /** @type {Array.<node-rdkafka.Consumer>} Instantiated consumers. */
  this._consumers = [];
  /** @type {Array.<node-rdkafka.ConsumerStream>} Instantiated consumers. */
  this._consumersStream = [];
});

/**
 * Expose the node-rdkafka library's CODES constants.
 *
 * @type {Object}
 */
KafkaAvro.CODES = Kafka.CODES;

KafkaAvro.mixin(Producer);
KafkaAvro.mixin(Consumer);

/**
 * Get the bunyan logger.
 *
 * @return {bunyan.Logger} The bunyan logger, singleton.
 * @static
 */
KafkaAvro.getLogger = function () {
  return rootLog;
};

/**
 * Initialize the library, fetch schemas and register them locally.
 *
 * @return {Promise(Array.<Object>)} A promise with the registered schemas.
 */
KafkaAvro.prototype.init = Promise.method(function () {
  log.info('init() :: Initializing KafkaAvro...');
  return this.sr.init();
});

/**
 * Dispose the method.
 *
 * @return {Promise} A Promise.
 */
KafkaAvro.prototype.dispose = Promise.method(function () {
  const disconnectPromises = [];

  log.info('dispose() :: Disposing kafka-avro instance. Total consumers: noOfConsumers, Total producers: noOfProducers',
    {
      noOfConsumers: this._consumers.length,
      noOfProducers: this._producers.length
    });

  this._consumers.forEach(function (consumer) {
    const discon = Promise.promisify(consumer.disconnect.bind(consumer));
    const disconProm = discon().catch(noop);

    disconnectPromises.push(disconProm);
  });
  this._producers.forEach(function (producer) {
    const discon = Promise.promisify(producer.disconnect.bind(producer));
    const disconProm = discon().catch(noop);
    disconnectPromises.push(disconProm);
  });

  return Promise.all(disconnectPromises)
    .then( () => {
      if(this.sr._refreshHandle)
        clearInterval(this.sr._refreshHandle);
    });
});

/**
 * @param {boolean} shouldFail Whether the producer should fail when no schema was found.
 */
KafkaAvro.prototype.setShouldFailWhenSchemaIsMissing = function(shouldFail) {
  this.__shouldFailWhenSchemaIsMissing = shouldFail;
};
