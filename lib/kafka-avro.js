/*
 * kafka-avro
 * Node.js bindings for librdkafka with Avro schema serialization.
 * https://github.com/waldophotos/kafka-avro
 *
 * Copyright © Waldo, Inc.
 * Licensed under the MIT license.
 */
var EventEmitter = require('events').EventEmitter;

var Promise = require('bluebird');
var cip = require('cip');
var Kafka = require('node-rdkafka');

var rootLog = require('./log.lib');
var log = rootLog.getChild(__filename);

var SchemaRegistry = require('./schema-registry');

//
// Mixins
//
var Producer = require('./kafka-producer');
var Consumer = require('./kafka-consumer');

var CeventEmitter = cip.cast(EventEmitter);

function noop() {}

/**
 * @fileOverview bootstrap and master exporing module.
 */

/**
 * The master module.
 *
 * @param {Object} opts The options.
 * @constructor
 */
var KafkaAvro = module.exports = CeventEmitter.extend(function(opts) {
  /** @type {string} The SR url */
  this.kafkaBrokerUrl = opts.kafkaBroker;

  var srOpts = {
    schemaRegistryUrl: opts.schemaRegistry,
    selectedTopics: opts.topics || null,
    fetchAllVersions: opts.fetchAllVersions || false,
    fetchRefreshRate: opts.fetchRefreshRate || 0,
    parseOptions: opts.parseOptions,
    httpsAgent: opts.httpsAgent
  };

  /** @type {kafka-avro.SchemaRegistry} Instanciated SR. */
  this.sr = new SchemaRegistry(srOpts);

  /** @type {boolean} Whether the producer should fail when no schema was found. */
  this._shouldFailWhenSchemaIsMissing = opts.shouldFailWhenSchemaIsMissing === true;

  /** @type {Array.<node-rdkafka.Producer>} Instanciated producers. */
  this._producers = [];
  /** @type {Array.<node-rdkafka.Consumer>} Instanciated consumers. */
  this._consumers = [];
  /** @type {Array.<node-rdkafka.ConsumerStream>} Instanciated consumers. */
  this._consumersStream = [];
});

/**
 * Expose the node-rdkafka library's CODES constants.
 *
 * @type {Object}
 */
KafkaAvro.CODES = Kafka.CODES;

//
// Add Mixins
//
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

  var disconnectPromises = [];

  log.info('dispose() :: Disposing kafka-avro instance. Total consumers:',
    this._consumers.length, 'Total producers:', this._producers.length);

  this._consumers.forEach(function(consumer) {
    var discon = Promise.promisify(consumer.disconnect.bind(consumer));
    var disconProm = discon().catch(noop);

    disconnectPromises.push(disconProm);
  });
  this._producers.forEach(function(producer) {
    var discon = Promise.promisify(producer.disconnect.bind(producer));
    var disconProm = discon().catch(noop);
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
