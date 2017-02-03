/*
 * kafka-avro
 * Node.js bindings for librdkafka with Avro schema serialization.
 * https://github.com/waldophotos/kafka-avro
 *
 * Copyright © Waldo, Inc.
 * Licensed under the MIT license.
 */
var url = require('url');
var EventEmitter = require('events').EventEmitter;

var Promise = require('bluebird');
var cip = require('cip');
var axios = require('axios');
var avro = require('avsc');
var Kafka = require('node-rdkafka');

var log = require('./log.lib').getChild(__filename);

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

  /** @type {string} The SR url */
  this.schemaRegistryUrl = opts.schemaRegistry;

  /** @type {Array.<node-rdkafka.Producer>} Instanciated producers. */
  this._producers = [];
  /** @type {Array.<node-rdkafka.Consumer>} Instanciated consumers. */
  this._consumers = [];

  /**
   * A dict containing all the value schemas with key the bare topic name and
   * value the instance of the "avsc" package.
   *
   * @type {Object}
   */
  this.valueSchemas = {};

  /**
   * A dict containing all the key schemas with key the bare topic name and
   * value the instance of the "avsc" package.
   *
   * @type {Object}
   */
  this.keySchemas = {};

  /**
   * A dict containing all the value schemas metadata, with key the bare
   * topic name and value the SR response on that topic:
   *
   * 'subject' {string} The full topic name, including the '-value' suffix.
   * 'version' {number} The version number of the schema.
   * 'id' {number} The schema id.
   * 'schema' {string} JSON serialized schema.
   *
   * @type {Object}
   */
  this.schemaMeta = {};
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
  return log;
};

/**
 * Initialize the library, fetch schemas and register them locally.
 *
 * @return {Promise(Array.<Object>)} A promise with the registered schemas.
 */
KafkaAvro.prototype.init = Promise.method(function () {
  log.info('init() :: Initializing KafkaAvro, fetching all schemas from SR...');

  return this._fetchAllSchemaTopics()
    .bind(this)
    .map(this._fetchSchema, {concurrency: 10})
    .map(this._registerSchema);
});

/**
 * Dispose the method.
 *
 * @return {Promise} A Promise.
 */
KafkaAvro.prototype.dispose = Promise.method(function () {

  var disconnectPromises = [];

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

  return Promise.all(disconnectPromises);
});

/**
 * Fetch all registered schema topics from SR.
 *
 * @return {Promise(Array.<string>)} A Promise with an arrray of string topics.
 * @private
 */
KafkaAvro.prototype._fetchAllSchemaTopics = Promise.method(function () {

  var fetchAllTopicsUrl = url.resolve(this.schemaRegistryUrl, '/subjects');

  log.debug('_fetchAllSchemaTopics() :: Fetching all schemas using url:',
    fetchAllTopicsUrl);

  return Promise.resolve(axios.get(fetchAllTopicsUrl))
    .bind(this)
    .then((response) => {
      log.info('_fetchAllSchemaTopics() :: Fetched total schemas:',
        response.data.length);
      return response.data;
    })
    .catch(this._handleAxiosError);
});

/**
 * Fetch a single schema from the SR and return its value along with metadata.
 *
 * @return {Promise(Array.<Object>)} A Promise with an array of objects,
 *   see return statement bellow for return schema.
 * @private
 */
KafkaAvro.prototype._fetchSchema = Promise.method(function (schemaTopic) {
  var parts = schemaTopic.split('-');
  var schemaType = parts.pop();
  var topic = parts.join('-');

  var fetchSchemaUrl = url.resolve(this.schemaRegistryUrl,
    '/subjects/' + schemaTopic + '/versions/latest');

  log.debug('KafkaAvro._fetchSchema() :: Fetching schema url:',
    fetchSchemaUrl);

  return Promise.resolve(axios.get(fetchSchemaUrl))
    .then((response) => {
      log.debug('KafkaAvro._fetchSchema() :: Fetched schema url:',
        fetchSchemaUrl);

      return {
        responseRaw: response.data,
        schemaType: schemaType,
        schemaTopicRaw: schemaTopic,
        topic: topic,
        schemaRaw: response.data,
      };
    })
    .catch(this._handleAxiosError);
});

/**
 * Register the schema locally using avro.
 *
 * @return {Promise(Array.<Object>)} A Promise with the object received
 *   augmented with the "type" property which stores the parsed avro schema.
 * @private
 */
KafkaAvro.prototype._registerSchema = Promise.method(function (schemaObj) {
  log.debug('KafkaAvro._registerSchema() :: Registering schema:',
    schemaObj.topic);

  try {
    schemaObj.type = avro.parse(schemaObj.schemaRaw.schema, {wrapUnions: true});
  } catch(ex) {
    log.warn('_registerSchema() :: Error parsing schema:',
      schemaObj.schemaTopicRaw, 'Error:', ex.message, 'Moving on...');
    return schemaObj;
  }

  log.debug('KafkaAvro._registerSchema() :: Registered schema:',
    schemaObj.topic);

  if (schemaObj.schemaType.toLowerCase() === 'value') {
    this.valueSchemas[schemaObj.topic] = schemaObj.type;
    this.schemaMeta[schemaObj.topic] = schemaObj.responseRaw;
  } else {
    this.keySchemas[schemaObj.topic] = schemaObj.type;
  }
  return schemaObj;
});

/**
 * Handle axios (http) error.
 *
 * @param {Error} err The error.
 * @private
 * @throws Error whatever is passed.
 */
KafkaAvro.prototype._handleAxiosError = function (err) {
  if (!err.port) {
    // not an axios error, bail early
    throw err;
  }

  log.warn('_handleAxiosError() :: http error:', err.message,
    'Url:', err.config.url);

  throw err;
};
