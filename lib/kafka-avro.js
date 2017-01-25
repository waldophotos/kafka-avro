/*
 * kafka-avro
 * Node.js bindings for librdkafka with Avro schema serialization.
 * https://github.com/waldo/kafka-avro
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

//
// Mixins
//
var Producer = require('./kafka-producer');
var Consumer = require('./kafka-consumer');

var CeventEmitter = cip.cast(EventEmitter);

/**
 * @fileOverview bootstrap and master exporing module.
 */

/**
 * The master module.
 *
 * @constructor
 */
var KafkaAvro = module.exports = CeventEmitter.extend(function(opts) {
  /** @type {string} The SR url */
  this.kafkaBrokerUrl = opts.kafkaBroker;

  /** @type {string} The SR url */
  this.schemaRegistryUrl = opts.schemaRegistry;

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
});

//
// Add Mixins
//
KafkaAvro.mixin(Producer);
KafkaAvro.mixin(Consumer);

/**
 * Initialize the library, fetch schemas and register them locally.
 *
 * @return {Promise(Array.<Object>)} A promise with the registered schemas.
 */
KafkaAvro.prototype.init = Promise.method(function () {
  return this._fetchAllSchemaTopics()
    .bind(this)
    .map(this._fetchSchema, {concurrency: 10})
    .map(this._registerSchema);
});

/**
 * Fetch all registered schema topics from SR.
 *
 * @return {Promise(Array.<string>)} A Promise with an arrray of string topics.
 * @private
 */
KafkaAvro.prototype._fetchAllSchemaTopics = Promise.method(function () {

  var fetchAllTopicsUrl = url.resolve(this.schemaRegistryUrl, '/subjects');
  return Promise.resolve(axios.get(fetchAllTopicsUrl))
    .bind(this)
    .then((response) => {
      console.log('Fetched total schemas:', response.data.length);
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

  return Promise.resolve(axios.get(fetchSchemaUrl))
    .then((response) => {
      return {
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
  try {
    schemaObj.type = avro.parse(schemaObj.schemaRaw.schema, {wrapUnions: true});
  } catch(ex) {
    console.error('Error parsing schema:', schemaObj.schemaTopicRaw, 'Error:',
      ex.message);
    throw ex;
  }

  if (schemaObj.schemaType.toLowerCase() === 'value') {
    this.valueSchemas[schemaObj.topic] = schemaObj.type;
  } else {
    this.keySchemas[schemaObj.topic] = schemaObj.type;
  }
  return schemaObj;
});

KafkaAvro.prototype._handleAxiosError = function (err) {
  if (!err.port) {
    // not an axios error, bail early
    throw err;
  }

  console.error('http error:', err.message, 'Url:', err.config.url);

  throw err;
};
