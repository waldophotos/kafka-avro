/**
 * @fileOverview Queries the Schema Registry for all schemas and evaluates them.
 */
var url = require('url');

var Promise = require('bluebird');
var cip = require('cip');
var axios = require('axios');
var avro = require('avsc');

var rootLog = require('./log.lib');
var log = rootLog.getChild(__filename);

/**
 * Queries the Schema Registry for all schemas and evaluates them.
 *
 * @param {Object} opts Options to initialize the SR module with:
 *   @param {string} schemaRegistryUrl The SR url.
 *   @param {Array.<string>|null} selectedTopics Selected topics to fetch if defined.
 *   @param {boolean} fetchAllVersions Fetch all past versions for each topic.
 * @constructor
 */
var SchemaRegistry = module.exports = cip.extend(function(opts) {
  /** @type {string} The SR url */
  this.schemaRegistryUrl = opts.schemaRegistryUrl;

  /** @type {Array.<string>|null} The selected topics to fetch, can be null */
  this.selectedTopics = opts.selectedTopics;

  /** @type {boolean} Fetch all versions for each topic. */
  this.fetchAllVersions = opts.fetchAllVersions;

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

  /**
   * A dict containing the instance of the "avsc" package, and key the SR schema id.
   *
   * @type {Object}
   */
  this.schemaTypeById = {};
});

/**
 * Initialize the library, fetch schemas and register them locally.
 *
 * @return {Promise(Array.<Object>)} A promise with the registered schemas.
 */
SchemaRegistry.prototype.init = Promise.method(function () {
  log.info('init() :: Initializing SR, will fetch all schemas from SR...');

  return this._fetchTopics()
    .bind(this)
    .map(this._fetchLatestVersions, {concurrency: 10})
    .map(this._fetchSchemaVersions, {concurrency: 10})
    .then(schemaVersions => [].concat(...schemaVersions)) // Flatten
    .map(this._fetchSchema, {concurrency: 10})
    .map(this._registerSchema);
});

/**
 * A master wrapper method to determine if all topics or just specific ones
 * need to be fetched.
 *
 * @return {Promise(Array.<string>)} A Promise with an arrray of string topics.
 * @private
 */
SchemaRegistry.prototype._fetchTopics = Promise.method(function () {
  if (this.selectedTopics) {
    return this.selectedTopics;
  } else {
    return this._fetchAllSchemaTopics();
  }
});

/**
 * Fetch all registered schema topics from SR.
 *
 * @return {Promise(Array.<string>)} A Promise with an arrray of string topics.
 * @private
 */
SchemaRegistry.prototype._fetchAllSchemaTopics = Promise.method(function () {

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
 * Fetch all available versions for a subject from the SR.
 *
 * @return {Promise(Array.<Object>)} A Promise with an array of objects
 *   containing the topic name and version number.
 * @private
 */
SchemaRegistry.prototype._fetchSchemaVersions = Promise.method(function (schemaTopic) {
  var fetchVersionsUrl = url.resolve(this.schemaRegistryUrl,
    '/subjects/' + schemaTopic + '/versions');

  log.debug('_fetchSchemaVersions() :: Fetching schema versions:',
    fetchVersionsUrl);

  return Promise.resolve(axios.get(fetchVersionsUrl))
    .then((response) => {
      log.debug('_fetchSchemaVersions() :: Fetched schema versions:',
        fetchVersionsUrl);

      return response.data.concat('latest').map(version => ({
        version: version,
        schemaTopic: schemaTopic,
      }));
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
SchemaRegistry.prototype._fetchSchema = Promise.method(function (topicMeta) {
  var schemaTopic = topicMeta.schemaTopic;
  var version = topicMeta.version;
  var parts = schemaTopic.split('-');
  var schemaType = parts.pop();
  var topic = parts.join('-');

  var fetchSchemaUrl = url.resolve(this.schemaRegistryUrl,
    '/subjects/' + schemaTopic + '/versions/' + version);

  log.debug('_fetchSchema() :: Fetching schema url:',
    fetchSchemaUrl);

  return Promise.resolve(axios.get(fetchSchemaUrl))
    .then((response) => {
      log.debug('_fetchSchema() :: Fetched schema url:',
        fetchSchemaUrl);

      return {
        version: version,
        responseRaw: response.data,
        schemaType: schemaType,
        schemaTopicRaw: schemaTopic,
        topic: topic,
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
SchemaRegistry.prototype._registerSchema = Promise.method(function (schemaObj) {
  log.debug('_registerSchema() :: Registering schema:',
    schemaObj.topic);

  try {
    schemaObj.type = avro.parse(schemaObj.responseRaw.schema, {wrapUnions: true});
  } catch(ex) {
    log.warn('_registerSchema() :: Error parsing schema:',
      schemaObj.schemaTopicRaw, 'Error:', ex.message, 'Moving on...');
    return schemaObj;
  }

  log.debug('_registerSchema() :: Registered schema:', schemaObj.topic);

  if (schemaObj.schemaType.toLowerCase() === 'value') {
    this.schemaTypeById['schema-' + schemaObj.responseRaw.id] = schemaObj.type;
    if (schemaObj.version === 'latest') {
      this.valueSchemas[schemaObj.topic] = schemaObj.type;
      this.schemaMeta[schemaObj.topic] = schemaObj.responseRaw;
    }
  } else {
    if (schemaObj.version === 'latest') {
      this.keySchemas[schemaObj.topic] = schemaObj.type;
    }
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
SchemaRegistry.prototype._handleAxiosError = function (err) {
  if (!err.port) {
    // not an axios error, bail early
    throw err;
  }

  log.warn('_handleAxiosError() :: http error:', err.message,
    'Url:', err.config.url);

  throw err;
};
