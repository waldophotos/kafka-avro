/**
 * @fileOverview Queries the Schema Registry for all schemas and evaluates them.
 */
const URL = require('url').URL;

const Promise = require('bluebird');
const cip = require('cip');
const axios = require('axios');
const avro = require('avsc');
const _ = require('lodash');
let instance = axios;

const rootLog = require('./log.lib');
const log = rootLog.getChild(__filename);

/**
 * Queries the Schema Registry for all schemas and evaluates them.
 *
 * @param {Object} opts Options to initialize the SR module with:
 *   @param {string} schemaRegistryUrl The SR url.
 *   @param {Array.<string>|null} selectedTopics Selected topics to fetch if defined.
 *   @param {boolean} fetchAllVersions Fetch all past versions for each topic.
 *   @param {?Object} parseOptions Schema parsing options to pass to avro.parse().
 * @constructor
 */
const SchemaRegistry = module.exports = cip.extend(function (opts) {
  if (opts.httpsAgent) {
    instance = axios.create({httpsAgent: opts.httpsAgent});
  }

  /** @type {Object} The SR URL object */
  this.schemaRegistryUrl = new URL(opts.schemaRegistryUrl + (opts.schemaRegistryUrl.endsWith('/') ? '' : '/'));

  /** @type {Array.<string>|null} The user selected topics to fetch, can be null */
  this.selectedTopics = opts.selectedTopics;

  /** @type {boolean} Fetch all versions for each topic. */
  this.fetchAllVersions = opts.fetchAllVersions;

  /** @type {?Array.<string>} List of schema topics */
  this.schemaTopics = null;

  /** @type {?Object} Schema parsing options to pass to avro.parse() */
  this.parseOptions = opts.parseOptions || {};
  if (this.parseOptions.wrapUnions === undefined) {
    this.parseOptions.wrapUnions = true;
  }

  /**
   * *Key schema subject strategy*
   *
   * Avro serializer registers a schema in Schema Registry under a subject name, which essentially defines a namespace in the registry:
   * Compatibility checks are per subject
   * Versions are tied to subjects
   * When schemas evolve, they are still associated to the same subject but get a new schema id and version
   *Overview
   *The subject name depends on the subject name strategy, which you can set to one of the following three values:
   *
   * *TopicNameStrategy* (io.confluent.kafka.serializers.subject.TopicNameStrategy) – this is the default
   * *RecordNameStrategy* (io.confluent.kafka.serializers.subject.RecordNameStrategy)
   * *TopicRecordNameStrategy* (io.confluent.kafka.serializers.subject.TopicRecordNameStrategy)
   */
  this.keySubjectStrategy = opts.keySubjectStrategy;

  /**
   * *Value schema subject strategy*
   *
   * Avro serializer registers a schema in Schema Registry under a subject name, which essentially defines a namespace in the registry:
   * Compatibility checks are per subject
   * Versions are tied to subjects
   * When schemas evolve, they are still associated to the same subject but get a new schema id and version
   *Overview
   *The subject name depends on the subject name strategy, which you can set to one of the following three values:
   *
   * *TopicNameStrategy* (io.confluent.kafka.serializers.subject.TopicNameStrategy) – this is the default
   * *RecordNameStrategy* (io.confluent.kafka.serializers.subject.RecordNameStrategy)
   * *TopicRecordNameStrategy* (io.confluent.kafka.serializers.subject.TopicRecordNameStrategy)
   */
  this.valueSubjectStrategy = opts.valueSubjectStrategy;

  /** @type {?Number} The refresh time (in seconds) that the schema registry will be fetch */
  this.fetchRefreshRate = opts.fetchRefreshRate;

  /**
   * A dict containing all the schemas with key the bare topic name and
   * value the instance of the "avsc" package.
   *
   * @type {Object}
   */
  this.schemas = {};

  /**
   * A dict containing all the value schemas metadata, with key the bare
   * topic name and value the SR response on that topic:
   *
   * 'subject' {string} The full topic name.
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


  /**
   * A dict containing the schema ids for keys and values.
   *
   * @type {Object}
   */
  this.schemaIds = {};
});

/**
 * Deep coping avsc parse options. Avsc will add registry to those options and if we try to parse schema with the same name it will fail
 * e.g. with fetch all versions enabled, trying to avsc.parse an evolved version of the same schema will trow an exception that there is a duplicate schema in the registry.
 *
 * @param parseOptions
 * @returns {Object}
 */
function deepCopyAvscParseOptionsToAvoidDuplicateSchemaException(parseOptions) {
  return _.extend({}, parseOptions);
}

/**
 * Get the avro RecordType from a schema response.
 *
 * @return {RecordType} An avro RecordType representing the parsed avro schema.
 */
function typeFromSchemaResponse(schema, parseOptions) {
  return avro.parse(schema, deepCopyAvscParseOptionsToAvoidDuplicateSchemaException(parseOptions));
}

/**
 * Initialize the library, fetch schemas and register them locally.
 *
 * @return {Promise(Array.<Object>)} A promise with the registered schemas.
 */
SchemaRegistry.prototype.init = Promise.method(function () {
  log.info('init() :: Initializing SR, will fetch all schemas from SR...');
  if (this.fetchRefreshRate > 0) {
    this._refreshHandle = setInterval(this._fetchSchemas.bind(this), this.fetchRefreshRate * 1000);
  }
  return this._fetchSchemas();
});

/**
 * After the initial schema registration is complete check if it is required to
 * fetch all the past versions for each topic and concatenate those results.
 *
 * @param {Array.<Object>} registeredSchemas The registered schemas from the first op.
 * @return {Promise(Array.<Object>)} The same schemas plus all the versions if requested.
 * @private
 */
SchemaRegistry.prototype._checkForAllVersions = Promise.method(
  function (registeredSchemas) {
    if (!this.fetchAllVersions) {
      return registeredSchemas;
    }

    // Fetch and register all past versions for each schema.
    return Promise.resolve(this.schemaTopics)
      .bind(this)
      .map(this._fetchAllSchemaVersions, {concurrency: 10})
      .filter(Boolean)
      .then(this._flatenResults)
      .map(this._fetchSchema, {concurrency: 10})
      .map(this._registerSchema)
      .then(function (allRegisteredSchemas) {
        return registeredSchemas.concat(allRegisteredSchemas);
      });
  });

/**
 * Flatten the array of arrays produced by _fetchAllSchemaVersions().
 *
 * @param {Array.<Array.<Object>>} results Results as produced by _fetchAllSchemaVersions().
 * @return {Promise(Array.<Object>)} Flattened results.
 * @private
 */
SchemaRegistry.prototype._flatenResults = Promise.method(function (results) {
  const flattenedResults = [];
  results.map(function (schemaVersions) {
    schemaVersions.forEach(function (schemaVersion) {
      flattenedResults.push(schemaVersion);
    });
  });

  return flattenedResults;
});

/**
 * A master wrapper method to determine if all topics or just specific ones
 * need to be fetched.
 *
 * @return {Promise(Array.<string>)} A Promise with an array of string topics.
 * @private
 */
SchemaRegistry.prototype._fetchTopics = Promise.method(function () {
  if (this.selectedTopics) {
    return this._processSelectedTopics();
  } else {
    return this._fetchAllSchemaTopics();
  }
});

/**
 * Will store the topics to fetch schemas for
 * annotations.
 *
 * @param {Array.<string>} schemaTopics An array of all the schema topics.
 * @return {Promise(Array.<string>)} A Promise with the input as is.
 * @private
 */
SchemaRegistry.prototype._storeTopics = Promise.method(function (schemaTopics) {
  this.schemaTopics = schemaTopics;
  return schemaTopics;
});

/**
 * Process the selected topics so as to append `-value` and `-key` values.
 *
 * @return {Array.<string>} An array of properly formated strings to be used as
 *   topics when fetching the schemas.
 * @private
 */
SchemaRegistry.prototype._processSelectedTopics = Promise.method(function () {
  const topics = [];

  this.selectedTopics.forEach(function (selectedTopic) {
    topics.push(selectedTopic);
  });

  return topics;
});

/**
 * Fetch all registered schema topics from SR.
 *
 * @return {Promise(Array.<string>)} A Promise with an array of string topics.
 * @private
 */
SchemaRegistry.prototype._fetchAllSchemaTopics = Promise.method(function () {
  const fetchAllTopicsUrl = new URL('subjects', this.schemaRegistryUrl).toString();

  log.debug('_fetchAllSchemaTopics() :: Fetching all schemas using url:',
    fetchAllTopicsUrl);

  return Promise.resolve(instance.get(fetchAllTopicsUrl))
    .bind(this)
    .then((response) => {
      log.info('_fetchAllSchemaTopics() :: Fetched total schemas:',
        response.data.length);
      return response.data;
    })
    .catch(this._suppressAxiosError);
});

/**
 * Fetch just the latest version for a topic from the SR.
 *
 * @param {string} schemaTopic The topic to fetch the latest version for.
 * @return {Promise(Object)} A Promise with an object containing the
 *   topic name and version number.
 * @private
 */
SchemaRegistry.prototype._fetchLatestVersion = Promise.method(function (schemaTopic) {
  const fetchLatestVersionUrl = new URL('subjects/' + schemaTopic + '/versions/latest', this.schemaRegistryUrl).toString();

  log.debug('_fetchLatestVersion() :: Fetching latest topic version from url:', fetchLatestVersionUrl);

  return Promise.resolve(instance.get(fetchLatestVersionUrl))
    .then((response) => {
      log.debug('_fetchLatestVersion() :: Fetched latest topic version from url:', fetchLatestVersionUrl);

      return {
        version: response.data.version,
        schemaTopic: schemaTopic,
      };
    })
    .catch(this._suppressAxiosError);
});


/**
 * Fetch all available versions for a subject from the SR.
 *
 * @return {Promise(Array.<Object>)} A Promise with an array of objects
 *   containing the topic name and version number.
 * @private
 */
SchemaRegistry.prototype._fetchAllSchemaVersions = Promise.method(function (schemaTopic) {
  const fetchVersionsUrl = new URL(
    'subjects/' + schemaTopic + '/versions',
    this.schemaRegistryUrl
  ).toString();

  log.debug('_fetchAllSchemaVersions() :: Fetching schema versions:',
    fetchVersionsUrl);

  return Promise.resolve(instance.get(fetchVersionsUrl))
    .then((response) => {
      log.debug('_fetchAllSchemaVersions() :: Fetched schema versions:',
        fetchVersionsUrl);

      return response.data.map(version => ({
        version: version,
        schemaTopic: schemaTopic,
      }));
    })
    .catch(this._suppressAxiosError);
});

/**
 * Fetch a single schema from the SR and return its value along with metadata.
 *
 * @param {Object} topicMeta An object containing the version
 * @return {Promise(Array.<Object>)} A Promise with an array of objects,
 *   see return statement bellow for return schema.
 * @private
 */
SchemaRegistry.prototype._fetchSchema = Promise.method(function (topicMeta) {
  const schemaTopic = topicMeta.schemaTopic;
  const version = topicMeta.version;
  const parts = schemaTopic.split('-');
  const schemaType = parts.pop();
  const topic = parts.join('-');

  const fetchSchemaUrl = new URL(
    'subjects/' + schemaTopic + '/versions/' + version,
    this.schemaRegistryUrl
  ).toString();

  log.debug('_fetchSchema() :: Fetching schema url:',
    fetchSchemaUrl);

  return Promise.resolve(instance.get(fetchSchemaUrl))
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
 * Register the provided schema locally using avro.
 *
 * @param {Object} schemaObj Schema object as produced by _fetchSchema().
 * @return {Promise(Array.<Object>)} A Promise with the object received
 *   augmented with the "type" property which stores the parsed avro schema.
 * @private
 */
SchemaRegistry.prototype._registerSchema = Promise.method(function (schemaObj) {
  log.debug('_registerSchema() :: Registering schema', {schema: schemaObj});

  try {
    schemaObj.type = typeFromSchemaResponse(schemaObj.responseRaw.schema, this.parseOptions);
  } catch (err) {
    log.warn({err}, '_registerSchema() :: Error parsing schema, moving on...',
      {schema: schemaObj});
    return schemaObj;
  }

  log.debug('_registerSchema() :: Registered schema', {schema: schemaObj});

  this.schemaTypeById[`schema-${schemaObj.responseRaw.id}`] = schemaObj.type;

  return schemaObj;
});

/**
 * Register the latest schema version locally using avro.
 *
 * @param {Object} schemaObj Schema object as produced by _fetchSchema().
 * @return {Promise(Array.<Object>)} A Promise with the object received
 *   augmented with the "type" property which stores the parsed avro schema.
 * @private
 */
SchemaRegistry.prototype._registerSchemaLatest = Promise.method(function (schemaObj) {
  log.debug('_registerSchemaLatest() :: Registering schema',
    {schema: schemaObj});

  try {
    schemaObj.type = typeFromSchemaResponse(schemaObj.responseRaw.schema, this.parseOptions);
  } catch (err) {
    log.warn({err}, '_registerSchemaLatest() :: Error parsing schema, moving on...',
      {schema: schemaObj});
    return schemaObj;
  }

  log.debug('_registerSchemaLatest() :: Registered schema:', schemaObj.topic);

  this.schemaTypeById[`schema-${schemaObj.responseRaw.id}`] = schemaObj.type;
  this.schemas[schemaObj.schemaTopicRaw] = schemaObj.type;
  this.schemaIds[schemaObj.schemaTopicRaw] = schemaObj.responseRaw.id;
  this.schemaMeta[schemaObj.schemaTopicRaw] = schemaObj.responseRaw;
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

  log.warn({err}, '_handleAxiosError() :: http error: message, url', {
    message: err.message,
    url: err.config.url
  });

  throw err;
};


/**
 * Suppress axios (http) error.
 *
 * @param {Error} err The error.
 * @throws Error if not from axios.
 * @return {null} empty response.
 * @private
 */
SchemaRegistry.prototype._suppressAxiosError = function (err) {
  if (!err.response) {
    // not an axios error, bail early
    throw err;
  }

  log.debug({err}, '_suppressAxiosError() :: http error for url, will continue operation.',
    {
      error: err.message,
      url: err.config.url
    });

  return null;
};

/**
 * Fetch schemas and register them locally.
 *
 * @return {Promise(Array.<Object>)} A promise with the registered schemas.
 * @private
 */
SchemaRegistry.prototype._fetchSchemas = function () {
  log.debug('_fetchSchemas() :: Schemas refreshed');
  return this._fetchTopics()
    .bind(this)
    .then(this._storeTopics)
    .map(this._fetchLatestVersion, {concurrency: 10})
    .filter(Boolean)
    .map(this._fetchSchema, {concurrency: 10})
    .map(this._registerSchemaLatest)
    .then(this._checkForAllVersions);
};
