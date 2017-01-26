/*
 * @fileOverview Main testing helper lib.
 */
var axios = require('axios');
var Promise = require('bluebird');

var schemaFix = require('../fixtures/schema.fix');

var testLib = module.exports = {};

testLib.KAFKA_SCHEMA_REGISTRY_URL = 'http://localhost:8081';
testLib.KAFKA_BROKER_URL = 'localhost:9092';

testLib.topic = schemaFix.name;
testLib.topicTwo = schemaFix.name + 'Two';

var testBoot = false;

/**
 * Require from all test scripts, prepares kafka for testing.
 *
 */
testLib.init = function() {
  if (testBoot) {
    return;
  }
  testBoot = true;

  beforeEach(function() {
    return Promise.all([
      testLib.registerSchema(testLib.topic, schemaFix),
      testLib.registerSchema(testLib.topicTwo, schemaFix),
    ]);
  });
};

/**
 * Register a schema on SR.
 *
 * @param {string} The topic.
 * @param {schema} Object The schema to register.
 * @return {Promise} A Promise.
 */
testLib.registerSchema = Promise.method(function(topic, schema) {
  var schemaCreateUrl = testLib.KAFKA_SCHEMA_REGISTRY_URL +
    '/subjects/' + topic + '-value/versions';

  var data = {
    schema: JSON.stringify(schema),
  };
  return axios({
    url: schemaCreateUrl,
    method: 'post',
    headers: {
      'Content-Type': 'application/vnd.schemaregistry.v1+json',
    },
    data: data,
  })
    .catch(function(err) {
      console.error('Axios SR creation failed:', err);
      throw err;
    });
});

/** @type {Object} simple logger */
testLib.log = {
  info: function() {
    let args = Array.prototype.splice.call(arguments, 0);
    console.log('INFO:', args.join(' '));
  },
  error: function() {
    let args = Array.prototype.splice.call(arguments, 0);
    console.log('ERROR:', args.join(' '));
  },
};

/**
 * Have a Cooldown period between tests.
 *
 * @param {number} seconds cooldown in seconds.
 * @return {Function} use is beforeEach().
 */
testLib.cooldown = function(seconds) {
  return function(done) {
    setTimeout(done, seconds);
  };
};
