/*
 * @fileOverview Main testing helper lib.
 */
var axios = require('axios');

var schemaFix = require('../fixtures/schema.fix');

var testLib = module.exports = {};

testLib.KAFKA_SCHEMA_REGISTRY_URL = 'http://schema-registry-confluent.internal.dev.waldo.photos';
testLib.KAFKA_BROKER_URL = 'broker-1.service.consul:9092,broker-3.service.consul:9092,broker-2.service.consul:9092';
testLib.topic = schemaFix.name;

// curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
//     --data '{"schema": "{\"name\": \"string\", \"long\": \"long\"}"}' \
//      http://schema-registry-confluent.internal.dev.waldo.photos/subjects/test-thanpolas-Kafka/versions

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
    var schemaCreateUrl = testLib.KAFKA_SCHEMA_REGISTRY_URL +
      '/subjects/' + testLib.topic + '-value/versions';

    var data = {
      schema: JSON.stringify(schemaFix),
    };
    console.log('DATA:', data);
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

  // # Register a new version of a schema under the subject "Kafka-value"
  // $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  //     --data '{"schema": "{\"type\": \"string\"}"}' \
  //      http://localhost:8081/subjects/Kafka-value/versions
  //   {"id":1}
};

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
