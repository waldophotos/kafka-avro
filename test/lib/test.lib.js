/*
 * @fileOverview Main testing helper lib.
 */
var axios = require('axios');

var tester = module.exports = {};

tester.KAFKA_SCHEMA_REGISTRY_URL = 'http://schema-registry-confluent.internal.dev.waldo.photos';
tester.KAFKA_BROKER_URL = 'http://rest-proxy-confluent.internal.dev.waldo.photos';
tester.topic = 'test-node-kafka-avro';

var testBoot = false;

/**
 * Require from all test scripts, prepares kafka for testing.
 *
 */
tester.init = function() {
  if (testBoot) {
    return;
  }
  testBoot = true;

  beforeEach(function() {
    var schemaCreateUrl = tester.KAFKA_SCHEMA_REGISTRY_URL +
      '/subjects/' + tester.topic + '-value/versions';

    return axios({
      url: schemaCreateUrl,
      method: 'post',
      headers: {
        'Content-Type': 'application/vnd.schemaregistry.v1+json',
      },
      data: {
        name: {
          type: 'string',
        },
        long: {
          type: 'long',
        },
      },
    });
  });

  // # Register a new version of a schema under the subject "Kafka-value"
  // $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  //     --data '{"schema": "{\"type\": \"string\"}"}' \
  //      http://localhost:8081/subjects/Kafka-value/versions
  //   {"id":1}
};

/** @type {Object} simple logger */
tester.log = {
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
tester.cooldown = function(seconds) {
  return function(done) {
    setTimeout(done, seconds);
  };
};
