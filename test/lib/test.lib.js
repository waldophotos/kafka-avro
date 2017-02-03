/*
 * @fileOverview Main testing helper lib.
 */
var axios = require('axios');
var Promise = require('bluebird');
// const fmt = require('bunyan-format');
// stream: fmt({
//   outputMode: 'short',
//   levelInString: true
// }),

var KafkaAvro = require('../..');

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
  beforeEach(function() {
    if (testBoot) {
      return;
    }
    testBoot = true;

    this.timeout(180000); // wait up to 3' for the SR to come up

    return Promise.all([
      testLib.registerSchema(testLib.topic, schemaFix),
      testLib.registerSchema(testLib.topicTwo, schemaFix),
    ]);
  });

  beforeEach(function() {
    this.kafkaAvro = new KafkaAvro({
      kafkaBroker: testLib.KAFKA_BROKER_URL,
      schemaRegistry: testLib.KAFKA_SCHEMA_REGISTRY_URL,
    });
    console.log('test.beforeEach 2: Invoking kafkaAvro.init()...');
    return this.kafkaAvro.init()
      .then(function() {
        console.log('test.beforeEach 2: kafkaAvro.init() done!');
      });
  });

};

/**
 * Register a schema on SR.
 *
 * @param {string} The topic.
 * @param {schema} Object The schema to register.
 * @param {number=} retries how many times has retried.
 * @return {Promise} A Promise.
 */
testLib.registerSchema = Promise.method(function(topic, schema, retries) {
  var schemaCreateUrl = testLib.KAFKA_SCHEMA_REGISTRY_URL +
    '/subjects/' + topic + '-value/versions';

  var data = {
    schema: JSON.stringify(schema),
  };

  retries = retries || 0;

  console.log('TEST :: Registering schema:', topic, 'on SR:', schemaCreateUrl);

  return axios({
    url: schemaCreateUrl,
    method: 'post',
    headers: {
      'Content-Type': 'application/vnd.schemaregistry.v1+json',
    },
    data: data,
  })
    .catch(function(err) {
      console.error('Axios SR creation failed:', retries, err.message);
      retries++;
      return new Promise(function(resolve) {
        setTimeout(function() {
          testLib.registerSchema(topic, schema, retries)
            .then(resolve);
        }, 1000);
      });
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
