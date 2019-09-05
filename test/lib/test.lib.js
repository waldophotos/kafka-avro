/*
 * @fileOverview Main testing helper lib.
 */
const axios = require('axios');
const Promise = require('bluebird');
const bunyan = require('bunyan');

const fmt = require('bunyan-format');

// override to enable logging
process.env.KAFKA_AVRO_LOG_LEVEL = 'debug';

const KafkaAvro = require('../..');

const schemaFix = require('../fixtures/schema.fix');

const schemaTwoFix = require('../fixtures/schema-two.fix');

const keySchemaFix = require('../fixtures/key-schema.fix');

const testLib = module.exports = {};

testLib.log = bunyan.createLogger({
  name: 'KafkaAvroTest',
  level: 'trace',
  stream: fmt({
    outputMode: 'long',
    levelInString: true,
  }),
});

testLib.KAFKA_SCHEMA_REGISTRY_URL = 'http://localhost:8081';
testLib.KAFKA_BROKER_URL = 'kafka:9092';

testLib.topic = schemaFix.name;
testLib.topicTwo = schemaTwoFix.name;
testLib.topicThreeWithDuplicateSchema = schemaFix.name + '-duplicateSchema';

let testBoot = false;

/**
 * Require from all test scripts, prepares kafka for testing.
 *
 */
testLib.init = function () {
  beforeEach(function () {
    if (testBoot) {
      return;
    }
    testBoot = true;

    let kafkaAvroLog = KafkaAvro.getLogger();

    kafkaAvroLog.addStream({
      type: 'stream',
      stream: fmt({
        outputMode: 'long',
        levelInString: true,
      }),
      level: 'debug',
    }, 'debug');

    this.timeout(180000); // wait up to 3' for the SR to come up

    return Promise.all([
      testLib.registerSchema(`Object-${testLib.topic}`, schemaFix, 'value'),
      testLib.registerSchema(`Object-${testLib.topicTwo}`, schemaTwoFix, 'value'),
      testLib.registerSchema(`Object-${testLib.topicThreeWithDuplicateSchema}`, schemaFix, 'value'),

      testLib.registerSchema(`String-${testLib.topic}`, keySchemaFix, 'key'),
      testLib.registerSchema(`Object-${testLib.topicTwo}`, keySchemaFix, 'key'),
      testLib.registerSchema(`Object-${testLib.topicThreeWithDuplicateSchema}`, keySchemaFix, 'key'),
    ]);
  });

  beforeEach(function () {
    let kafkaAvro = new KafkaAvro({
      kafkaBroker: testLib.KAFKA_BROKER_URL,
      schemaRegistry: testLib.KAFKA_SCHEMA_REGISTRY_URL,
    });

    testLib.log.info('test.beforeEach 2: Invoking kafkaAvro.init()...');

    return kafkaAvro.init()
      .then(() => {
        testLib.log.info('test.beforeEach 2: kafkaAvro.init() done!');
        this.kafkaAvro = kafkaAvro;
        this.sr = kafkaAvro.sr;
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
testLib.registerSchema = Promise.method(function (topic, schema, type, retries) {
  const schemaCreateUrl = testLib.KAFKA_SCHEMA_REGISTRY_URL +
    '/subjects/' + topic + '-' + type + '/versions';

  const data = {
    schema: JSON.stringify(schema),
  };

  retries = retries || 0;

  testLib.log.info('TEST :: Registering schema subject in sr', {topic, sr: schemaCreateUrl});

  return axios({
    url: schemaCreateUrl,
    method: 'post',
    headers: {
      'Content-Type': 'application/vnd.schemaregistry.v1+json',
    },
    data: data,
  })
    .catch(function (err) {
      testLib.log.error({err}, 'Axios SR creation failed after noOfRetries', {noOfRetries: retries});
      retries++;
      return new Promise(function (resolve) {
        setTimeout(function () {
          testLib.registerSchema(topic, schema, type, retries)
            .then(resolve);
        }, 1000);
      });
    });
});

/**
 * Have a Cooldown period between tests.
 *
 * @param {number} seconds cooldown in seconds.
 * @return {Function} use is beforeEach().
 */
testLib.cooldown = function (seconds) {
  return function (done) {
    setTimeout(done, seconds);
  };
};
