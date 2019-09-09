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

const schemaStudent = require('../fixtures/schema-student.fix');
const schemaTeacher = require('../fixtures/schema-teacher.fix');

const testLib = module.exports = {};

testLib.log = bunyan.createLogger({
  name: 'KafkaAvroTest',
  level: 'trace',
  stream: fmt({
    outputMode: 'long',
    levelInString: true,
  }),
});

testLib.KAFKA_SCHEMA_REGISTRY_URL = 'http://schema-registry:8081';
testLib.KAFKA_BROKER_URL = 'kafka:9092';

testLib.topic = schemaFix.name;
testLib.topicTwo = schemaTwoFix.name;
testLib.topicTree = `${schemaTeacher.name}-${schemaStudent.name}`;
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
      testLib.registerSchema(`${testLib.topic}-Object`, schemaFix, 'value'),
      testLib.registerSchema(`${testLib.topicTwo}-Object`, schemaTwoFix, 'value'),
      testLib.registerSchema(`${testLib.topicThreeWithDuplicateSchema}-Object`, schemaFix, 'value'),

      testLib.registerSchema(`${testLib.topicTree}-Teacher`, schemaTeacher, 'key'),
      testLib.registerSchema(`${testLib.topicTree}-Teacher`, schemaTeacher, 'value'),
      testLib.registerSchema(`${testLib.topicTree}-Student`, schemaStudent, 'key'),
      testLib.registerSchema(`${testLib.topicTree}-Student`, schemaStudent, 'value'),

      testLib.registerSchema(`${testLib.topic}-String`, keySchemaFix, 'key'),
      testLib.registerSchema(`${testLib.topicTwo}-Object`, keySchemaFix, 'key'),
      testLib.registerSchema(`${testLib.topicThreeWithDuplicateSchema}-Object`, keySchemaFix, 'key'),
    ]);
  });

  beforeEach(function () {
    let kafkaAvro = new KafkaAvro({
      kafkaBroker: testLib.KAFKA_BROKER_URL,
      schemaRegistry: testLib.KAFKA_SCHEMA_REGISTRY_URL,
      keySubjectStrategy: 'TopicRecordNameStrategy',
      valueSubjectStrategy: 'TopicRecordNameStrategy',
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

      if (retries > 20) {
        // process.exit();
        throw new Error('Environment has not started correctly');
      }

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
