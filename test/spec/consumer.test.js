/**
 * @fileOverview Test produce and consume messages using kafka-avro.
 */
const crypto = require('crypto');

const Promise = require('bluebird');
const chai = require('chai');
const expect = chai.expect;

const testLib = require('../lib/test.lib');

function noop() {
}

const Student = /** @class */ (function () {
  function Student(firstName, middleInitial, lastName) {
    this.firstName = firstName;
    this.middleInitial = middleInitial;
    this.lastName = lastName;
    this.fullName = firstName + ' ' + middleInitial + ' ' + lastName;
  }

  return Student;
}());

const Teacher = /** @class */ (function () {
  function Teacher(firstName, lastName, profession) {
    this.firstName = firstName;
    this.lastName = lastName;
    this.profession = profession;
  }

  return Teacher;
}());

function getRandomInt(max) {
  return Math.floor(Math.random() * Math.floor(max));
}

function studentEquals(dataValue, student) {
  expect(dataValue.constructor.name).to.equal(student.constructor.name);
  expect(dataValue.firstName).to.equal(student.firstName);
  expect(dataValue.lastName).to.equal(student.lastName);
  expect(dataValue.middleInitial).to.equal(student.middleInitial);
  expect(dataValue.fullName).to.equal(student.fullName);
}

function teacherEquals(dataValue, teacher) {
  expect(dataValue.constructor.name).to.equal(teacher.constructor.name);
  expect(dataValue.firstName).to.equal(teacher.firstName);
  expect(dataValue.profession).to.equal(teacher.profession);
}

describe('Consume', function () {
  testLib.init();

  beforeEach(function () {
    this.consOpts = {
      'group.id': 'testKafkaAvro' + crypto.randomBytes(20).toString('hex'),
      'enable.auto.commit': true,
    };

    testLib.log.info('beforeEach 1 on Consume');
    return this.kafkaAvro.getConsumer(this.consOpts)
      .bind(this)
      .then(function (consumer) {
        testLib.log.info('beforeEach 1 on Consume: Got consumer');
        this.consumer = consumer;
      });
  });

  beforeEach(function () {
    testLib.log.info('beforeEach 2 on Consume');
    return this.kafkaAvro.getProducer({
      'dr_cb': true,
    })
      .bind(this)
      .then(function (producer) {
        testLib.log.info('beforeEach 2 on Consume: Got producer');
        this.producer = producer;

        producer.on('event.log', function (log) {
          testLib.log.info('producer log:', log);
        });

        //logging all errors
        producer.on('error', function (err) {
          testLib.log.error('Error from producer:', err);
        });

        producer.on('delivery-report', function (err, report) {
          testLib.log.info('delivery-report:' + JSON.stringify(report));
          this.gotReceipt = true;
        }.bind(this));

        testLib.log.info('beforeEach 2 on Consume: Done');

      });
  });

  afterEach(function () {
    testLib.log.info('afterEach 1 on Consume: Disposing...');
    return this.kafkaAvro.dispose()
      .then(function () {
        testLib.log.info('afterEach 1 on Consume: Disposed');
      });
  });

  describe('Consumer direct "on"', function () {

    beforeEach(function () {
      return new Promise(function (resolve, reject) {
        this.consumer.on('ready', function () {
          testLib.log.debug('getConsumer() :: Got "ready" event.');
          resolve();
        });

        this.consumer.connect({}, function (err) {
          if (err) {
            testLib.log.error('getConsumer() :: Connect failed:', err);
            reject(err);
            return;
          }
          testLib.log.debug('getConsumer() :: Got "connect()" callback.');
          resolve(); // depend on Promises' single resolve contract.
        });
      }.bind(this));
    });

    it('should produce and consume a message using consume "on"', function (done) {
      let produceTime = 0;

      const message = {
        name: 'Thanasis',
        long: 540,
      };

      const key = 'test-key';

      // //start consuming messages
      this.consumer.subscribe([testLib.topic]);
      this.consumer.consume();

      this.consumer.on('data', function (rawData) {
        const dataValue = rawData.parsed;
        const dataKey = rawData.parsedKey;
        const diff = Date.now() - produceTime;
        testLib.log.info('Produce to consume time in ms:', diff);
        expect(dataValue).to.have.keys([
          'name',
          'long',
        ]);
        expect(dataValue.name).to.equal(message.name);
        expect(dataValue.long).to.equal(message.long);

        expect(dataKey).to.equal(key);

        done();
      }.bind(this));

      setTimeout(() => {
        produceTime = Date.now();
        this.producer.produce(testLib.topic, -1, message, key);
      }, 10000);
    });

    it('should produce and consume a multi type message using consume "on"', function (done) {
      const teacher = new Teacher('TeacherValue', `${getRandomInt(1000)}`, `${getRandomInt(1000)}`);
      const teacherKey = new Teacher('TeacherKey', `${getRandomInt(1000)}`, `${getRandomInt(1000)}`);
      const student = new Student('StudentValue', `${getRandomInt(1000)}`, '' + `${getRandomInt(1000)}`);
      const studentKey = new Student('StudentKey', `${getRandomInt(1000)}`, '' + `${getRandomInt(1000)}`);

      this.consumer.subscribe([testLib.topicTree]);
      this.consumer.consume();
      let receivedMessages = 0;
      this.consumer.on('data', function (rawData) {
        receivedMessages++;
        const k = rawData.parsedKey;
        const v = rawData.parsed;

        if (v.constructor.name === 'Teacher') {
          teacherEquals(k, teacherKey);
          teacherEquals(v, teacher);
        } else {
          studentEquals(k, studentKey);
          studentEquals(v, student);
        }
        if (receivedMessages === 2) {
          done();
        }
      }.bind(this));

      setTimeout(() => {
        this.producer.produce(testLib.topicTree, -1, teacher, teacherKey);
        this.producer.produce(testLib.topicTree, -1, student, studentKey);
      }, 10000);
    });

    it('should produce and consume a message using consume "on" with timestamp when provided', function (done) {
      let produceTime = Date.parse('04 Dec 2015 00:12:00 GMT'); //use date in the past to guarantee we don't get Date.now()

      const message = {
        name: 'Thanasis',
        long: 540,
      };

      // //start consuming messages
      this.consumer.subscribe([testLib.topic]);
      this.consumer.consume();

      this.consumer.on('data', function (rawData) {
        expect(rawData.timestamp).to.equal(produceTime);
        done();
      }.bind(this));

      setTimeout(() => {
        produceTime = Date.now();
        this.producer.produce(testLib.topic, -1, message, 'key', produceTime);
      }, 10000);
    });

    it('should produce and consume a message using consume "on", on a non Schema Registry topic', function (done) {
      let produceTime = 0;

      const topicName = testLib.testNodeKafkaWithoutSR;

      const message = {
        name: 'Thanasis',
        long: 540,
      };

      const key = 'no-schema-key';

      // //start consuming messages
      this.consumer.subscribe([topicName]);
      this.consumer.consume();

      this.consumer.on('data', function (rawData) {
        const dataValue = rawData.parsed;
        const dataKey = rawData.parsedKey;
        const diff = Date.now() - produceTime;
        testLib.log.info('Produce to consume time in ms:', diff);
        expect(dataValue).to.have.keys([
          'name',
          'long',
        ]);
        expect(dataValue.name).to.equal(message.name);
        expect(dataValue.long).to.equal(message.long);

        expect(dataKey).to.equal(key);

        done();
      }.bind(this));

      setTimeout(() => {
        testLib.log.info('Producing on non SR topic...');
        produceTime = Date.now();
        this.producer.produce(topicName, -1, message, key);
      }, 10000);
    });

    it('should produce and consume on two topics using a single consumer', function (done) {
      let produceTime = 0;

      const message = {
        name: 'Thanasis',
        long: 540,
      };

      const key = 'two-topics';

      // //start consuming messages
      this.consumer.subscribe([
        testLib.topic,
        testLib.topicTwo,
      ]);
      this.consumer.consume();

      let receivedOne = false;
      let receivedTwo = false;

      this.consumer.on('data', function (rawData) {
        if (rawData.topic === testLib.topic) {
          receivedOne = true;
        } else {
          receivedTwo = true;
        }

        const dataValue = rawData.parsed;
        const dataKey = rawData.parsedKey;
        const diff = Date.now() - produceTime;
        testLib.log.info('Produce to consume time in ms:', diff);
        expect(dataValue).to.have.keys([
          'name',
          'long',
        ]);
        expect(dataValue.name).to.equal(message.name);
        expect(dataValue.long).to.equal(message.long);
        expect(dataKey).to.equal(key);

        if (receivedOne && receivedTwo) {
          done();
        }
      }.bind(this));

      setTimeout(() => {
        produceTime = Date.now();
        this.producer.produce(testLib.topicTwo, -1, message, key);
        this.producer.produce(testLib.topic, -1, message, key);
      }, 10000);
    });
  })
  ;

  describe('Consume using Streams', function () {
    it('should produce and consume a message using streams on two topics', function (done) {
      let produceTime = 0;

      const message = {
        name: 'Thanasis',
        long: 540,
      };

      const key = 'key-stream';

      let isDone = false;

      this.kafkaAvro.getConsumerStream(this.consOpts, {'enable.auto.commit': true}, {topics: [testLib.topic, testLib.topicTwo]})
        .then(function (consumerStream) {
          consumerStream.on('error', noop);

          consumerStream.on('data', function (dataRaw) {
            const dataValue = dataRaw.parsed;
            const dataKey = dataRaw.parsedKey;
            const diff = Date.now() - produceTime;
            testLib.log.info('Produce to consume time in ms:', diff);
            expect(dataValue).to.have.keys([
              'name',
              'long',
            ]);

            expect(dataValue.name).to.equal(message.name);
            expect(dataValue.long).to.equal(message.long);
            expect(dataKey).to.equal(key);

            if (!isDone) {
              consumerStream.consumer.disconnect();
              done();
            }
            isDone = true;
          });
        });

      setTimeout(() => {
        produceTime = Date.now();
        this.producer.produce(testLib.topicTwo, -1, message, key);
        this.producer.produce(testLib.topic, -1, message, key);
      }, 10000);
    });

    it('should produce and consume a message using streams on a not SR topic', function (done) {
      let produceTime = 0;

      const topicName = testLib.testNodeKafkaWithoutSR;

      const message = {
        name: 'Thanasis',
        long: 540,
      };

      const key = 'not-sr-key';

      this.kafkaAvro.getConsumerStream(this.consOpts, {'enable.auto.commit': true}, {topics: topicName})
        .then(function (consumerStream) {
          consumerStream.on('error', noop);

          consumerStream.on('data', function (dataRaw) {
            const dataValue = dataRaw.parsed;
            const dataKey = dataRaw.parsedKey;
            const diff = Date.now() - produceTime;
            testLib.log.info('Produce to consume time in ms:', diff);
            expect(dataValue).to.have.keys([
              'name',
              'long',
            ]);

            expect(dataValue.name).to.equal(message.name);
            expect(dataValue.long).to.equal(message.long);

            expect(dataKey).to.equal(key);

            consumerStream.consumer.disconnect();
            done();
          });
        });

      setTimeout(() => {
        produceTime = Date.now();
        this.producer.produce(topicName, -1, message, key);
      }, 10000);
    });
  });
})
;
