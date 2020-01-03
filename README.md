# kafka-avro

> Node.js bindings for librdkafka with Avro schema serialization.

[![CircleCI](https://circleci.com/gh/waldophotos/kafka-avro/tree/master.svg?style=svg)](https://circleci.com/gh/waldophotos/kafka-avro/tree/master)

The kafka-avro library is a wrapper that combines the [node-rdkafka][node-rdkafka] and [avsc][avsc] libraries to allow for Production and Consumption of messages on kafka validated and serialized by Avro.

## Install

Install the module using NPM:

```
npm install kafka-avro --save
```

## Documentation

The kafka-avro library operates in the following steps:

1. You provide your Kafka Brokers and Schema Registry (SR) Url to a new instance of kafka-avro.
1. You initialize kafka-avro, that will tell the library to query the SR for all registered schemas, evaluate and store them in runtime memory.
1. kafka-avro will then expose the `getConsumer()` and `getProducer()` methods, which both return instances of the corresponding Constructors from the [node-rdkafka][node-rdkafka] library.

The instances of "node-rdkafka" that are returned by kafka-avro are hacked so as to intercept produced and consumed messages and run them by the Avro de/serializer along with Confluent's Schema Registry Magic Byte and Schema Id.

You are highly encouraged to read the ["node-rdkafka" documentation](https://blizzard.github.io/node-rdkafka/current/), as it explains how you can use the Producer and Consumer instances as well as check out the [available configuration options of node-rdkafka](https://github.com/edenhill/librdkafka/blob/2213fb29f98a7a73f22da21ef85e0783f6fd67c4/CONFIGURATION.md).

### node-rdkafka CODES

The `Kafka.CODES` enumeration of constant values provided by the "node-rdkafka" library is also available as a static var at:

```js
const KafkaAvro = require('kafka-avro');

console.log(KafkaAvro.CODES);
```

### Initialize kafka-avro

```js
const KafkaAvro = require('kafka-avro');

const kafkaAvro = new KafkaAvro({
    kafkaBroker: 'localhost:9092',
    schemaRegistry: 'http://localhost:8081',
});

// Query the Schema Registry for all topic-schema's
// fetch them and evaluate them.
kafkaAvro.init()
    .then(function() {
        console.log('Ready to use');
    });
```

### Kafka-avro options

When instantiating kafka-avro you may pass the following options:

* `kafkaBroker` **String REQUIRED** The url or comma delimited strings pointing to your kafka brokers.
* `schemaRegistry` **String REQUIRED** The url to the Schema Registry.
* `topics` **Array of Strings** You may optionally define specific topics to be fetched by kafka-avro vs fetching schemas for all the topics which is the default behavior.
* `fetchAllVersions` **Boolean** Set to true to fetch all versions for each topic, use it when updating of schemas is often in your environment.
* `fetchRefreshRate` **Number** The pooling time (in seconds) to the schemas be fetched and updated in background. This is useful to keep with schemas changes in production. The default value is `0` seconds (disabled).
* `parseOptions` **Object** Schema parse options to pass to `avro.parse()`. `parseOptions.wrapUnions` is set to `true` by default.
* `httpsAgent` **Object** initialized [https Agent class](https://nodejs.org/api/https.html#https_class_https_agent)
* `shouldFailWhenSchemaIsMissing` **Boolean** Set to true if producing a message for which no AVRO schema can be found should throw an error
* `keySubjectStrategy` **String** A SubjectNameStrategy for key. It is used by the Avro serializer to determine the subject name under which the event record schemas should be registered in the schema registry. The default is TopicNameStrategy. Allowed values are [TopicRecordNameStrategy, TopicNameStrategy, RecordNameStrategy]
* `valueSubjectStrategy` **String** A SubjectNameStrategy for value. It is used by the Avro serializer to determine the subject name under which the event record schemas should be registered in the schema registry. The default is TopicNameStrategy. Allowed values are [TopicRecordNameStrategy, TopicNameStrategy, RecordNameStrategy]

### Producer

> NOTICE: You need to initialize kafka-avro before you can produce or consume messages.

By invoking the `kafkaAvro.getProducer()` method, kafka-avro will instantiate a Producer, make it connect and wait for it to be ready before the promise is resolved.

```js
kafkaAvro.getProducer({
  // Options listed bellow
})
    // "getProducer()" returns a Bluebird Promise.
    .then(function(producer) {
        const topicName = 'test';

        producer.on('disconnected', function(arg) {
          console.log('producer disconnected. ' + JSON.stringify(arg));
        });

        const value = {name:'John'};
        const key = 'key';

        // if partition is set to -1, librdkafka will use the default partitioner
        const partition = -1;
        producer.produce(topicName, partition, value, key);
    })
```

What kafka-avro basically does is wrap around node-rdkafka and intercept the produce method to validate and serialize the message.

* [node-rdkafka Producer Tutorial](https://blizzard.github.io/node-rdkafka/current/tutorial-producer_.html)
* [Full list of Producer's options](https://github.com/edenhill/librdkafka/blob/2213fb29f98a7a73f22da21ef85e0783f6fd67c4/CONFIGURATION.md)

### Consumer

> NOTICE: You need to initialize kafka-avro before you can produce or consume messages.

By invoking the `kafkaAvro.getConsumer()` method, kafka-avro will instantiate a Consumer, listen on log, error and disconnect events and return it to you. Depending on the consuming pattern you follow you may or may not need to perform a `connect()`.

#### Consumer using events to consume

When consuming topics using the `data` event you will need to perform a `connect()` as per node-rdkafka documentation:

```js
kafkaAvro.getConsumer({
  'group.id': 'librd-test',
  'socket.keepalive.enable': true,
  'enable.auto.commit': true,
})
  // the "getConsumer()" method will return a bluebird promise.
  .then(function(consumer) {
    // Perform a consumer.connect()
    return new Promise(function (resolve, reject) {
      consumer.on('ready', function() {
        resolve(consumer);
      });

      consumer.connect({}, function(err) {
        if (err) {
          reject(err);
          return;
        }
        resolve(consumer); // depend on Promises' single resolve contract.
      });
    });
  })
  .then(function(consumer) {
    // Subscribe and consume.
    const topicName = 'test';
    consumer.subscribe([topicName]);
    consumer.consume();
    consumer.on('data', function(rawData) {
      console.log('data:', rawData);
    });
  });
```

#### Consumer using streams to consume

```js
kafkaAvro.getConsumerStream({
  'group.id': 'librd-test',
  'socket.keepalive.enable': true,
  'enable.auto.commit': true,
},
{
  'request.required.acks': 1
},
{
  'topics': 'test'
})
  .then(function(stream) {
      stream.on('error', function(err) {
        if (err) console.log(err);
        process.exit(1);
      });

      stream.on('data', function (rawData) {
          console.log('data:', rawData)
      });

      stream.on('error', function(err) {
        console.log(err);
        process.exit(1);
      });

      stream.consumer.on('event.error', function(err) {
        console.log(err);
      })
  });
```


Same deal here, thin wrapper around node-rdkafka and deserialize incoming messages before they reach your consuming method.

* [node-rdkafka Consumer Tutorial](https://blizzard.github.io/node-rdkafka/current/tutorial-consumer.html)
* [Full list of Consumer's options](https://github.com/edenhill/librdkafka/blob/2213fb29f98a7a73f22da21ef85e0783f6fd67c4/CONFIGURATION.md)

#### Consumer Data Object

kafka-avro intercepts all incoming messages and augments the object with two more properties named `parsed` and `parsedKey`, which contained the avro deserialized object's value and key. Here is a breakdown of the properties included in the `message` object you receive when consuming messages:

* `value` **Buffer** The raw message buffer from Kafka.
* `size` **Number** The size of the message.
* `key` **String|Number** Partioning key used.
* `topic` **String** The topic this message comes from.
* `offset` **Number** The Kafka offset.
* `partition` **Number** The kafka partion used.
* `parsed` **Object** The avro deserialized value as a JS Object ltieral.
* `schemaId` **Number** The Registry Value Schema id of the consumed message.
* `parsedKey` **Object** The avro deserialized key as a JS Object ltieral.
* `schemaIdKey` **Number** The Registry Key Schema id of the consumed message.

The KafkaAvro instance also provides the following methods:

### Support for several event types in the same topic
Kafka Avro can support several events types in the same topic. This requires using TopicRecordNameStrategy strategy.

```js
const KafkaAvro = require('kafka-avro');

const kafkaAvro = new KafkaAvro({
    kafkaBroker: 'localhost:9092',
    schemaRegistry: 'http://localhost:8081',
    keySubjectStrategy: "TopicRecordNameStrategy",
    valueSubjectStrategy: "TopicRecordNameStrategy",
});

// Query the Schema Registry for all topic-schema's
// fetch them and evaluate them.
kafkaAvro.init()
    .then(function() {
        console.log('Ready to use');
    });
```

You can read more about this here : https://www.confluent.io/blog/put-several-event-types-kafka-topic/

### Logging

The Kafka Avro library logs messages using the [Bunyan logger](https://github.com/trentm/node-bunyan/). To enable logging you will have to define at least one of the needed ENV variables:

* `KAFKA_AVRO_LOG_LEVEL` Set it a valid Bunyan log level value to activate console logging (Typically you'd need either `info` or `debug` as values.)
* `KAFKA_AVRO_LOG_NO_COLORS` Set this to any value to disable color when logging.


#### KafkaAvro.getLogger()

> **WARNING** The logger will not emit any messages as it was expected, there is an [open issue on Bunyan's repository](https://github.com/trentm/node-bunyan/issues/479) pending a solution on this. So no logging for now.


> **NOTICE** This is a **static method** on the `KafkaAvro` constructor, not the instance. Therefore there is a single logger instance for the whole runtime.

**Returns** {Bunyan.Logger} [Bunyan logger](https://github.com/trentm/node-bunyan/) instance.

```js
const KafkaAvro = require('kafka-avro');
const fmt = require('bunyan-format');

const kafkaLog  = KafkaAvro.getLogger();

kafkaLog.addStream({
    type: 'stream',
    stream: fmt({
        outputMode: 'short',
        levelInString: true,
    }),
    level: 'info',
});
```

Read more about the [bunyan-format package](https://github.com/thlorenz/bunyan-format).

### Helper Methods

#### kafkaAvro.serialize(type, schemaId, value)

Serialize the provided value with Avro, including the magic Byte and schema id provided.

**Returns** {Buffer} Serialized buffer message.

* `type` {avsc.Type} The avro type instance.
* `schemaId` {number} The Schema Id in the SR.
* `value` {*} Any value to serialize.

#### kafkaAvro.deserialize(type, message)

Deserialize the provided message, expects a message that includes Magic Byte and schema id.

**Returns** {*} Deserialized message.

* `type` {avsc.Type} The avro type instance.
* `message` {Buffer} Message in byte code.

## Testing

You can use `docker-compose up` to up all the stack before you call your integration tests with `npm test`. How the integration tests are outside the containers, you will need set you `hosts` file to :

```
127.0.0.1 kafka schema-registry
```


## Releasing

1. Update the changelog bellow.
1. Ensure you are on master.
1. Type: `grunt release`
    * `grunt release:minor` for minor number jump.
    * `grunt release:major` for major number jump.

## Release History
- **3.0.1**, *03 Jan 2020*
    - Fix a bug to custom strategies `keySubjectStrategy` and `valueSubjectStrategy` - they were not working as expected. The default behavior was not impacted.
- **3.0.0**, *19 Sep 2019* 
    - Adds support for `RecordNameStrategy`(io.confluent.kafka.serializers.subject.RecordNameStrategy) and `TopicRecordNameStrategy`(io.confluent.kafka.serializers.subject.TopicRecordNameStrategy)
schema subject name strategy. The purpose of the new strategies is to allow to put several event types in the same kafka topic (https://www.confluent.io/blog/put-several-event-types-kafka-topic) (by [pleszczy](github.com/pleszczy))
    - Adds new optional config params `keySubjectStrategy` and `valueSubjectStrategy` to configure schema subject name strategy for message key and value. Supported strategies are
      `[TopicNameStrategy, TopicRecordNameStrategy, RecordNameStrategy]` (by [pleszczy](github.com/pleszczy))
- **2.0.0**, *09 Sep 2019*
    - **Breaking change** Update version of node-rdkafka to ~v2.7.1 - this version uses `librdkafka` v1.1.0
- **1.2.1**, *06 Sep 2019*
    - Adds a new the optional config param `shouldFailWhenSchemaIsMissing` to let the producer fail when no schema could be found (instead of producing as JSON) (by [bfncs](https://github.com/bfncs))
- **1.2.0**, *03 March 2019*
    - Fixed cases when both key and value schemas were available, but the value was being serialized using the key schema (by [macabu](https://github.com/macabu))
    - Support for (de)serialization of keys. Added `parsedKey` and `schemaIdKey` to the consumer data object (by [macabu](https://github.com/macabu))
- **1.1.4**, *27 February 2019*
    - Adding clearInterval on periodic schema update, when dispose() called (by [mbieser](https://github.com/mbieser))
- **1.1.3**, *02 January 2019*
    - Fixing broken `fetchAllVersions` feature (by [ricardohbin](https://github.com/ricardohbin))
- **1.1.2**, *08 November 2018*
    - Handle topics which use identical schemas (by [scottwd9](https://github.com/scottwd9))
- **1.1.1**, *23 August 2018*
    - Set `schemaMeta` for key schemas also (by [eparreno](https://github.com/eparreno))
- **1.1.0**, *06 August 2018*
    - Updating to node-rdkafka v2.3.4 (by [ricardohbin](https://github.com/ricardohbin))
- **1.0.6**, *11 July 2018*
    - Adding the `fetchRefreshRate` parameter, to set a way to update the schemas after the app initialization. (by [ricardohbin](https://github.com/ricardohbin))
- **1.0.5**, *27 June 2018*
    - Fixes kafka-producer to pass timestamp and opaque correctly (by [javierholguera](https://github.com/javierholguera))
- **1.0.4**, *30 May 2018*
    - Allowing schema-registry urls with paths (by [941design](https://github.com/941design))
- **1.0.3**, *28 May 2018*
    - Added support for providing configured https.Agent to support schema-registry using private certificates. (by [scottchapman](https://github.com/scottchapman))
- **1.0.2**, *23 Apr 2018*
    - Using URL module instead of strings to resolve schema registry urls. (by [ricardohbin](https://github.com/ricardohbin))
- **1.0.1**, *03 Apr 2018*
    - **Breaking change**: New consumer stream API, changes in producer and fixing all integration tests (by [ricardohbin](https://github.com/ricardohbin)).
- **1.0.0**, *28 Mar 2018*
    - Updating docs and the libs avsc (v5.2.3) and node-rdkafka (v2.1.3) (by [ricardohbin](https://github.com/ricardohbin)).
- **v0.8.1**, *01 Feb 2018*
    - Allow customization of AVSC parse options (thank you [dapetcu21](https://github.com/dapetcu21)).
- **v0.8.0**, *27 Nov 2017*
    - Provides option to fetch all past versions of a topic (thank you [CMTegner](https://github.com/CMTegner)).
    - Provides option to select which topics should be fetched.
- **v0.7.0**, *24 Feb 2017*
    - New mechanism in deserializing messages, the schema id will now be parsed from the message and if this schema id is found in the local registry kafka-avro will use that schema to deserialize. If it is not found then it will use the provided schema type, which would be the last known for the topic.
    - Added `schemaId` property on the consumed messages.
- **v0.6.4**, *23 Feb 2017*
    - Catch errors thrown by the deserializer.
- **v0.6.3**, *20 Feb 2017*
    - Will now pass topic options on getConsumer and getProducer methods.
- **v0.6.2**, *18 Feb 2017*
    - Fixed Magic Byte encoding for large payloads.
- **v0.6.1**, *17 Feb 2017*
    - Don't log consumer log messages (logger, not kafka).
- **v0.6.0**, *16 Feb 2017*
    - **Breaking change**: Consumers will no longer auto-connect, you are required to perform the `connect()` method manually, check the docs.
    - Refactored logging, you can now enable it using an env var, check docs on Logging.
- **v0.5.1**, *15 Feb 2017*
    - Catch errors from `connect()` callbacks for both Consumer and Producer.
- **v0.5.0**, *15 Feb 2017*
    - Upgrade to node-rdkafka `0.7.0-ALPHA.3` which changes the consumer API by decoupling subscribing from consuming.
- **v0.4.3**, *15 Feb 2017*
    - Locked this version to `0.7.0-ALPHA.2` of node-rdkafka which broke BC in `0.7.0-ALPHA.3`.
- **v0.4.2**, *15 Feb 2017*
    - Fixed `connect()` invocation for consumers and producers.
- **v0.4.1**, *10 Feb 2017*
    - Fixed relaying Kafka consumer logs.
- **v0.4.0**, *03 Feb 2017*
    - Refactored all logging to use a central Bunyan logger that is now provided through the static method `KafkaAvro.getLogger()`.
    - Allowed for an Array of strings as topic argument for Consumer's `getReadStream()` method.
- **v0.3.0**, *01 Feb 2017*
    - Now force uses Magic Byte in any occasion when de/serializing.
    - Exposed `serialize()` and `deserialize()` methods.
    - Fixed de/serializing of topics not found in the Schema Registry.
    - Tweaked log namespaces, still more work required to eventize them.
- **v0.2.0**, *30 Jan 2017*
    - Added Confluent's Magic Byte support when encoding and decoding messages.
- **v0.1.2**, *27 Jan 2017*
    - Suppress schema parsing errors.
- **v0.1.1**, *27 Jan 2017*
    - Fix signature of `getConsumer()` method.
- **v0.1.1**, *27 Jan 2017*
    - Expose CODES enum from node-rdkafka.
    - Write more docs, add the event based consuming method.
- **v0.1.0**, *26 Jan 2017*
    - First fully working release.
- **v0.0.1**, *25 Jan 2017*
    - Big Bang

## License

Copyright Waldo, Inc. [Licensed under the MIT](/LICENSE).

[avsc]: https://github.com/mtth/avsc
[node-rdkafka]: https://github.com/Blizzard/node-rdkafka
