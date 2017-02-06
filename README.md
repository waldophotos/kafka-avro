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
var KafkaAvro = require('kafka-avro');

console.log(KafkaAvro.CODES);
```

### Initialize kafka-avro

```js
var KafkaAvro = require('kafka-avro');

var kafkaAvro = new KafkaAvro({
    kafkaBroker: 'localhost:9092',
    schemaRegistry: 'localhost:8081',
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

### Producer

> NOTICE: You need to initialize kafka-avro before you can produce or consume messages.

By invoking the `kafkaAvro.getProducer()` method, kafka-avro will instantiate a Producer, make it connect and wait for it to be ready before the promise is resolved.

```js
kafkaAvro.getProducer({
  // Options listed bellow
})
    // "getProducer()" returns a Bluebird Promise.
    .then(function(producer) {
        var topicName = 'test';

        producer.on('disconnected', function(arg) {
          console.log('producer disconnected. ' + JSON.stringify(arg));
        });

        //Create a Topic object with any options our Producer
        //should use when producing to that topic.
        var topic = producer.Topic(topicName, {
        // Make the Kafka broker acknowledge our message (optional)
        'request.required.acks': 1
        });

        var value = new Buffer('value-' +i);
        var key = 'key';

        // if partition is set to -1, librdkafka will use the default partitioner
        var partition = -1;
        producer.produce(topic, partition, value, key);
    })
```

What kafka-avro basically does is wrap around node-rdkafka and intercept the produce method to validate and serialize the message.

* [node-rdkafka Producer Tutorial](https://blizzard.github.io/node-rdkafka/current/tutorial-producer_.html)
* [Full list of Producer's options](https://github.com/edenhill/librdkafka/blob/2213fb29f98a7a73f22da21ef85e0783f6fd67c4/CONFIGURATION.md)

### Consumer

> NOTICE: You need to initialize kafka-avro before you can produce or consume messages.

By inoking the `kafkaAvro.getConsumer()` method, kafka-avro will instantiate a Consumer, make it connect and wait for it to be ready before the promise is resolved.


#### Consumer using events to consume

```js
kafkaAvro.getConsumer({
  'group.id': 'librd-test',
  'socket.keepalive.enable': true,
  'enable.auto.commit': true,
})
  // the "getConsumer()" method will return a bluebird promise.
  .then(function(consumer) {
    var topicName = 'test';
    this.consumer.consume([topicName]);
    this.consumer.on('data', function(rawData) {
      console.log('data:', rawData);
    });
  });
```

#### Consumer using streams to consume

```js
kafkaAvro.getConsumer({
  'group.id': 'librd-test',
  'socket.keepalive.enable': true,
  'enable.auto.commit': true,
})
    // the "getConsumer()" method will return a bluebird promise.
    .then(function(consumer) {
        // Topic Name can be a string, or an array of strings
        var topicName = 'test';

        var stream = consumer.getReadStream(topicName, {
          waitInterval: 0
        });

        stream.on('error', function() {
          process.exit(1);
        });

        consumer.on('error', function(err) {
          console.log(err);
          process.exit(1);
        });

        stream.on('data', function(message) {
            console.log('Received message:', message);
        });
    });
```

Same deal here, thin wrapper around node-rdkafka and deserialize incoming messages before they reach your consuming method.

* [node-rdkafka Consumer Tutorial](https://blizzard.github.io/node-rdkafka/current/tutorial-consumer.html)
* [Full list of Consumer's options](https://github.com/edenhill/librdkafka/blob/2213fb29f98a7a73f22da21ef85e0783f6fd67c4/CONFIGURATION.md)

#### Consumer Data Object

kafka-avro intercepts all incoming messages and augments the object with one more property named `parsed` which contained the avro deserialized object. Here is a breakdown of the properties included in the `message` object you receive when consuming messages:

* `value` **Buffer** The raw message buffer from Kafka.
* `size` **Number** The size of the message.
* `key` **String|Number** Partioning key used.
* `topic` **String** The topic this message comes from.
* `offset` **Number** The Kafka offset.
* `partition` **Number** The kafka partion used.
* `parsed` **Object** The avro deserialized message as a JS Object ltieral.

### Helper Methods

The KafkaAvro instance also provides the following methods:

#### KafkaAvro.getLogger()

> **NOTICE** This is a **static method** on the `KafkaAvro` constructor, not the instance. Therefore there is a single logger instance for the whole runtime.

**Returns** {Bunyan.Logger} [Bunyan logger](https://github.com/trentm/node-bunyan/) instance.

```js
var KafkaAvro = require('kafka-avro');
var fmt = require('bunyan-format');

var kafkaLog  = KafkaAvro.getLogger();

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

[Use the kafka-avro-stub library](https://github.com/waldophotos/kafka-avro-stub) to avoid requiring Kafka and Schema Registry to run on your local for testing your service.

## Releasing

1. Update the changelog bellow.
1. Ensure you are on master.
1. Type: `grunt release`
    * `grunt release:minor` for minor number jump.
    * `grunt release:major` for major number jump.

## Release History

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
