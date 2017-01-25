# kafka-avro

> Node.js bindings for librdkafka with Avro schema serialization.

**WARNING** Still WIP, consumer not working!

## Install

Install the module using NPM:

```
npm install kafka-avro --save
```

## Documentation

### Initialize kafka-avro

Initialize kafka-avro
```js
var KafkaAvro = require('kafka-avro');

var kafkaAvro = new KafkaAvro({
    kafkaBroker: 'localhost:9092',
    schemaRegistry: 'localhost:8081',
});

kafkaAvro.on('log', function(message) {
    console.log(message);
})

// Query the Schema Registry for all topic-schema's
// fetch them and evaluate them.
kafkaAvro.init()
    .then(function() {
        console.log('Ready to use');
    });
```

### Quick Usage Producer

> NOTICE: You need to initialize kafka-avro before you can produce or consume messages.

```js
var producer = kafkaAvro.getProducer({
  // Options listed bellow
});

var topicName = 'test';

//Wait for the ready event before producing
producer.on('ready', function() {
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
  producer.produce(topicName, topic, partition, value, key);
});

producer.on('disconnected', function(arg) {
  console.log('producer disconnected. ' + JSON.stringify(arg));
});

//starting the producer
producer.connect();
```

What kafka-avro basically does is wrap around node-rdkafka and intercept the produce method to validate and serialize the message.

* [node-rdkafka Producer Tutorial](https://blizzard.github.io/node-rdkafka/current/tutorial-producer_.html)
* [Full list of Producer's options](https://github.com/edenhill/librdkafka/blob/2213fb29f98a7a73f22da21ef85e0783f6fd67c4/CONFIGURATION.md)

### Quick Usage Consumer

```js
var Transform = require('stream').Transform;

var consumer = kafkaAvro.getConsumer({
  'group.id': 'librd-test',
  'socket.keepalive.enable': true,
  'enable.auto.commit': true,
});

var topicName = 'test';

var stream = consumer.getReadStream(topicName, {
  waitInterval: 0
});

stream.on('error', function() {
  process.exit(1);
});

stream
  .pipe(new Transform({
    objectMode: true,
    transform: function(data, encoding, callback) {
      // do your async stuff, then:
      callback(null, data.message);
    }
  }))
  .pipe(process.stdout);

consumer.on('error', function(err) {
  console.log(err);
});
```

Same deal here, thin wrapper around node-rdkafka and deserialize incoming messages before they reach your consuming method.

* [node-rdkafka Consumer Tutorial](https://blizzard.github.io/node-rdkafka/current/tutorial-consumer.html)
* [Full list of Consumer's options](https://github.com/edenhill/librdkafka/blob/2213fb29f98a7a73f22da21ef85e0783f6fd67c4/CONFIGURATION.md)

## Releasing

1. Update the changelog bellow.
1. Ensure you are on master.
1. Type: `grunt release`
    * `grunt release:minor` for minor number jump.
    * `grunt release:major` for major number jump.

## Release History

- **v0.0.1**, *TBD*
    - Big Bang

## License

Copyright Waldo, Inc. All rights reserved.
