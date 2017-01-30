/**
 * @fileOverview Test produce and consume messages using kafka-avro and magic byte.
 */
var crypto = require('crypto');

var chai = require('chai');
var expect = chai.expect;

var KafkaAvro = require('../..');
var testLib = require('../lib/test.lib');

function noop() {}
