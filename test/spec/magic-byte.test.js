/**
 * @fileOverview Test Magic Byte implementation.
 */
var chai = require('chai');
var expect = chai.expect;
var avro = require('avsc');

// var testLib = require('../lib/test.lib');
var magicByte = require('../../lib/magic-byte');

var schemaFix = require('../fixtures/schema.fix');

describe('Magic Byte', function() {

  it('should encode a large message', function() {
    var message = {
      name: new Array(40000).join('0'),
      long: 540,
    };

    var type = avro.parse(schemaFix, {wrapUnions: true});

    magicByte.toMessageBuffer(message, type, 109);
  });
});
