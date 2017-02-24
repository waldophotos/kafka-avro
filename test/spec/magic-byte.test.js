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

  it('should extract schemaId from encoded message', function() {
    var message = {
      name: new Array(40).join('0'),
      long: 540,
    };

    var schemaId = 109;

    var type = avro.parse(schemaFix, {wrapUnions: true});

    var encoded = magicByte.toMessageBuffer(message, type, schemaId);

    var decoded = magicByte.fromMessageBuffer(type, encoded, this.sr);

    expect(decoded.value.name).to.equal(message.name);
    expect(decoded.value.long).to.equal(message.long);
    expect(decoded.schemaId).to.equal(schemaId);
  });
});
