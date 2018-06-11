/**
 * @fileOverview Test Magic Byte implementation.
 */
var chai = require('chai');
var expect = chai.expect;
var avro = require('avsc');

// var testLib = require('../lib/test.lib');
var magicByte = require('../../lib/magic-byte');

var schemaFix = require('../fixtures/schema.fix');
var schemaWithDefault = require('../fixtures/schema-default-value.fix');

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

    var decoded = magicByte.fromMessageBuffer(type, encoded, {
      schemaTypeById: {
        schemaId: schemaId
      }
    });

    expect(decoded.value.name).to.equal(message.name);
    expect(decoded.value.long).to.equal(message.long);
    expect(decoded.schemaId).to.equal(schemaId);
  });

  it('should use default value from reader schema when payload does not provide a value', function() {
    var message = {
      name: new Array(40).join('0'),
      long: 540,
    };

    var schemaId = 109;

    var writerType = avro.parse(schemaFix, {wrapUnions: true});
    var readerType = avro.parse(schemaWithDefault, {wrapUnions: true});

    var encoded = magicByte.toMessageBuffer(message, writerType, schemaId);

    var decoded = magicByte.fromMessageBuffer(writerType, encoded,
      {
      schemaTypeById: {
        schemaId: schemaId
        }
      },
      readerType);

    expect(decoded.value.name).to.equal(message.name);
    expect(decoded.value.long).to.equal(message.long);
    expect(decoded.value.anotherString).to.equal('defaultValue');
    expect(decoded.schemaId).to.equal(schemaId);
  });

  it('should override default value from reader schema when payload provides a value', function() {
    var message = {
      name: new Array(40).join('0'),
      long: 540,
      anotherString: 'not-your-default'
    };

    var schemaId = 109;

    schemaFix.fields.push({name: 'anotherString', type: 'string' });

    var writerType = avro.parse(JSON.stringify(schemaFix), {wrapUnions: true});
    var readerType = avro.parse(schemaWithDefault, {wrapUnions: true});

    var encoded = magicByte.toMessageBuffer(message, writerType, schemaId);

    var decoded = magicByte.fromMessageBuffer(writerType, encoded,
      {
      schemaTypeById: {
        schemaId: schemaId
        }
      }, readerType);

    expect(decoded.value.name).to.equal(message.name);
    expect(decoded.value.long).to.equal(message.long);
    expect(decoded.value.anotherString).to.equal('not-your-default');
    expect(decoded.schemaId).to.equal(schemaId);
  })
});
