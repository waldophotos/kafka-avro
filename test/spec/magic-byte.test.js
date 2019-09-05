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
    const message = {
      name: new Array(40000).join('0'),
      long: 540,
    };

    const type = avro.parse(schemaFix, {wrapUnions: true});

    magicByte.toMessageBuffer(message, type, 109);
  });

  it('should extract schemaId from encoded message', function() {
    const message = {
      name: new Array(40).join('0'),
      long: 540,
    };

    const schemaId = 109;

    const type = avro.parse(schemaFix, {wrapUnions: true});

    const encoded = magicByte.toMessageBuffer(message, type, schemaId);

    const decoded = magicByte.fromMessageBuffer(encoded, {
      schemaTypeById: {
        'schema-109': {
          // eslint-disable-next-line no-unused-vars
          'decode': function (buf, pos, resolver) {
            return {
              'value': message
            };
          }
        }
      }
    });

    expect(decoded.value.name).to.equal(message.name);
    expect(decoded.value.long).to.equal(message.long);
    expect(decoded.schemaId).to.equal(schemaId);
  });
});
