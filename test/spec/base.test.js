/**
 * @fileOverview Base API Surface tests.
 */
const chai = require('chai');
const expect = chai.expect;

const KafkaAvro = require('../..');

describe('Base API Surface', function() {
  it('should expose expected methods', function(){
    expect(KafkaAvro).to.be.a('function');
  });
});
