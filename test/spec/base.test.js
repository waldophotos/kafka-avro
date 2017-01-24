/**
 * @fileOverview Base API Surface tests.
 */
const chai = require('chai');
const expect = chai.expect;

const awesomeLib = require('../..');

describe('Base API Surface', function() {
  it('should expose expected methods', function(){
    expect(awesomeLib).to.have.keys([
      'awesome',
      'method',
    ]);
  });
});
