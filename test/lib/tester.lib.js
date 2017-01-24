/*
 * @fileOverview Main testing helper lib.
 */

var tester = module.exports = {};

/** @type {Object} simple logger */
tester.log = {
  info: function() {
    let args = Array.prototype.splice.call(arguments, 0);
    console.log('INFO:', args.join(' '));
  },
  error: function() {
    let args = Array.prototype.splice.call(arguments, 0);
    console.log('ERROR:', args.join(' '));
  },
};

/**
 * Have a Cooldown period between tests.
 *
 * @param {number} seconds cooldown in seconds.
 * @return {Function} use is beforeEach().
 */
tester.cooldown = function(seconds) {
  return function(done) {
    setTimeout(done, seconds);
  };
};
