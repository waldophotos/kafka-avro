/**
 * @fileOverview Provides a Bunyan logger.
 */
const bunyan = require('bunyan');

/**
 * Create a singleton bunyan logger and expose it.
 */
const logger = module.exports = bunyan.createLogger({
  name: 'KafkaAvro',
  level: 'info',
  // Mute logger
  stream: {
    write: function() {}
  },
});

/**
 * Get a child logger with a relative path to the provided full module path.
 *
 * Usage: const log = require('./util/logger').getChild(__filename);
 *
 * @param {string} modulePath The full modulepath.
 * @return {bunyan.Child} A child logger to use.
 */
logger.getChild = function(modulePath) {
  let cleanModulePath = modulePath.split('kafka-avro/lib').pop();

  return logger.child({module: cleanModulePath});
};
