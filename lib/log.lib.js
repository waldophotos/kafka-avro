/**
 * @fileOverview Provides a Bunyan logger.
 */
const bunyan = require('bunyan');
const fmt = require('bunyan-format');

const shouldLog = !!process.env.KAFKA_AVRO_LOG_LEVEL;
const logLevel = process.env.KAFKA_AVRO_LOG_LEVEL || 'info';
const noColors = !!process.env.KAFKA_AVRO_LOG_NO_COLORS;

// FIXME: The users of this lib should be allowed to inject their own loggers

// default outstream mutes
let outStream = {
  write: function () {
  }
};

if (shouldLog) {
  outStream = fmt({
    outputMode: 'long',
    levelInString: true,
    color: !noColors,
  });
}

/**
 * Create a singleton bunyan logger and expose it.
 */
const logger = module.exports = bunyan.createLogger({
  name: 'KafkaAvro',
  level: logLevel,
  stream: outStream,
});

/**
 * Get a child logger with a relative path to the provided full module path.
 *
 * Usage: const log = require('./util/logger').getChild(__filename);
 *
 * @param {string} modulePath The full module path.
 * @return {bunyan.Child} A child logger to use.
 */
logger.getChild = function (modulePath) {
  const cleanModulePath = modulePath.split('kafka-avro/lib').pop();

  return logger.child({module: cleanModulePath});
};
