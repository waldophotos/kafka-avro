/**
 * @fileOverview Encode and decode an Avro message for confluent SR
 *   with magic byte.
 */

var magicByte = module.exports = {};

/** @const {string} The magic byte value */
var MAGIC_BYTE = 0;

/**
 * Encode and decode an Avro value into a message, as expected by
 * Confluent's Kafka Avro deserializer.
 *
 * @param val {*} The Avro value to encode.
 * @param type {avsc.Type} Your value's Avro type.
 * @param schemaId {Integer} Your schema's ID (inside the registry).
 * @param optLength {Integer=} Optional initial buffer length. Set it high enough
 * to avoid having to resize. Defaults to 1024.
 * @return {Byte} Serialized value.
 */
magicByte.toMessageBuffer = function (val, type, schemaId, optLength) {
  var length = optLength || 1024;
  var buf = new Buffer(length);

  buf[0] = MAGIC_BYTE; // Magic byte.
  buf.writeInt32BE(schemaId, 1);

  var pos = type.encode(val, buf, 5);

  if (pos < 0) {
    // The buffer was too short, we need to resize.
    return magicByte.toMessageBuffer(val, type, schemaId, length - pos);
  }
  return buf.slice(0, pos);
};

/**
 * Decode a confluent SR message with magic byte.
 *
 * @param {avsc.Type} writerType The topic's Avro writer schema decoder.
 * @param {Buffer} encodedMessage The incoming message.
 * @param {kafka-avro.SchemaRegistry} sr The local SR instance.
 * @param {avsc.Type} readerType The topic's Avro reader schema decoded.
 * @return {Object} Object with:
 *   @param {number} schemaId The schema id.
 *   @param {Object} value The decoded avro value.
 */
magicByte.fromMessageBuffer = function (writerType, encodedMessage, sr, readerType) {
  if (encodedMessage[0] !== MAGIC_BYTE) {
    throw new TypeError('Message not serialized with magic byte');
  }

  var schemaId = encodedMessage.readInt32BE(1);

  var schemaKey = 'schema-' + schemaId;

  var wt = sr.schemaTypeById[schemaKey] || writerType;
  var decoded;
  if (readerType) {
    var resolver = readerType.createResolver(wt);
    decoded = readerType.decode(encodedMessage, 5, resolver);
  }
  else {
    decoded = wt.decode(encodedMessage, 5);
  }

  return {
    value: decoded.value,
    schemaId: schemaId,
  };
};