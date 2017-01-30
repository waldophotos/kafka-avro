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
 * @param length {Integer=} Optional initial buffer length. Set it high enough
 * to avoid having to resize. Defaults to 1024.
 * @return {Byte} Serialized value.
 */
magicByte.toMessageBuffer = function (val, type, schemaId, length) {
  var buf = new Buffer(length || 1024);
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
 * @param {avsc.Type} The topic's Avro type.
 * @param {Byte} val The incoming message.
 * @return {*} Deserialized value.
 */
magicByte.fromMessageBuffer = function (type, val) {
  return type.decode(val, 5);
};
