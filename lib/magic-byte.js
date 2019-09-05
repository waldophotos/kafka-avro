/**
 * @fileOverview Encode and decode an Avro message for confluent SR
 *   with magic byte.
 */
const magicByte = module.exports = {};

/** @const {string} The magic byte value */
const MAGIC_BYTE = 0;

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
  const length = optLength || 1024;
  const buf = new Buffer(length);

  buf[0] = MAGIC_BYTE;
  buf.writeInt32BE(schemaId, 1);

  const pos = type.encode(val, buf, 5);

  if (pos < 0) {
    // The buffer was too short, we need to resize.
    return magicByte.toMessageBuffer(val, type, schemaId, length - pos);
  }
  return buf.slice(0, pos);
};

/**
 * Decode a confluent SR message with magic byte.
 *
 * @param {avsc.Type} type The topic's Avro decoder.
 * @param {Buffer} encodedMessage The incoming message.
 * @param {kafka-avro.SchemaRegistry} sr The local SR instance.
 * @return {Object} Object with:
 *   @param {number} schemaId The schema id.
 *   @param {Object} value The decoded avro value.
 */
magicByte.fromMessageBuffer = function (encodedMessage, sr) {
  if (!encodedMessage || encodedMessage[0] !== MAGIC_BYTE) {
    return null;
  }

  const schemaId = encodedMessage.readInt32BE(1);
  const schemaType = sr.schemaTypeById[('schema-' + schemaId)];

  if (!schemaType) {
    return null;
  }

  return {
    value: schemaType.decode(encodedMessage, 5).value,
    schemaId: schemaId,
  };
};
