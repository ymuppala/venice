package com.linkedin.venice.spark.chunk;

import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SCHEMA_WITH_SCHEMA_ID;
import static com.linkedin.venice.spark.SparkConstants.KEY_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.MESSAGE_TYPE_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.OFFSET_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.RMD_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.RMD_VERSION_ID_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.SCHEMA_ID_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.VALUE_COLUMN_NAME;

import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.hadoop.input.kafka.chunk.ChunkAssembler;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.ByteUtils;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;


/**
 * Spark adapter for ChunkAssembler that handles chunked values and RMDs.
 * Converts Spark Rows to the format expected by ChunkAssembler, assembles chunks,
 * and returns the result as a Spark Row.
 */
public class SparkChunkAssembler implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final RecordSerializer<KafkaInputMapperValue> SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(KafkaInputMapperValue.SCHEMA$);

  private final boolean isRmdChunkingEnabled;
  private transient ChunkAssembler chunkAssembler;

  public SparkChunkAssembler(boolean isRmdChunkingEnabled) {
    this.isRmdChunkingEnabled = isRmdChunkingEnabled;
  }

  /**
   * Get or lazily initialize the ChunkAssembler.
   * Transient field needs to be recreated after deserialization.
   */
  private ChunkAssembler getChunkAssembler() {
    if (chunkAssembler == null) {
      chunkAssembler = new ChunkAssembler(isRmdChunkingEnabled);
    }
    return chunkAssembler;
  }

  /**
   * Assemble chunks for a single key.
   *
   * @param keyBytes The key bytes
   * @param rows Iterator of rows for this key (MUST be sorted by offset DESC - highest offset first)
   * @return Assembled row with DEFAULT_SCHEMA_WITH_SCHEMA_ID schema, or null if DELETE or incomplete chunks
   */
  public Row assembleChunks(byte[] keyBytes, Iterator<Row> rows) {
    // Handle empty iterator early
    if (!rows.hasNext()) {
      return null;
    }

    // Convert Spark Row iterator to byte[] iterator (serialized KafkaInputMapperValue)
    Iterator<byte[]> valueIterator = new RowToSerializedValueIterator(rows);

    // Call the core MR ChunkAssembler logic
    ChunkAssembler.ValueBytesAndSchemaId assembled;
    try {
      assembled = getChunkAssembler().assembleAndGetValue(keyBytes, valueIterator);
    } catch (Exception e) {
      // If assembly fails (e.g., incomplete chunks, missing manifest, orphan chunks), return null
      return null;
    }

    if (assembled == null) {
      // Latest record is DELETE, or chunks are incomplete
      return null;
    }

    byte[] rmdBytes = null;
    ByteBuffer rmdPayload = assembled.getReplicationMetadataPayload();
    if (rmdPayload != null && rmdPayload.hasRemaining()) {
      rmdBytes = ByteUtils.extractByteArray(rmdPayload);
    }

    // Convert result back to Spark Row
    return new GenericRowWithSchema(
        new Object[] { keyBytes, assembled.getBytes(), rmdBytes, assembled.getSchemaID(),
            assembled.getReplicationMetadataVersionId() },
        DEFAULT_SCHEMA_WITH_SCHEMA_ID);
  }

  /**
   * Inner class to adapt Spark Row iterator to byte[] iterator.
   * ChunkAssembler.assembleAndGetValue() expects an Iterator of byte[] where each byte[]
   * is a serialized KafkaInputMapperValue.
   */
  private static class RowToSerializedValueIterator implements Iterator<byte[]> {
    private final Iterator<Row> rowIterator;

    RowToSerializedValueIterator(Iterator<Row> rowIterator) {
      this.rowIterator = rowIterator;
    }

    @Override
    public boolean hasNext() {
      return rowIterator.hasNext();
    }

    @Override
    public byte[] next() {
      Row row = rowIterator.next();

      // Create KafkaInputMapperValue from Spark Row
      KafkaInputMapperValue mapperValue = new KafkaInputMapperValue();
      mapperValue.schemaId = row.getAs(SCHEMA_ID_COLUMN_NAME);
      mapperValue.offset = row.getAs(OFFSET_COLUMN_NAME);

      // Value type (PUT vs DELETE)
      int messageType = row.getAs(MESSAGE_TYPE_COLUMN_NAME);
      mapperValue.valueType = messageType == 0 ? MapperValueType.PUT : MapperValueType.DELETE;

      // Value field
      byte[] valueBytes = row.getAs(VALUE_COLUMN_NAME);
      mapperValue.value = valueBytes != null ? ByteBuffer.wrap(valueBytes) : ByteBuffer.allocate(0);

      // RMD fields
      mapperValue.replicationMetadataVersionId = row.getAs(RMD_VERSION_ID_COLUMN_NAME);
      byte[] rmdBytes = row.getAs(RMD_COLUMN_NAME);
      mapperValue.replicationMetadataPayload = rmdBytes != null ? ByteBuffer.wrap(rmdBytes) : ByteBuffer.allocate(0);

      // For chunk records, the chunkedKeySuffix is used by ChunkAssembler to match chunks with manifest
      // We pass the full key here - ChunkAssembler will extract the suffix by comparing with base key
      byte[] keyBytes = row.getAs(KEY_COLUMN_NAME);
      mapperValue.chunkedKeySuffix = ByteBuffer.wrap(keyBytes);

      // Serialize to byte[] (format expected by ChunkAssembler)
      return SERIALIZER.serialize(mapperValue);
    }
  }
}
