package com.linkedin.venice.spark.chunk;

import static com.linkedin.venice.spark.SparkConstants.KEY_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.RMD_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.RMD_VERSION_ID_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.SCHEMA_FOR_CHUNK_ASSEMBLY;
import static com.linkedin.venice.spark.SparkConstants.SCHEMA_ID_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.VALUE_COLUMN_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.testng.annotations.Test;


/**
 * Unit tests for SparkChunkAssembler.
 *
 * Note: These tests focus on the adapter layer (Spark Row <-> KafkaInputMapperValue conversion).
 * The core chunk assembly logic is thoroughly tested in TestInMemoryChunkAssembler.
 *
 * We test:
 * 1. Regular (non-chunked) records pass through correctly
 * 2. DELETE records are handled properly
 * 3. Adapter correctly converts Spark Rows to the format expected by ChunkAssembler
 * 4. Edge cases: empty rows, null values, etc.
 */
public class SparkChunkAssemblerTest {
  private static final int REGULAR_SCHEMA_ID = 1;
  private static final int CHUNK_SCHEMA_ID = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();

  /**
   * Test assembling a regular non-chunked PUT record.
   * This should pass through without any assembly.
   */
  @Test
  public void testRegularPutRecord() {
    SparkChunkAssembler assembler = new SparkChunkAssembler(false);

    byte[] key = "test-key".getBytes();
    byte[] value = "test-value".getBytes();
    byte[] rmd = "test-rmd".getBytes();
    int schemaId = REGULAR_SCHEMA_ID;
    int rmdVersionId = 1;
    long offset = 100L;

    Row row = createRow(key, value, rmd, schemaId, rmdVersionId, offset);
    Iterator<Row> rows = Arrays.asList(row).iterator();

    Row assembled = assembler.assembleChunks(key, rows);

    assertNotNull(assembled, "Regular PUT should return assembled row");
    assertEquals(assembled.getAs(KEY_COLUMN_NAME), key, "Key should match");
    assertEquals(assembled.getAs(VALUE_COLUMN_NAME), value, "Value should match");
    assertEquals(assembled.getAs(RMD_COLUMN_NAME), rmd, "RMD should match");
    assertEquals((int) assembled.getAs(SCHEMA_ID_COLUMN_NAME), schemaId, "Schema ID should match");
    assertEquals((int) assembled.getAs(RMD_VERSION_ID_COLUMN_NAME), rmdVersionId, "RMD version ID should match");
  }

  /**
   * Test handling DELETE record with RMD.
   * DELETE records with RMD should return a row with empty value but preserved RMD.
   * Note: ChunkAssembler returns the value bytes it received (empty array), not null.
   */
  @Test
  public void testDeleteRecordWithRmd() {
    SparkChunkAssembler assembler = new SparkChunkAssembler(false);

    byte[] key = "delete-key".getBytes();
    byte[] emptyValue = new byte[0];
    byte[] rmd = "delete-rmd".getBytes();
    int deleteSchemaId = REGULAR_SCHEMA_ID; // Positive schema ID for DELETE
    int rmdVersionId = 1;
    long offset = 100L;

    Row row = createRow(key, emptyValue, rmd, deleteSchemaId, rmdVersionId, offset);
    Iterator<Row> rows = Arrays.asList(row).iterator();

    Row assembled = assembler.assembleChunks(key, rows);

    // DELETE with RMD should return a row
    assertNotNull(assembled, "DELETE with RMD should return a row");
    assertEquals(assembled.getAs(KEY_COLUMN_NAME), key, "Key should match");
    // ChunkAssembler returns the bytes it received (empty array for DELETE)
    byte[] assembledValue = assembled.getAs(VALUE_COLUMN_NAME);
    assertNotNull(assembledValue, "Value should not be null");
    assertEquals(assembledValue.length, 0, "Value should be empty for DELETE");
    assertEquals(assembled.getAs(RMD_COLUMN_NAME), rmd, "RMD should be preserved for DELETE");
  }

  /**
   * Test handling DELETE record without RMD.
   * DELETE records without RMD should still return a row with empty value
   * (ChunkAssembler doesn't distinguish - it looks at RMD payload buffer remaining()).
   */
  @Test
  public void testDeleteRecordWithoutRmd() {
    SparkChunkAssembler assembler = new SparkChunkAssembler(false);

    byte[] key = "delete-key-no-rmd".getBytes();
    byte[] emptyValue = new byte[0];
    byte[] emptyRmd = new byte[0];
    int deleteSchemaId = REGULAR_SCHEMA_ID;
    int rmdVersionId = 1;
    long offset = 100L;

    Row row = createRow(key, emptyValue, emptyRmd, deleteSchemaId, rmdVersionId, offset);
    Iterator<Row> rows = Arrays.asList(row).iterator();

    Row assembled = assembler.assembleChunks(key, rows);

    // Even DELETE without meaningful RMD returns a row (empty RMD still has buffer)
    // This is expected behavior from ChunkAssembler
    assertNotNull(assembled, "DELETE should return a row");
    byte[] assembledValue = assembled.getAs(VALUE_COLUMN_NAME);
    assertEquals(assembledValue.length, 0, "Value should be empty for DELETE");
  }

  /**
   * Test handling chunk records without manifest.
   * Chunks without a manifest should be ignored and return null.
   */
  @Test
  public void testChunkWithoutManifest() {
    SparkChunkAssembler assembler = new SparkChunkAssembler(false);

    byte[] key = "chunk-key".getBytes();
    byte[] chunkValue = "chunk-data".getBytes();
    byte[] emptyRmd = new byte[0];

    // Create a chunk record (CHUNK schema ID)
    Row chunkRow = createRow(key, chunkValue, emptyRmd, CHUNK_SCHEMA_ID, -1, 100L);
    Iterator<Row> rows = Arrays.asList(chunkRow).iterator();

    Row assembled = assembler.assembleChunks(key, rows);

    // Orphan chunks without manifest should return null (assembly fails)
    assertNull(assembled, "Chunk without manifest should return null");
  }

  /**
   * Test handling empty row iterator.
   * Empty iterator should return null.
   */
  @Test
  public void testEmptyRowIterator() {
    SparkChunkAssembler assembler = new SparkChunkAssembler(false);

    byte[] key = "empty-key".getBytes();
    Iterator<Row> emptyRows = new ArrayList<Row>().iterator();

    Row assembled = assembler.assembleChunks(key, emptyRows);

    assertNull(assembled, "Empty iterator should return null");
  }

  /**
   * Test multiple records with highest offset being regular PUT.
   * The record with highest offset should be used.
   */
  @Test
  public void testMultipleRecordsHighestOffsetWins() {
    SparkChunkAssembler assembler = new SparkChunkAssembler(false);

    byte[] key = "multi-record-key".getBytes();
    byte[] newValue = "new-value".getBytes();
    byte[] oldValue = "old-value".getBytes();
    byte[] rmd = "rmd".getBytes();
    int schemaId = REGULAR_SCHEMA_ID;
    int rmdVersionId = 1;

    // Create rows in descending order by offset (highest first)
    List<Row> rows = new ArrayList<>();
    rows.add(createRow(key, newValue, rmd, schemaId, rmdVersionId, 102L)); // Highest offset - should be used
    rows.add(createRow(key, oldValue, rmd, schemaId, rmdVersionId, 101L)); // Lower offset - ignored

    Row assembled = assembler.assembleChunks(key, rows.iterator());

    assertNotNull(assembled, "Should return assembled row");
    // Should use the value with highest offset
    assertEquals(assembled.getAs(VALUE_COLUMN_NAME), newValue, "Should use value with highest offset");
  }

  /**
   * Test serialization of SparkChunkAssembler.
   * The assembler should be serializable for Spark.
   */
  @Test
  public void testSerializability() throws Exception {
    SparkChunkAssembler assembler = new SparkChunkAssembler(true);

    // Test that it can be serialized and deserialized
    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos);
    oos.writeObject(assembler);
    oos.close();

    java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(baos.toByteArray());
    java.io.ObjectInputStream ois = new java.io.ObjectInputStream(bais);
    SparkChunkAssembler deserializedAssembler = (SparkChunkAssembler) ois.readObject();
    ois.close();

    assertNotNull(deserializedAssembler, "Deserialized assembler should not be null");

    // Test that the deserialized assembler still works
    byte[] key = "test-key".getBytes();
    byte[] value = "test-value".getBytes();
    byte[] rmd = "test-rmd".getBytes();
    Row row = createRow(key, value, rmd, REGULAR_SCHEMA_ID, 1, 100L);

    Row assembled = deserializedAssembler.assembleChunks(key, Arrays.asList(row).iterator());
    assertNotNull(assembled, "Deserialized assembler should work correctly");
    assertEquals(assembled.getAs(VALUE_COLUMN_NAME), value, "Value should match after deserialization");
  }

  /**
   * Test RMD chunking enabled flag.
   * The assembler should accept both enabled and disabled RMD chunking.
   */
  @Test
  public void testRmdChunkingFlag() {
    // Test with RMD chunking disabled
    SparkChunkAssembler assemblerDisabled = new SparkChunkAssembler(false);
    assertNotNull(assemblerDisabled, "Assembler with RMD chunking disabled should be created");

    // Test with RMD chunking enabled
    SparkChunkAssembler assemblerEnabled = new SparkChunkAssembler(true);
    assertNotNull(assemblerEnabled, "Assembler with RMD chunking enabled should be created");

    // Both should work for regular records
    byte[] key = "test-key".getBytes();
    byte[] value = "test-value".getBytes();
    byte[] rmd = "test-rmd".getBytes();
    Row row = createRow(key, value, rmd, REGULAR_SCHEMA_ID, 1, 100L);

    Row assembledDisabled = assemblerDisabled.assembleChunks(key, Arrays.asList(row).iterator());
    Row assembledEnabled = assemblerEnabled.assembleChunks(key, Arrays.asList(row).iterator());

    assertNotNull(assembledDisabled, "Assembler with RMD chunking disabled should work");
    assertNotNull(assembledEnabled, "Assembler with RMD chunking enabled should work");
  }

  // Helper methods

  private Row createRow(byte[] key, byte[] value, byte[] rmd, int schemaId, int rmdVersionId, long offset) {
    return new GenericRowWithSchema(
        new Object[] { key, value, rmd, schemaId, rmdVersionId, offset, 0 }, // message_type = 0 (PUT)
        SCHEMA_FOR_CHUNK_ASSEMBLY);
  }
}
