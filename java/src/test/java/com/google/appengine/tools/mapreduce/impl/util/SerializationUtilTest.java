// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.util;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.testutil.DatastoreExtension;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil.CompressionType;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;
import java.util.Random;

/**
 * @author ohler@google.com (Christian Ohler)
 */
@ExtendWith({DatastoreExtension.class, DatastoreExtension.ParameterResolver.class})
public class SerializationUtilTest {

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper();
  private Datastore datastore;

  @BeforeEach
  protected void setUp(Datastore datastore) throws Exception {
    helper.setUp();
    this.datastore = datastore;
  }

  @AfterEach
  protected void tearDown() throws Exception {
    helper.tearDown();
  }

  @Test
  public void testGetBytes_slice1() throws Exception {
    ByteBuffer b = ByteBuffer.allocate(10);
    b.putShort((short) 0x1234);
    b.limit(2);
    b.position(0);
    ByteBuffer slice = b.slice();
    byte[] bytes = SerializationUtil.getBytes(slice);
    assertEquals(2, bytes.length);
    assertTrue(Arrays.equals(new byte[] { 0x12, 0x34 }, bytes));
  }

  @Test
  public void testGetBytes_slice2() throws Exception {
    ByteBuffer b = ByteBuffer.allocate(10);
    b.position(2);
    b.putShort((short) 0x1234);
    b.position(2);
    b.limit(4);
    ByteBuffer slice = b.slice();
    byte[] bytes = SerializationUtil.getBytes(slice);
    assertEquals(2, bytes.length);
    assertTrue(Arrays.equals(new byte[] { 0x12, 0x34 }, bytes));
  }

  @Test
  public void testSerializeToFromByteArrayWithNoParams() throws Exception {
    Serializable original = "hello";
    byte[] bytes = SerializationUtil.serializeToByteArray(original);
    assertEquals(12, bytes.length);
    bytes = SerializationUtil.serializeToByteArray(original, true);
    assertEquals(12, bytes.length);
    bytes = SerializationUtil.serializeToByteArray(original, true, CompressionType.NONE);
    assertEquals(49, bytes.length);

    bytes = SerializationUtil.serializeToByteArray(original, true, CompressionType.GZIP);
    assertEquals(57, bytes.length);
    bytes = SerializationUtil.serializeToByteArray(original);
    Object restored = SerializationUtil.deserializeFromByteArray(bytes);
    assertEquals(original, restored);
  }

  @Test
  public void testSerializeToFromByteArray() throws Exception {
    Iterable<CompressionType> compressionTypes =
        asList(CompressionType.NONE, CompressionType.GZIP, null);
    for (Serializable original : asList(10L, "hello", new Value(1000), CompressionType.GZIP)) {
      for (boolean ignoreHeader : asList(true, false)) {
        for (CompressionType compression : compressionTypes) {
          byte[] bytes =
              SerializationUtil.serializeToByteArray(original, ignoreHeader, compression);
          Object restored = SerializationUtil.deserializeFromByteArray(bytes, ignoreHeader);
          assertEquals(original, restored);
          ByteBuffer buffer  = ByteBuffer.wrap(bytes);
          restored = SerializationUtil.deserializeFromByteBuffer(buffer, ignoreHeader);
          assertEquals(original, restored);
          bytes = SerializationUtil.serializeToByteArray(original, ignoreHeader);
          restored = SerializationUtil.deserializeFromByteArray(bytes, ignoreHeader);
          assertEquals(original, restored);
        }
      }
    }
  }

  private static class Value implements Serializable {

    private static final long serialVersionUID = -2908491492725087639L;
    private byte[] bytes;

    Value(int kb) {
      bytes = new byte[kb * 1024];
      new Random().nextBytes(bytes);
    }

    @Override
    public int hashCode() {
      return ByteBuffer.wrap(bytes).getInt();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Value) {
        Value other = (Value) obj;
        return Arrays.equals(bytes, other.bytes);
      }
      return false;
    }
  }

  @ValueSource(ints = {
    0, 500, 2000, 10000, 1500
  })
  @ParameterizedTest
  public void testSerializeToDatastore(int size) throws Exception {
    Iterable<CompressionType> compressionTypes =
        asList(CompressionType.NONE, CompressionType.GZIP, null);

    Value value = new Value(size);

    for (CompressionType compression : compressionTypes) {
      int id = 1 + size + Optional.ofNullable(compression).map(c -> c.ordinal() + 1).orElse(0); // hacky, but avoids contention in emulator
      Key key = Key.newBuilder(datastore.getOptions().getProjectId(), "serializeToDatastoreTest", id).build();
      Transaction tx = datastore.newTransaction();

      Entity.Builder entity = Entity.newBuilder(key);
      SerializationUtil.serializeToDatastoreProperty(tx, entity, "foo", value, compression);
      datastore.put(entity.build());
      tx.commit(); // why when try to do PUT here, do we get 'too much contention'? it's just a put of 4 values in a txn

      Entity fromDb = datastore.get(key);
      Serializable restored = SerializationUtil.deserializeFromDatastoreProperty(datastore, fromDb, "foo");
      assertEquals(value, restored);
    }
  }
}
