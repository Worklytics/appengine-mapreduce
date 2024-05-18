// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.util;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.DatastoreExtension;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author ohler@google.com (Christian Ohler)
 */
@ExtendWith({
  DatastoreExtension.class,
  //AppEngineEnvironmentExtension.class,
  DatastoreExtension.ParameterResolver.class,
})
public class SerializationUtilTest {

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper();

  Datastore datastore;

  @BeforeEach
  public void injectDatastore(Datastore datastore) {
    this.datastore = datastore;
  }

  @BeforeEach
  protected void setUp() throws Exception {
    helper.setUp();
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

    bytes = SerializationUtil.serializeToByteArray(original);
    Object restored = SerializationUtil.deserialize(bytes);
    assertEquals(original, restored);
  }

  @Test
  public void testSerializeToFromByteArray() throws Exception {
    for (Serializable original : asList(10L, "hello", new Value(1000))) {
        byte[] bytes =
            SerializationUtil.serializeToByteArray(original);
        Object restored = SerializationUtil.deserialize(bytes);
        assertEquals(original, restored);
        bytes = SerializationUtil.serializeToByteArray(original);
        restored = SerializationUtil.deserialize(bytes);
        assertEquals(original, restored);

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

  @Test
  public void testSerializeToDatastore() throws Exception {
    Key key = this.datastore.newKeyFactory().setKind("mr-entity").newKey(1);
    List<Value> values = asList(null, new Value(0), new Value(500), new Value(2000),
        new Value(10000), new Value(1500));
    for (Value original : values) {
      Transaction tx = datastore.newTransaction();
      Entity.Builder entity = Entity.newBuilder(key);
      SerializationUtil.serializeToDatastoreProperty(tx, entity, "foo", original);
      datastore.put(entity.build());
      tx.commit();
      Entity fromDb = datastore.get(key);
      Serializable restored = SerializationUtil.deserializeFromDatastoreProperty(tx, fromDb, "foo");
      assertEquals(original, restored);
    }
  }
}
