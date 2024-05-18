// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.handlers;


import com.google.appengine.tools.mapreduce.EndToEndTestCase;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobController;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobService;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobServiceFactory;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobState;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status;
import com.google.appengine.tools.mapreduce.impl.shardedjob.TestTask;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.common.collect.ImmutableList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;


public class StatusHandlerTest extends EndToEndTestCase {

  private static final class DummyWorkerController
      extends ShardedJobController<TestTask> {
    private static final long serialVersionUID = 1L;

    @Override
    public void failed(Status status) {}

    @Override
    public void completed(Iterator<TestTask> results) {}
  }

  @Test
  public void testCleanupJob() throws Exception {
    ShardedJobService jobService = ShardedJobServiceFactory.getShardedJobService();
    assertTrue(jobService.cleanupJob(getDatastore(), "testCleanupJob")); // No such job yet
    ShardedJobSettings settings = new ShardedJobSettings.Builder().build();
    ShardedJobController<TestTask> controller = new DummyWorkerController();
    byte[] bytes = new byte[1024 * 1024];
    new Random().nextBytes(bytes);
    TestTask s1 = new TestTask(0, 2, 2, 2, bytes);
    TestTask s2 = new TestTask(1, 2, 2, 1);
    jobService.startJob(getDatastore(), "testCleanupJob", ImmutableList.of(s1, s2), controller, settings);
    assertFalse(jobService.cleanupJob(getDatastore(), "testCleanupJob"));
    executeTasksUntilEmpty();


    Query<Entity> query = Query.newEntityQueryBuilder()
      .setKind("MR-ShardedJob") // Specify your entity kind here
      .build();
    //TODO: what other entities to count??
    assertEquals(3, countResults(getDatastore().run(query)));


    assertTrue(jobService.cleanupJob(getDatastore(), "testCleanupJob"));
    executeTasksUntilEmpty();
    assertEquals(0, countResults(getDatastore().run(query)));
  }

  int countResults(QueryResults<?> results) {
    int i = 0;
    while (results.hasNext()) {
      results.next();
      i++;
    }
    return i;
  }

  // Tests that an job that has just been initialized returns a reasonable job detail.
  @Test
  public void testGetJobDetail_empty() throws Exception {
    ShardedJobService jobService = ShardedJobServiceFactory.getShardedJobService();
    ShardedJobSettings settings = new ShardedJobSettings.Builder().build();
    ShardedJobController<TestTask> controller = new DummyWorkerController();
    jobService.startJob(getDatastore(), "testGetJobDetail_empty", ImmutableList.<TestTask>of(),
      controller, settings);

    JSONObject result = StatusHandler.handleGetJobDetail(getDatastore(), "testGetJobDetail_empty");
    assertEquals("testGetJobDetail_empty", result.getString("mapreduce_id"));
    assertEquals(0, result.getJSONArray("shards").length());
    assertNotNull(result.getJSONObject("mapper_spec"));
    assertEquals("testGetJobDetail_empty", result.getString("name"));
    assertEquals(0, result.getJSONObject("counters").length());
  }

  // Tests that a populated job (with a couple of shards) generates a reasonable job detail.
  @Test
  public void testGetJobDetail_populated() throws Exception {
    ShardedJobService jobService = ShardedJobServiceFactory.getShardedJobService();
    ShardedJobSettings settings = new ShardedJobSettings.Builder().build();
    ShardedJobController<TestTask> controller = new DummyWorkerController();
    TestTask s1 = new TestTask(0, 2, 2, 2);
    TestTask s2 = new TestTask(1, 2, 2, 1);
    jobService.startJob(getDatastore(), "testGetJobDetail_populated", ImmutableList.of(s1, s2),
      controller, settings);
    ShardedJobState state = jobService.getJobState(getDatastore(), "testGetJobDetail_populated");
    assertEquals(2, state.getActiveTaskCount());
    assertEquals(2, state.getTotalTaskCount());
    assertEquals(new Status(Status.StatusCode.RUNNING), state.getStatus());
    JSONObject jobDetail = StatusHandler.handleGetJobDetail(getDatastore(), "testGetJobDetail_populated");
    assertNotNull(jobDetail);
    assertEquals("testGetJobDetail_populated", jobDetail.getString("mapreduce_id"));
    assertEquals("testGetJobDetail_populated", jobDetail.getString("name"));
    assertTrue(jobDetail.getBoolean("active"));
    assertEquals(2, jobDetail.getInt("active_shards"));
    verify(jobDetail, tuple("mapreduce_id", "testGetJobDetail_populated"),
        tuple("chart_width", 300),
        tuple("shards",
            array(tuple("shard_description", pattern("[^\"]*")), tuple("active", true),
                tuple("updated_timestamp_ms", pattern("[0-9]*")), tuple("shard_number", 0)),
            array(tuple("shard_description", pattern("[^\"]*")), tuple("active", true),
                tuple("updated_timestamp_ms", pattern("[0-9]*")), tuple("shard_number", 1))),
        tuple("mapper_spec", tuple("mapper_params", tuple("Shards total", 2),
            tuple("Shards active", 2), tuple("Shards completed", 0))),
        tuple("name", "testGetJobDetail_populated"),
        tuple("active", true),
        tuple("active_shards", 2),
        tuple("updated_timestamp_ms", pattern("[0-9]*")),
        tuple("chart_url", pattern("[^\"]*")),
        tuple("counters", pattern("\\{\\}")),
        tuple("start_timestamp_ms", pattern("[0-9]*")),
        tuple("chart_data", 0L, 0L));


    executeTasksUntilEmpty();

    jobDetail = StatusHandler.handleGetJobDetail(getDatastore(), "testGetJobDetail_populated");
    assertNotNull(jobDetail);
    assertEquals("testGetJobDetail_populated", jobDetail.getString("mapreduce_id"));
    assertEquals("testGetJobDetail_populated", jobDetail.getString("name"));
    assertFalse(jobDetail.getBoolean("active"));
    assertEquals(0, jobDetail.getInt("active_shards"));
    verify(jobDetail, tuple("chart_width", 300), tuple("chart_url", pattern("[^\"]*")),
        tuple("result_status", pattern("DONE")), tuple("chart_data", 0L, 0L),
        tuple("counters", tuple("TestTaskSum", 6L)),
        tuple("mapreduce_id", "testGetJobDetail_populated"),
        tuple("shards",
            array(tuple("shard_description", pattern("[^\"]*")),
                tuple("active", false), tuple("updated_timestamp_ms", pattern("[0-9]*")),
                tuple("result_status", pattern("DONE")), tuple("shard_number", 0)),
            array(tuple("shard_description", pattern("[^\"]*")),
                tuple("active", false), tuple("updated_timestamp_ms", pattern("[0-9]*")),
                tuple("result_status", pattern("DONE")), tuple("shard_number", 1))),
       tuple("mapper_spec", tuple("mapper_params", tuple("Shards total", 2),
           tuple("Shards active", 0), tuple("Shards completed", 2))),
        tuple("name", "testGetJobDetail_populated"),
        tuple("active", false),
        tuple("updated_timestamp_ms", pattern("[0-9]*")),
        tuple("active_shards", 0),
        tuple("start_timestamp_ms", pattern("[0-9]*")));
  }

  private static class Tuple<V> {

    private final String key;
    private final V value;

    Tuple(String key, V value) {
      this.key = key;
      this.value = value;
    }
  }

  @SafeVarargs
  private static <T> T[] array(T... values) {
    return values;
  }

  private static <V> Tuple<V> tuple(String key, V value) {
    return new Tuple<>(key, value);
  }

  @SafeVarargs
  private static <V> Tuple<V[]> tuple(String key, V... value) {
    return new Tuple<>(key, value);
  }

  private static Pattern pattern(String pattern) {
    return Pattern.compile(pattern);
  }

  @SuppressWarnings("unchecked")
  private void verify(JSONObject jsonObj, Tuple<?>... expected) throws JSONException {
    Map<String, Object> expectedMap = new HashMap<>();
    for (Tuple<?> tuple : expected) {
      expectedMap.put(tuple.key, tuple.value);
    }
    Iterator<String> keys = jsonObj.keys();
    while (keys.hasNext()) {
      String key = keys.next();
      Object value = jsonObj.get(key);
      Object expectedValue = expectedMap.remove(key);
      assertNotNull(expectedValue, "Missing " + key );
      if (expectedValue instanceof Pattern) {
        Pattern pattern = (Pattern) expectedValue;
        assertTrue(pattern.matcher(value.toString()).matches(), value + " does not match " + pattern);
      } else if (value instanceof JSONObject) {
        if (!expectedValue.getClass().isArray()) {
          expectedValue = new Tuple<?>[] {(Tuple<?>) expectedValue};
        }
        verify((JSONObject) value, (Tuple<?>[]) expectedValue);
      } else if (value instanceof JSONArray || expectedValue.getClass().isArray()) {
        if (!expectedValue.getClass().isArray()) {
          expectedValue = new Object[] {expectedValue};
        }
        if (!(value instanceof JSONArray)) {
          value = new JSONArray(value);
        }
        verify((JSONArray) value, expectedValue);
      } else {
        assertEquals(expectedValue, value);
      }
    }
    assertTrue(expectedMap.isEmpty(), "Unexpected leftover: " + expectedMap);
  }

  private void verify(JSONArray jsonArray, Object array) throws JSONException {
    int length = Array.getLength(array);
    assertEquals(length, jsonArray.length(), jsonArray + " length is not " + length);
    for (int i = 0; i < length; i++) {
      Object value = jsonArray.get(i);
      Object expected = Array.get(array, i);
      if (value instanceof JSONObject) {
        verify((JSONObject) value, (Tuple<?>[]) expected);
      } else if (value instanceof JSONArray) {
        verify((JSONArray) value, expected);
      } else {
        assertEquals(expected, value, "mismatch array[" + i + "] " + value + " != " + expected);
      }
    }
  }
}
