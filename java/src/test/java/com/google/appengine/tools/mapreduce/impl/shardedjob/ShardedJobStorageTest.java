package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.appengine.tools.mapreduce.EndToEndTestCase;

import com.google.cloud.datastore.*;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

/**
 * Tests the format in which ShardedJobs are written to the datastore.
 *
 */
public class ShardedJobStorageTest extends EndToEndTestCase {


  Datastore datastore;

  @Test
  public void testRoundTripJob() {
    ShardedJobStateImpl<TestTask> job = createGenericJobState();
    Entity entity = ShardedJobStateImpl.ShardedJobSerializer.toEntity(null, job);
    datastore.put(entity);
    Entity readEntity = datastore.get(entity.getKey());
    assertEquals(entity, readEntity);
    ShardedJobStateImpl<TestTask> fromEntity =
        ShardedJobStateImpl.ShardedJobSerializer.fromEntity(datastore, readEntity);
    assertEquals(job.getJobId(), fromEntity.getJobId());
    assertEquals(job.getActiveTaskCount(), fromEntity.getActiveTaskCount());
    assertEquals(job.getMostRecentUpdateTimeMillis(), fromEntity.getMostRecentUpdateTimeMillis());
    assertEquals(job.getStartTimeMillis(), fromEntity.getStartTimeMillis());
    assertEquals(job.getTotalTaskCount(), fromEntity.getTotalTaskCount());
    assertEquals(job.getSettings().toString(), fromEntity.getSettings().toString());
    assertEquals(job.getStatus(), fromEntity.getStatus());
    assertEquals(job.getController(), fromEntity.getController());
  }

  @Test
  public void testExpectedFields() {
    ShardedJobStateImpl<TestTask> job = createGenericJobState();
    Entity entity = ShardedJobStateImpl.ShardedJobSerializer.toEntity(null, job);
    Map<String, Value<?>> properties = entity.getProperties();
    assertEquals(10, entity.getLong("taskCount"));
    assertTrue(properties.containsKey("activeShards"));
    assertTrue(properties.containsKey("status"));
    assertTrue(properties.containsKey("startTimeMillis"));
    assertTrue(properties.containsKey("settings"));
    assertTrue(properties.containsKey("mostRecentUpdateTimeMillis"));
  }

  @Test
  public void testFetchJobById() {
    ShardedJobStateImpl<TestTask> job = createGenericJobState();
    Entity entity = ShardedJobStateImpl.ShardedJobSerializer.toEntity(null, job);
    datastore.put(entity);
    Entity readEntity = datastore.get(ShardedJobStateImpl.ShardedJobSerializer.makeKey(datastore, "jobId"));
    assertEquals(entity, readEntity);
  }

  private ShardedJobStateImpl<TestTask> createGenericJobState() {
    return ShardedJobStateImpl.create("jobId", new TestController(11),
        new ShardedJobSettings.Builder().build(), 10, System.currentTimeMillis());
  }

  @Test
  public void testQueryByKind() {
    EntityQuery.Builder builder = EntityQuery.newEntityQueryBuilder()
      .setKind(ShardedJobStateImpl.ShardedJobSerializer.ENTITY_KIND);

    EntityQuery query = builder.build();
    Iterator<Entity> iterable = datastore.run(query);
    assertFalse(iterable.hasNext());

    ShardedJobStateImpl<TestTask> job = createGenericJobState();
    Entity entity = ShardedJobStateImpl.ShardedJobSerializer.toEntity(null, job);
    datastore.put(entity);

    Entity singleEntity = datastore.run(query).next();
    assertEquals(entity, singleEntity);
  }
}
