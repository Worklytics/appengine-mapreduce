package com.google.appengine.tools.mapreduce.impl.shardedjob.pipeline;

import static java.util.concurrent.Executors.callable;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.StopStrategies;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTask;
import com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTaskState;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardRetryState;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunner;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Value;
import com.google.common.base.Throwables;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A pipeline job for finalizing the shards information and cleaning up unnecessary state.
 */
public class FinalizeShardsInfos extends Job0<Void> {

  private static final long serialVersionUID = -2315635076911526336L;

  private final String jobId;
  private final Status status;
  private final int start;
  private final int end;

  FinalizeShardsInfos(String jobId, Status status, int start, int end) {
    this.jobId = jobId;
    this.status = status;
    this.start = start;
    this.end = end;
  }

  @Override
  public Value<Void> run() {
    final List<Key> toFetch = new ArrayList<>(end - start);
    final List<Entity> toUpdate = new ArrayList<>();
    final List<Key> toDelete = new ArrayList<>();
    for (int i = start; i < end; i++) {
      String taskId = ShardedJobRunner.getTaskId(jobId, i);
      toFetch.add(IncrementalTaskState.Serializer.makeKey(taskId));
      Key retryStateKey = ShardRetryState.Serializer.makeKey(taskId);
      toDelete.add(retryStateKey);
      for (Key key : SerializationUtil.getShardedValueKeysFor(null, retryStateKey, null)) {
        toDelete.add(key);
      }
    }
    try {
      ShardedJobRunner.getRetryerBuilder().withStopStrategy(StopStrategies.neverStop())
        .build().call(callable(() -> {
        Future<Void> deleteAsync =
          DatastoreServiceFactory.getAsyncDatastoreService().delete(null, toDelete);
        Map<Key, Entity> entities = DatastoreServiceFactory.getDatastoreService().get(toFetch);
        Iterator<Key> keysIter = toFetch.iterator();
        while (keysIter.hasNext()) {
          Entity entity = entities.get(keysIter.next());
          if (entity != null) {
            IncrementalTaskState<IncrementalTask> taskState =
              IncrementalTaskState.Serializer.fromEntity(entity, true);
            if (taskState.getTask() != null) {
              taskState.getTask().jobCompleted(status);
              toUpdate.add(IncrementalTaskState.Serializer.toEntity(null, taskState));
            }
          }
        }
        if (!toUpdate.isEmpty()) {
          DatastoreServiceFactory.getDatastoreService().put(null, toUpdate);
        }
        try {
          deleteAsync.get();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Request interrupted");
        } catch (ExecutionException e) {
          Throwables.propagateIfPossible(e.getCause());
          throw new RuntimeException("Async get failed", e);
        }
      }));
    } catch (ExecutionException | RetryException e) {
      throw new RuntimeException(e);
    }

    return null;
  }

  @Override
  public String getJobDisplayName() {
    return "FinalizeShardsInfos: " + jobId + "[" + start + "-" + end + "]";
  }
}
