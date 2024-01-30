package com.google.appengine.tools.mapreduce.impl.shardedjob.pipeline;

import static java.util.concurrent.Executors.callable;

import com.github.rholder.retry.StopStrategies;
import com.google.appengine.tools.mapreduce.RetryExecutor;
import com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTask;
import com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTaskState;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardRetryState;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunner;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Value;
import com.google.cloud.datastore.*;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

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
    //TODO: inject this or something
    // (q: should we obtain from Job? maybe, as property of execution context?? )
    Datastore datastore = DatastoreOptions.newBuilder().build().getService();

    final List<Key> toFetch = new ArrayList<>(end - start);
    final Queue<Entity> toUpdate = new ConcurrentLinkedQueue<>();
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

    RetryExecutor.call(
      ShardedJobRunner.getRetryerBuilder().withStopStrategy(StopStrategies.neverStop()),
      callable(() -> {




      Iterator<Entity> entities = datastore.get(toFetch);
      entities.forEachRemaining(entity -> {
        IncrementalTaskState<IncrementalTask> taskState =
          IncrementalTaskState.Serializer.fromEntity(datastore, entity, true);
        if (taskState.getTask() != null) {
          taskState.getTask().jobCompleted(status);
          toUpdate.add(IncrementalTaskState.Serializer.toEntity(null, taskState));
        }
      });

      if (!toUpdate.isEmpty()) {
        datastore.put(toUpdate.stream().toArray(Entity[]::new));
      }

        Runnable deletion = () -> {
          datastore.delete(toDelete.stream().toArray(Key[]::new));
        };
        deletion.run();
    }));

    return null;
  }

  @Override
  public String getJobDisplayName() {
    return "FinalizeShardsInfos: " + jobId + "[" + start + "-" + end + "]";
  }
}
