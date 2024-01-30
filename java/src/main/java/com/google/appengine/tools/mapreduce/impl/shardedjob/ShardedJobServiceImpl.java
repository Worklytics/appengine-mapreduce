// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.google.cloud.datastore.Datastore;
import lombok.AllArgsConstructor;

import java.util.Iterator;
import java.util.List;

/**
 * Implementation of {@link ShardedJobService}.
 *
 * @author ohler@google.com (Christian Ohler)
 */
@AllArgsConstructor
class ShardedJobServiceImpl implements ShardedJobService {

  Datastore datastore;

  @Override
  public <T extends IncrementalTask> void startJob(
      String jobId,
      List<? extends T> initialTasks,
      ShardedJobController<T> controller,
      ShardedJobSettings settings) {
    new ShardedJobRunner<T>(datastore).startJob(jobId, initialTasks, controller, settings);
  }

  @Override
  public ShardedJobState getJobState(String jobId) {
    return new ShardedJobRunner<>(datastore).getJobState(jobId);
  }

  @Override
  public Iterator<IncrementalTaskState<IncrementalTask>> lookupTasks(ShardedJobState state) {
    return new ShardedJobRunner<>(datastore).lookupTasks(state.getJobId(), state.getTotalTaskCount(), true);
  }

  @Override
  public void abortJob(String jobId) {
    new ShardedJobRunner<>(datastore).abortJob(jobId);
  }

  @Override
  public boolean cleanupJob(String jobId) {
    return new ShardedJobRunner<>(datastore).cleanupJob(jobId);
  }
}
