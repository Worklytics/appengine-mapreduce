// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Transaction;

import java.util.Iterator;
import java.util.List;

/**
 * Implementation of {@link ShardedJobService}.
 *
 * @author ohler@google.com (Christian Ohler)
 */
class ShardedJobServiceImpl implements ShardedJobService {

  @Override
  public <T extends IncrementalTask> void startJob(
    Datastore datastore, String jobId,
    List<? extends T> initialTasks,
    ShardedJobController<T> controller,
    ShardedJobSettings settings) {
    new ShardedJobRunner<T>().startJob(datastore, jobId, initialTasks, controller, settings);
  }

  @Override
  public ShardedJobState getJobState(Datastore datastore, String jobId) {
    return new ShardedJobRunner<>().getJobState(datastore, jobId);
  }

  @Override
  public Iterator<IncrementalTaskState<IncrementalTask>> lookupTasks(Transaction tx, ShardedJobState state) {
    return new ShardedJobRunner<>().lookupTasks(tx, state.getJobId(), state.getTotalTaskCount(), true);
  }

  @Override
  public void abortJob(Datastore datastore, String jobId) {
    new ShardedJobRunner<>().abortJob(datastore, jobId);
  }

  @Override
  public boolean cleanupJob(Datastore datastore, String jobId) {
    return new ShardedJobRunner<>().cleanupJob(datastore, jobId);
  }
}
