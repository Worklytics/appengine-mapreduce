// Copyright 2013 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.pipeline;

import com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTask;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobController;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobServiceFactory;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Value;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.List;

/**
 * ShardedJob pipeline.
 *
 *
 * @param <T> type of task
 */
@RequiredArgsConstructor
public class ShardedJob<T extends IncrementalTask> extends Job0<Void> {

  private static final long serialVersionUID = 1L;

  // alternatively, put namespace on SharedJobSettings; project+databaseId+namespace there; or something else
  @NonNull private final DatastoreOptions datastoreOptions;
  @NonNull private final String jobId;
  @NonNull private final List<? extends T> workers;
  @NonNull private final ShardedJobController<T> controller;
  @NonNull private final ShardedJobSettings settings;

  @Override
  public Value<Void> run() {
    Datastore datastore = datastoreOptions.getService();
    ShardedJobServiceFactory.getShardedJobService().startJob(datastore, jobId, workers, controller, settings);
    setStatusConsoleUrl(settings.getMapReduceStatusUrl());
    return null;
  }
}
