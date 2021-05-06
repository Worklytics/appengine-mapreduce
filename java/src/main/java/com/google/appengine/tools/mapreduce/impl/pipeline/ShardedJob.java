// Copyright 2013 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.pipeline;

import com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTask;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobController;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobServiceFactory;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Value;
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

  @NonNull private final String jobId;
  @NonNull private final List<? extends T> workers;
  @NonNull private final ShardedJobController<T> controller;
  @NonNull private final ShardedJobSettings settings;

  @Override
  public Value<Void> run() {
    ShardedJobServiceFactory.getShardedJobService().startJob(jobId, workers, controller, settings);
    setStatusConsoleUrl(settings.getMapReduceStatusUrl());
    return null;
  }
}
