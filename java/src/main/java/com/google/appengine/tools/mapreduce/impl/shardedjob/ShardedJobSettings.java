// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.modules.ModulesServiceFactory;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineEnvironment;
import lombok.*;

import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_BASE_URL;
import static com.google.appengine.tools.mapreduce.MapSettings.WORKER_PATH;
import static com.google.appengine.tools.mapreduce.MapSettings.CONTROLLER_PATH;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_SHARD_RETRIES;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_SLICE_RETRIES;

import java.io.IOException;
import java.io.Serializable;

/**
 * Execution settings for a sharded job.
 *
 * @author ohler@google.com (Christian Ohler)
 */
@Getter
@Builder(builderClassName = "Builder")
@ToString
public final class ShardedJobSettings implements Serializable {

  private static final long serialVersionUID = 286995366653078363L;

  public static final int DEFAULT_SLICE_TIMEOUT_MILLIS = 11 * 60000;

  //q: does this need to get bucketName / credentials?

  /*Nullable*/ private final String service;
  /*Nullable*/ private final String version;
  // TODO(ohler): Integrate with pipeline and put this under /_ah/pipeline.
  /*Nullable*/ private final String pipelineStatusUrl;


  /*Nullable*/ private final String mrStatusUrl;

  @lombok.Builder.Default
  @NonNull
  private final String controllerPath = DEFAULT_BASE_URL + CONTROLLER_PATH;

  @lombok.Builder.Default
  @NonNull
  private final String workerPath = DEFAULT_BASE_URL + WORKER_PATH;

  @NonNull
  @lombok.Builder.Default
  private final String queueName = "default";

  @lombok.Builder.Default
  private final int maxShardRetries= DEFAULT_SHARD_RETRIES;

  @lombok.Builder.Default
  private final int maxSliceRetries = DEFAULT_SLICE_RETRIES;

  @lombok.Builder.Default
  private final int sliceTimeoutMillis = DEFAULT_SLICE_TIMEOUT_MILLIS;


  @Deprecated // dependency on Modules service; move this out/up
  public String getTaskQueueTarget() {
    return ModulesServiceFactory.getModulesService().getVersionHostname(service, version);
  }
  /*Nullable*/ public String getMapReduceStatusUrl() {
    return mrStatusUrl;
  }
}
