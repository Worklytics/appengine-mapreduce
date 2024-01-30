// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import static com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings.DEFAULT_SLICE_TIMEOUT_MILLIS;
import static com.google.common.base.Preconditions.checkNotNull;

import com.github.rholder.retry.*;
import com.google.appengine.api.backends.BackendService;
import com.google.appengine.api.backends.BackendServiceFactory;
import com.google.appengine.api.modules.ModulesException;
import com.google.appengine.api.modules.ModulesService;
import com.google.appengine.api.modules.ModulesServiceFactory;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TransientFailureException;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineEnvironment;
import com.google.appengine.tools.pipeline.impl.servlets.PipelineServlet;
import com.google.cloud.datastore.Key;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Settings that affect how a Map job is executed.  May affect performance and
 * resource usage, but should not affect the result (unless the result is
 * dependent on the performance or resource usage of the computation, or if
 * different backends, modules or different base urls have different versions of the code).
 */
@SuperBuilder(toBuilder = true)
@ToString
@Getter
public class MapSettings implements Serializable {

  private static final long serialVersionUID = 51425056338041064L;

  private static final RetryerBuilder getQueueRetryerBuilder() {
    return RetryerBuilder.newBuilder()
      .withWaitStrategy(WaitStrategies.exponentialWait(20_000, TimeUnit.MILLISECONDS))
      .withStopStrategy(StopStrategies.stopAfterAttempt(8))
      .retryIfExceptionOfType(TransientFailureException.class);
  }


  public static final String DEFAULT_BASE_URL = "/mapreduce/";
  public static final String CONTROLLER_PATH = "controllerCallback";
  public static final String WORKER_PATH = "workerCallback";
  public static final int DEFAULT_MILLIS_PER_SLICE = 180_000;
  public static final double DEFAULT_SLICE_TIMEOUT_RATIO = 1.1;
  public static final int DEFAULT_SHARD_RETRIES = 4;
  public static final int DEFAULT_SLICE_RETRIES = 20;

  @lombok.Builder.Default
  private final String baseUrl = DEFAULT_BASE_URL;
  private final String module;
  private final String workerQueueName;

  @lombok.Builder.Default
  private final int millisPerSlice = DEFAULT_MILLIS_PER_SLICE;

  @lombok.Builder.Default
  private final double sliceTimeoutRatio = DEFAULT_SLICE_TIMEOUT_RATIO;

  @lombok.Builder.Default
  private final int maxShardRetries = DEFAULT_SHARD_RETRIES;

  @lombok.Builder.Default
  private final int maxSliceRetries = DEFAULT_SLICE_RETRIES;


  @Override
  public String toString() {
    return getClass().getSimpleName() + "("
        + baseUrl + ", "
        + module + ", "
        + workerQueueName + ", "
        + millisPerSlice + ", "
        + sliceTimeoutRatio + ", "
        + maxSliceRetries + ", "
        + maxShardRetries + ")";
  }

  JobSetting[] toJobSettings(JobSetting... extra) {
    JobSetting[] settings = new JobSetting[3 + extra.length];
    settings[0] = new JobSetting.OnService(module);
    settings[1] = new JobSetting.OnQueue(workerQueueName);
    System.arraycopy(extra, 0, settings, 3, extra.length);
    return settings;
  }


  ShardedJobSettings toShardedJobSettings(AppEngineEnvironment environment, String shardedJobId, Key pipelineKey) {

    String module = getModule();
    String version = null;


    if (module == null) {
      module = environment.getService();
      version = environment.getVersion();
    }
    final ShardedJobSettings.Builder builder = ShardedJobSettings.builder()
        .controllerPath(baseUrl + CONTROLLER_PATH + "/" + shardedJobId)
        .workerPath(baseUrl + WORKER_PATH + "/" + shardedJobId)
        .mrStatusUrl(baseUrl + "detail?mapreduce_id=" + shardedJobId)
        .pipelineStatusUrl(PipelineServlet.makeViewerUrl(pipelineKey, pipelineKey))
        .service(module)
        .version(version)
        .queueName(workerQueueName)
        .maxShardRetries(maxShardRetries)
        .maxSliceRetries(maxSliceRetries)
        .sliceTimeoutMillis(
            Math.max(DEFAULT_SLICE_TIMEOUT_MILLIS, (int) (millisPerSlice * sliceTimeoutRatio)));
    return builder.build();
  }

  private String checkQueueSettings(String queueName) {
    if (queueName == null) {
      return null;
    }
    final Queue queue = QueueFactory.getQueue(queueName);
    try {
      // Does not work as advertise (just check that the queue name is valid).
      // See b/13910616. Probably after the bug is fixed the check would need
      // to inspect EnforceRate for not null.
      RetryExecutor.call(getQueueRetryerBuilder(), () -> {
          // Does not work as advertise (just check that the queue name is valid).
          // See b/13910616. Probably after the bug is fixed the check would need
          // to inspect EnforceRate for not null.
          queue.fetchStatistics();
          return null;
        });
    } catch (Throwable ex) {
      if (ex instanceof ExecutionException) {
        if (ex.getCause() instanceof IllegalStateException) {
          throw new RuntimeException("Queue '" + queueName + "' does not exists");
        }
        throw new RuntimeException(
          "Could not check if queue '" + queueName + "' exists", ex.getCause());
      } else {
        throw ex;
      }
    }
    return queueName;
  }
}
