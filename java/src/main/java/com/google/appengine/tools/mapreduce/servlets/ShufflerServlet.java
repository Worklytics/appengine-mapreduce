// Copyright 2014 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.appengine.tools.mapreduce.servlets;

import static java.util.concurrent.Executors.callable;

import com.google.appengine.api.modules.ModulesServiceFactory;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskAlreadyExistsException;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.tools.cloudstorage.ExceptionHandler;
import com.google.appengine.tools.cloudstorage.RetryHelper;
import com.google.appengine.tools.cloudstorage.RetryParams;
import com.google.appengine.tools.mapreduce.*;
import com.google.appengine.tools.mapreduce.impl.MapReduceConstants;
import com.google.appengine.tools.mapreduce.inputs.GoogleCloudStorageLevelDbInput;
import com.google.appengine.tools.mapreduce.inputs.GoogleCloudStorageLineInput;
import com.google.appengine.tools.mapreduce.inputs.UnmarshallingInput;
import com.google.appengine.tools.mapreduce.mappers.IdentityMapper;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageFileOutput;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageLevelDbOutput;
import com.google.appengine.tools.mapreduce.outputs.MarshallingOutput;
import com.google.appengine.tools.mapreduce.reducers.IdentityReducer;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.appengine.tools.pipeline.Value;
import com.google.apphosting.api.ApiProxy.ArgumentException;
import com.google.apphosting.api.ApiProxy.RequestTooLargeException;
import com.google.apphosting.api.ApiProxy.ResponseTooLargeException;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This servlet provides a way for Python MapReduce Jobs to use the Java MapReduce as a shuffle. It
 * takes in a list of files to shuffle and a task queue to send the completion notification to. When
 * the job finishes a message will be sent to that queue which indicates the status and where to
 * find the results.
 */
public class ShufflerServlet extends HttpServlet {

  private static final long serialVersionUID = 2L;

  private static final Logger log = Logger.getLogger(ShufflerServlet.class.getName());

  private static final String MIME_TYPE = "application/octet-stream";

  private static final int MAX_VALUES_COUNT = 10000;

  private static final ExceptionHandler EXCEPTION_HANDLER = new ExceptionHandler.Builder()
      .retryOn(Exception.class).abortOn(IllegalArgumentException.class,
                                        RequestTooLargeException.class, ResponseTooLargeException.class, ArgumentException.class)
      .build();

  private static final RetryParams RETRY_PARAMS = new RetryParams.Builder()
    .initialRetryDelayMillis(1000)
    .maxRetryDelayMillis(30000)
    .retryMinAttempts(10)
    .retryMaxAttempts(10)
    .build();

  @VisibleForTesting
  static final class ShuffleMapReduce extends Job0<Void> {

    private static final long serialVersionUID = 2L;

    private final Marshaller<ByteBuffer> identityMarshaller = Marshallers.getByteBufferMarshaller();

    private final ShufflerParams shufflerParams;

    public ShuffleMapReduce(ShufflerParams shufflerParams) {
      this.shufflerParams = shufflerParams;
    }

    @Override
    public Value<Void> run() throws Exception {
      MapReduceJob<KeyValue<ByteBuffer, ByteBuffer>, ByteBuffer, ByteBuffer,
          KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>, GoogleCloudStorageFileSet> job =
          new MapReduceJob<>(createSpec(), createSettings());

      FutureValue<MapReduceResult<GoogleCloudStorageFileSet>> result = futureCall(job);

      // Take action once the Map Reduce job is complete.
      return futureCall(new Complete(shufflerParams), result, maxAttempts(10));
    }

    private MapReduceSettings createSettings() {
      return new MapReduceSettings.Builder()
          .setBucketName(shufflerParams.getGcsBucket())
          .setWorkerQueueName(shufflerParams.getShufflerQueue())
          .setServiceAccountKey(shufflerParams.getServiceAccountKey())
          .build();
    }

    private MapReduceSpecification<KeyValue<ByteBuffer, ByteBuffer>, ByteBuffer, ByteBuffer,
      KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>, GoogleCloudStorageFileSet>
        createSpec() {
      return new MapReduceSpecification.Builder<KeyValue<ByteBuffer, ByteBuffer>, ByteBuffer,
          ByteBuffer, KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>,
          GoogleCloudStorageFileSet>()

          .setInput(createInput())
          .setMapper(new IdentityMapper<>())
          .setReducer(new IdentityReducer<>(MAX_VALUES_COUNT))
          .setOutput(createOutput())
          .setJobName("Shuffle")
          .setKeyMarshaller(identityMarshaller)
          .setValueMarshaller(identityMarshaller)
          .setNumReducers(shufflerParams.getOutputShards())
          .build();
    }

    private MarshallingOutput<KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>,
        GoogleCloudStorageFileSet> createOutput() {
      String jobId = getPipelineKey().getName();

      GoogleCloudStorageFileOutput.Options gcsOutputOptions = GoogleCloudStorageFileOutput.BaseOptions.defaults()
        .withServiceAccountKey(shufflerParams.getServiceAccountKey());

      return new MarshallingOutput<>(
        new GoogleCloudStorageLevelDbOutput(shufflerParams.getGcsBucket(), getOutputNamePattern(jobId), MIME_TYPE, gcsOutputOptions),
          Marshallers.getKeyValuesMarshaller(identityMarshaller, identityMarshaller)
      );
    }

    @VisibleForTesting
    String getOutputNamePattern(String jobId) {
      return shufflerParams.getOutputDir() + "/sortedData-" + jobId + "/shard-%04d";
    }

    private UnmarshallingInput<KeyValue<ByteBuffer, ByteBuffer>> createInput() {
      List<String> fileNames = Arrays.asList(shufflerParams.getInputFileNames());
      return new UnmarshallingInput<>(new GoogleCloudStorageLevelDbInput(
          new GoogleCloudStorageFileSet(shufflerParams.getGcsBucket(), fileNames), GoogleCloudStorageLineInput.BaseOptions.defaults().withServiceAccountKey(shufflerParams.getServiceAccountKey())),
          Marshallers.getKeyValueMarshaller(identityMarshaller, identityMarshaller));
    }

    /**
     * Logs the error and notifies the requester.
     */
    public Value<Void> handleException(Throwable t) {
      String jobId = getPipelineKey().getName();
      log.log(Level.SEVERE, "Shuffle job failed: jobId=" + jobId, t);
      enqueueCallbackTask(shufflerParams, "job=" + jobId + "&status=failed", "Shuffled-" + jobId);
      return immediate(null);
    }
  }

  /**
   * Save the output filenames in GCS with one filename per line. Then invokes
   * {@link #enqueueCallbackTask}
   */
  private static final class Complete extends
      Job1<Void, MapReduceResult<GoogleCloudStorageFileSet>> {
    private static final long serialVersionUID = 2L;
    private final ShufflerParams shufflerParams;

    private Complete(ShufflerParams shufflerParams) {
      this.shufflerParams = shufflerParams;
    }

    @Override
    public Value<Void> run(MapReduceResult<GoogleCloudStorageFileSet> result) throws Exception {
      String jobId = getPipelineKey().getName();

      String manifestPath = shufflerParams.getOutputDir() + "/Manifest-" + jobId + ".txt";

      log.info("Shuffle job done: jobId=" + jobId + ", results located in " + manifestPath + "]");

      Storage client = GcpCredentialOptions.getStorageClient(this.shufflerParams);

      Blob blob = client.create(BlobInfo.newBuilder(shufflerParams.getGcsBucket(), manifestPath).setContentType("text/plain").build());

      WriteChannel output = blob.writer();

      for (com.google.appengine.tools.mapreduce.GcsFilename fileName : result.getOutputResult().getFiles()) {
        output.write(StandardCharsets.UTF_8.encode(fileName.getObjectName()));
        output.write(StandardCharsets.UTF_8.encode("\n"));
      }
      output.close();

      enqueueCallbackTask(shufflerParams,
          "job=" + jobId + "&status=done&output=" + URLEncoder.encode(manifestPath, "UTF-8"),
          "Shuffled-" + jobId);
      return null;
    }
  }

  /**
   * Notifies the caller that the job has completed.
   */
  private static void enqueueCallbackTask(final ShufflerParams shufflerParams, final String url,
      final String taskName) {
    RetryHelper.runWithRetries(callable(new Runnable() {
      @Override
      public void run() {
        String hostname = ModulesServiceFactory.getModulesService().getVersionHostname(
            shufflerParams.getCallbackService(), shufflerParams.getCallbackVersion());
        Queue queue = QueueFactory.getQueue(shufflerParams.getCallbackQueue());
        String separator = shufflerParams.getCallbackPath().contains("?") ? "&" : "?";
        try {
          queue.add(TaskOptions.Builder.withUrl(shufflerParams.getCallbackPath() + separator + url)
              .method(TaskOptions.Method.GET).header("Host", hostname).taskName(taskName));
        } catch (TaskAlreadyExistsException e) {
          // harmless dup.
        }
      }
    }), RETRY_PARAMS, EXCEPTION_HANDLER);
  }

  @VisibleForTesting
  static ShufflerParams readShufflerParams(InputStream in) throws IOException {
    Marshaller<ShufflerParams> marshaller =
        Marshallers.getGenericJsonMarshaller(ShufflerParams.class);
    ShufflerParams params = marshaller.fromBytes(ByteBuffer.wrap(ByteStreams.toByteArray(in)));
    if (params.getOutputShards() <= 0
        || params.getOutputShards() > MapReduceConstants.MAX_REDUCE_SHARDS) {
      throw new IllegalArgumentException(
          "Invalid requested number of shards: " + params.getOutputShards());
    }
    if (params.getOutputDir().length() > 850) {
      throw new IllegalArgumentException(
          "OutputDir is too long: " + params.getOutputDir().length());
    }
    if (params.getOutputDir().contains("\n")) {
      throw new IllegalArgumentException("OutputDir may not contain a newline");
    }
    if (params.getGcsBucket() == null) {
      throw new IllegalArgumentException("GcsBucket parameter is mandatory");
    }
    if (params.getCallbackService() == null || params.getCallbackVersion() == null) {
      throw new IllegalArgumentException(
          "CallbackModule and CallbackVersion parameters are mandatory");
    }
    return params;
  }

  @Override
  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    ShufflerParams shufflerParams = readShufflerParams(req.getInputStream());
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new ShuffleMapReduce(shufflerParams),
        new JobSetting.OnQueue(shufflerParams.getShufflerQueue()));
    log.info("Started shuffler: jobId=" + pipelineId + ", params=" + shufflerParams);

    resp.setStatus(HttpServletResponse.SC_OK);
    resp.getWriter().append(pipelineId);
  }

}
