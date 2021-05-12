package com.google.appengine.tools.mapreduce.impl.pipeline;

import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.RetriesExhaustedException;
import com.google.appengine.tools.cloudstorage.RetryParams;
import com.google.appengine.tools.mapreduce.GcsFilename;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Value;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A job which deletes all the files in the provided GoogleCloudStorageFileSet
 */
public class DeleteFilesJob extends Job1<Void, List<GcsFilename>> {

  public static final RetryParams GCS_RETRY_PARAMETERS = new RetryParams.Builder()
    .requestTimeoutMillis(30000)
    .retryMaxAttempts(10)
    .retryMinAttempts(6)
    .maxRetryDelayMillis(30000)
    .totalRetryPeriodMillis(120000)
    .initialRetryDelayMillis(250)
    .build();

  private static final long serialVersionUID = 4821135390816992131L;
  private static final GcsService gcs = GcsServiceFactory.createGcsService(GCS_RETRY_PARAMETERS);
  private static final Logger log = Logger.getLogger(DeleteFilesJob.class.getName());

  /**
   * Deletes the files in the provided GoogleCloudStorageFileSet
   */
  @Override
  public Value<Void> run(List<GcsFilename> files) throws Exception {
    for (GcsFilename file : files) {
      try {
        gcs.delete(new com.google.appengine.tools.cloudstorage.GcsFilename(file.getBucketName(), file.getObjectName()));
      } catch (RetriesExhaustedException | IOException e) {
        log.log(Level.WARNING, "Failed to cleanup file: " + file, e);
      }
    }
    return null;
  }
}
