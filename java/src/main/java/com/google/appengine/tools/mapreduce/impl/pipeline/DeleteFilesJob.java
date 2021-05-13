package com.google.appengine.tools.mapreduce.impl.pipeline;


import com.google.appengine.tools.mapreduce.GcpCredentialOptions;
import com.google.appengine.tools.mapreduce.GcsFilename;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Value;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A job which deletes all the files in the provided GoogleCloudStorageFileSet
 */
@RequiredArgsConstructor
public class DeleteFilesJob extends Job1<Void, List<GcsFilename>> {

  private static final long serialVersionUID = 4821135390816992131L;
  private static final Logger log = Logger.getLogger(DeleteFilesJob.class.getName());

  private final GcpCredentialOptions credentialOptions;


  /**
   * Deletes the files in the provided GoogleCloudStorageFileSet
   */
  @Override
  public Value<Void> run(List<GcsFilename> files) throws Exception {
    Storage storage = GcpCredentialOptions.getStorageClient(credentialOptions);
    try {
      List<Boolean> results = storage.delete((BlobId[]) files.stream().map(GcsFilename::asBlobId).toArray());
      for (int i = 0; i < results.size(); i++)  {
        if (!results.get(i)) {
          log.log(Level.WARNING, "Failed to cleanup file: " + files.get(i));
        }
      }
    } catch (Throwable e) {
      log.log(Level.WARNING, "Failed to cleanup files", e);
    }

    return null;
  }
}
