package com.google.appengine.tools.mapreduce.outputs;

import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.DEFAULT_IO_BUFFER_SIZE;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.GcsFilename;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.MapReduceConstants;
import com.google.auth.Credentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.*;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An {@link OutputWriter} that writes bytes to a GCS file that it creates. Produces a single file
 * output (usually on a per-shard basis). This format does not insert any separator characters, so
 * it by default cannot be read back with the CloudStorageLineInputReader.
 *
 */
@RequiredArgsConstructor
@ToString
public class GoogleCloudStorageFileOutputWriter extends OutputWriter<ByteBuffer> {
  private static final long serialVersionUID = 2L;
  private static final Logger logger =
      Logger.getLogger(GoogleCloudStorageFileOutputWriter.class.getName());

  private static final Random RND = new SecureRandom();

  public static final long MEMORY_REQUIRED_WITHOUT_SLICE_RETRY =
      MapReduceConstants.DEFAULT_IO_BUFFER_SIZE * 2;
  public static final long MEMORY_REQUIRED = MapReduceConstants.DEFAULT_IO_BUFFER_SIZE * 3;

  @Getter
  @NonNull private final GcsFilename file;
  @NonNull private final String mimeType;
  @NonNull private final Options options;

  private transient Storage client;
  private BlobId shardBlobId;
  private BlobId sliceBlobId;
  private transient WriteChannel sliceChannel;
  private List<BlobId> toDelete = new ArrayList<>();

  interface Options extends Serializable {

    Boolean getSupportSliceRetries();

    Optional<Credentials> getCredentials();

    String getProjectId();
  }

  @Getter
  @Builder
  @With
  @ToString
  public static class BaseOptions implements Serializable, Options {

    @Builder.Default
    private final Boolean supportSliceRetries = true;

    private Credentials credentials;

    private String projectId;

    public static BaseOptions defaults() {
      return BaseOptions.builder().build();
    }

    public Optional<Credentials> getCredentials() {
      return Optional.ofNullable(this.credentials);
    }
  }

  @Override
  public void cleanup() {
    for (BlobId id : toDelete) {
      try {
        getClient().delete(id);
      } catch (StorageException ex) {
        logger.log(Level.WARNING, "Could not cleanup temporary file " + id.getName(), ex);
      }
    }
    toDelete.clear();
  }



  protected Storage getClient() {
    if (client == null) {
      //TODO: set retry param (GCS_RETRY_PARAMETERS)
      //TODO: set User-Agent to "App Engine MR"?
      if (this.options.getCredentials().isPresent()) {
        client = StorageOptions.newBuilder()
          .setCredentials(this.options.getCredentials().get())
          .setProjectId(this.options.getProjectId())
          .build().getService();
      } else {
        client = StorageOptions.getDefaultInstance().getService();
      }
    }
    return client;
  }

  @Override
  public void beginShard() throws IOException {

    BlobInfo blobInfo = BlobInfo.newBuilder(file.getBucketName(), file.getObjectName() + "~")
      .setContentType(mimeType)
      .build();
    Blob shardBlob = getClient().create(blobInfo);
    shardBlobId = BlobId.of(shardBlob.getBucket(), shardBlob.getName());
    sliceChannel = null;
    toDelete.clear();
  }

  @Override
  public void beginSlice() throws IOException {
    cleanup();
    if (options.getSupportSliceRetries()) {
      if (sliceBlobId != null) {
        //append latest version of previous slice's file to shard's file
        // q: why not do this in endSlice??
        // q: why are we doing this as part of every slice? why not just do a big compose of all slices
        // into shard at endShard?
        BlobId latestSliceBlobId = BlobId.of(sliceBlobId.getBucket(), sliceBlobId.getName());

        //q: race condition here? what if this append() doesn't really see the 'latest' copy of blob?
        append(latestSliceBlobId, shardBlobId);
        toDelete.add(latestSliceBlobId);
      }
      String name = file.getObjectName() + "~" + Math.abs(RND.nextLong());
      Blob sliceBlob = getClient().create(BlobInfo.newBuilder(file.getBucketName(), name)
        .setContentType(mimeType).build());
      sliceBlobId = BlobId.of(sliceBlob.getBucket(), sliceBlob.getName());
      sliceChannel = sliceBlob.writer();
    } else {
      //if won't retry slices, can just write straight into shard's blob
      sliceChannel = getClient().get(shardBlobId).writer();
    }
    sliceChannel.setChunkSize(DEFAULT_IO_BUFFER_SIZE);
  }


  void append(BlobId src, BlobId dest) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(MapReduceConstants.DEFAULT_IO_BUFFER_SIZE);
    WriteChannel destChannel =
      getClient().writer(BlobInfo.newBuilder(dest.getBucket(), dest.getName()).build());
    try (ReadChannel reader = getClient().reader(src)) {
      while (reader.read(buffer) >= 0) {
        buffer.flip();
        while (buffer.hasRemaining()) {
          destChannel.write(buffer);
        }
        buffer.clear();
      }
    }
    destChannel.close();
    //q: return dest with updated version number? (generation ID?)
  }

  @Override
  public void write(ByteBuffer bytes) throws IOException {
    Preconditions.checkState(sliceChannel != null, "%s: channel was not created", this);
    while (bytes.hasRemaining()) {
      sliceChannel.write(bytes);
    }
  }

  @Override
  public void endSlice() throws IOException {
    sliceChannel.close();
  }

  @Override
  public void endShard() throws IOException {
    if (sliceBlobId == null) {
      return;
    }
    if (options.getSupportSliceRetries() && sliceChannel != null) {
      // compose temporary destination and last slice to final destination
      List<String> source = ImmutableList.of(shardBlobId.getName(), sliceBlobId.getName());
      BlobInfo dest = BlobInfo.newBuilder(file.getBucketName(), file.getObjectName())
        .setContentType(this.mimeType)
        .build();
      // unclear that we can "compose" if also using customer-managed encryption keys because:
      // https://cloud.google.com/storage/docs/encryption/customer-managed-keys#key-resources
      // "when one or more of the source objects are encrypted with a customer-managed encryption key."
      // but https://cloud.google.com/storage/docs/json_api/v1/objects/compose
      // says "To compose objects encrypted by a customer-supplied encryption key, use the headers listed on the Encryption page in your request."
      getClient().compose(Storage.ComposeRequest.of(source, dest));
    } else {
      // rename temporary destination to final destination
      //q: race condition here? what if this copy() doesn't really see the 'latest' copy of sliceBlob?
      // is there a way to get the latest generation number from our last write, so that we can make
      // this copy request contingent upon seeing that??

      getClient().copy(Storage.CopyRequest.of(BlobId.of(sliceBlobId.getBucket(), sliceBlobId.getName()), BlobInfo.newBuilder(file.getBucketName(), file.getObjectName()).build()));
    }

    //queue blob for deletion
    toDelete.add(BlobId.of(shardBlobId.getBucket(), shardBlobId.getName()));
    shardBlobId = null;
    sliceChannel = null;
  }

  @Override
  public long estimateMemoryRequirement() {
    return MEMORY_REQUIRED;
  }


}
