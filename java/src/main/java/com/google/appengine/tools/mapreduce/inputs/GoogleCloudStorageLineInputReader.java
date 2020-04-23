package com.google.appengine.tools.mapreduce.inputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageFileOutputWriter;
import com.google.auth.Credentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Getter;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * CloudStorageLineInputReader reads files from Cloud Storage one line at a time.
 *
 */
class GoogleCloudStorageLineInputReader extends InputReader<byte[]> {
  private static final long serialVersionUID = 2L;

  private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;


  public interface Options extends Serializable {

    Integer getBufferSize();

    Optional<Credentials> getCredentials();

  }

  @Builder
  public static class BaseOptions implements Options {

    @Getter @Builder.Default
    Integer bufferSize = DEFAULT_BUFFER_SIZE;

    private Credentials credentials;

    public static BaseOptions defaults() {
      return GoogleCloudStorageLineInputReader.BaseOptions.builder().build();
    }

    public Optional<Credentials> getCredentials() {
      return Optional.ofNullable(this.credentials);
    }

  }


  @VisibleForTesting final long startOffset;
  @VisibleForTesting final long endOffset;
  private final GcsFilename file;
  private long offset;
  private final byte separator;
  private Options options;

  private transient LineInputStream in;
  private transient Storage client;


  GoogleCloudStorageLineInputReader(GcsFilename file, long startOffset, long endOffset,
      byte separator) {
    this(file, startOffset, endOffset, separator, (Options) BaseOptions.defaults());
  }

  GoogleCloudStorageLineInputReader(GcsFilename file, long startOffset, long endOffset,
      byte separator, Options options) {
    this.separator = separator;
    this.file = checkNotNull(file, "Null file");
    Preconditions.checkArgument(endOffset >= startOffset);
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    Preconditions.checkArgument(options.getBufferSize() > 0, "buffersize must be > 0");
    this.options = options;
  }

  protected Storage getClient() {
    if (client == null) {
      //TODO: set retry param (GCS_RETRY_PARAMETERS)
      //TODO: set User-Agent to "App Engine MR"?
      if (this.options.getCredentials().isPresent()) {
        client = StorageOptions.newBuilder()
          .setCredentials(this.options.getCredentials().get())
          .build().getService();
      } else {
        client = StorageOptions.getDefaultInstance().getService();
      }
    }
    return client;
  }


  @Override
  public Double getProgress() {
    if (endOffset == startOffset) {
      return 1.0;
    } else {
      double currentOffset = offset + (in == null ? 0 : in.getBytesCount());
      return Math.min(1.0, currentOffset / (endOffset - startOffset));
    }
  }

  @Override
  public void beginShard() {
    offset = 0;
    in = null;
  }

  @Override
  public void beginSlice() throws IOException {
    Preconditions.checkState(in == null, "%s: Already initialized: %s", this, in);

    ReadChannel reader = getClient().reader(file.getBucketName(), file.getObjectName());
    reader.setChunkSize(options.getBufferSize());
    reader.seek(startOffset + offset);

    @SuppressWarnings("resource")
    InputStream inputStream = Channels.newInputStream(reader);
    in = new LineInputStream(inputStream, endOffset - startOffset - offset, separator);
    skipRecordReadByPreviousShard();
  }

  /**
   * The previous record is responsible for reading past it's endOffset until a whole record is
   * read.
   */
  private void skipRecordReadByPreviousShard() {
    if (startOffset != 0L && offset == 0L) {
      try {
        in.next();
      } catch (NoSuchElementException e) {
        // Empty slice is ok.
      }
    }
  }

  @Override
  public void endSlice() throws IOException {
    offset += in.getBytesCount();
    in.close();
    in = null;
  }

  @Override
  public byte[] next() throws NoSuchElementException {
    return in.next();
  }

  @Override
  public long estimateMemoryRequirement() {
    return options.getBufferSize() * 2; // Double buffered
  }
}
