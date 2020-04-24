// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
import com.google.appengine.api.appidentity.AppIdentityServiceFailureException;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.ToString;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Settings that affect how a MapReduce is executed. May affect performance and resource usage, but
 * should not affect the result (unless the result is dependent on the performance or resource usage
 * of the computation, or if different backends, modules or different base urls have different
 * versions of the code).
 *
 * @author ohler@google.com (Christian Ohler)
 */
@ToString
public class MapReduceSettings extends MapSettings {

  private static final long serialVersionUID = 610088354289299175L;
  private static final Logger log = Logger.getLogger(MapReduceSettings.class.getName());
  public static final int DEFAULT_MAP_FANOUT = 32;
  public static final int DEFAULT_SORT_BATCH_PER_EMIT_BYTES = 32 * 1024;
  public static final int DEFAULT_SORT_READ_TIME_MILLIS = 180000;
  public static final int DEFAULT_MERGE_FANIN = 32;


  @Getter
  private final String bucketName;
  @Getter
  private final int mapFanout;
  @Getter
  private final Long maxSortMemory;
  @Getter
  private final int sortReadTimeMillis;
  @Getter
  private final int sortBatchPerEmitBytes;
  @Getter
  private final int mergeFanin;

  /**
   * credentials to use when accessing
   *
   * NOTE: as these will be serialized / copied to datastore/etc during pipeline execution, this exists mainly for dev
   * purposes where you're running outside a GCP environment where the default implicit credentials will work. For
   * production use, relying on implicit credentials and granting the service account under which your code is executing
   * in GAE/GCE/etc is the most secure approach, as no keys need to be generated or passed around, which always entails
   * some risk of exposure.
   */
  @Getter
  private final Credentials storageCredentials;

  public static class Builder extends BaseBuilder<Builder> {

    private String bucketName;
    private int mapFanout = DEFAULT_MAP_FANOUT;
    private Long maxSortMemory;
    private int sortReadTimeMillis = DEFAULT_SORT_READ_TIME_MILLIS;
    private int sortBatchPerEmitBytes = DEFAULT_SORT_BATCH_PER_EMIT_BYTES;
    private int mergeFanin = DEFAULT_MERGE_FANIN;
    @Getter
    private Credentials storageCredentials;

    public Builder() {}

    @SneakyThrows
    public Builder(MapReduceSettings settings) {
      super(settings);
      this.bucketName = settings.bucketName;
      this.mapFanout = settings.mapFanout;
      this.maxSortMemory = settings.maxSortMemory;
      this.sortReadTimeMillis = settings.sortReadTimeMillis;
      this.sortBatchPerEmitBytes = settings.sortBatchPerEmitBytes;
      this.mergeFanin = settings.mergeFanin;
      this.storageCredentials = GoogleCredentials.getApplicationDefault();
    }

    public Builder(MapSettings settings) {
      super(settings);
    }

    @Override
    protected Builder self() {
      return this;
    }

    /**
     * Sets the GCS bucket that will be used for temporary files. If this is not set or {@code null}
     * the app's default bucket will be used.
     */
    public Builder setBucketName(String bucketName) {
      this.bucketName = bucketName;
      return this;
    }

    /**
     * The maximum number of files the map stage will write to at the same time. A higher number may
     * increase the speed of the job at the expense of more memory used during the map and sort
     * phases and more intermediate files created.
     *
     * Using the default is recommended.
     */
    public Builder setMapFanout(int mapFanout) {
      Preconditions.checkArgument(mapFanout > 0);
      this.mapFanout = mapFanout;
      return this;
    }

    /**
     * The maximum memory the sort stage should allocate (in bytes). This is used to lower the
     * amount of memory it will use. Regardless of this setting it will not exhaust available
     * memory. Null or unset will use the default (no maximum)
     *
     * Using the default is recommended.
     */
    public Builder setMaxSortMemory(Long maxMemory) {
      Preconditions.checkArgument(maxMemory == null || maxMemory >= 0);
      this.maxSortMemory = maxMemory;
      return this;
    }

    /**
     * The maximum length of time sort should spend reading input before it starts sorting it and
     * writing it out.
     *
     * Using the default is recommended.
     */
    public Builder setSortReadTimeMillis(int sortReadTimeMillis) {
      Preconditions.checkArgument(sortReadTimeMillis >= 0);
      this.sortReadTimeMillis = sortReadTimeMillis;
      return this;
    }

    /**
     * Size (in bytes) of items to batch together in the output of the sort. (A higher value saves
     * storage cost, but needs to be small enough to not impact memory use.)
     *
     * Using the default is recommended.
     */
    public Builder setSortBatchPerEmitBytes(int sortBatchPerEmitBytes) {
      Preconditions.checkArgument(sortBatchPerEmitBytes >= 0);
      this.sortBatchPerEmitBytes = sortBatchPerEmitBytes;
      return this;
    }

    /**
     * Number of files the merge stage will read at the same time. A higher number can increase the
     * speed of the job at the expense of requiring more memory in the merge stage.
     *
     * Using the default is recommended.
     */
    public Builder setMergeFanin(int mergeFanin) {
      this.mergeFanin = mergeFanin;
      return this;
    }

    /**
     * credentials to use when accessing storage for sort/shuffle phases of this MR j
     */
    public Builder setStorageCredentials(Credentials credentials) {
      this.storageCredentials = credentials;
      return this;
    }

    public MapReduceSettings build() {
      return new MapReduceSettings(this);
    }
  }

  private MapReduceSettings(Builder builder) {
    super(builder);
    mapFanout = builder.mapFanout;
    maxSortMemory = builder.maxSortMemory;
    sortReadTimeMillis = builder.sortReadTimeMillis;
    sortBatchPerEmitBytes = builder.sortBatchPerEmitBytes;
    mergeFanin = builder.mergeFanin;
    storageCredentials = builder.storageCredentials;
    bucketName = verifyAndSetBucketName(builder.bucketName);
  }

  private String verifyAndSetBucketName(String bucket) {
    if (Strings.isNullOrEmpty(bucket)) {
      try {
        bucket = AppIdentityServiceFactory.getAppIdentityService().getDefaultGcsBucketName();
        if (Strings.isNullOrEmpty(bucket)) {
          String message = "The BucketName property was not set in the MapReduceSettings object, "
              + "and this application does not have a default bucket configured to fall back on.";
          log.log(Level.SEVERE, message);
          throw new IllegalArgumentException(message);
        }
      } catch (AppIdentityServiceFailureException e) {
        throw new RuntimeException(
            "The BucketName property was not set in the MapReduceSettings object, "
            + "and could not get the default bucket.", e);
      }
    }
    try {
      verifyBucketIsWritable(bucket);
    } catch (Exception e) {
      throw new RuntimeException("Writeable Bucket '" + bucket + "' test failed. See "
          + "http://developers.google.com/appengine/docs/java/googlecloudstorageclient/activate"
          + " for more information on how to setup Google Cloude storage.", e);
    }
    return bucket;
  }

  protected Storage getClient() {
    if (this.storageCredentials == null) {
      return StorageOptions.getDefaultInstance().getService();
    } else {
      return StorageOptions.newBuilder()
        .setCredentials(this.storageCredentials)
        .build().getService();
    }
  }

  private void verifyBucketIsWritable(String bucket) throws IOException {
    BlobId blobId = BlobId.of(bucket, UUID.randomUUID() + ".tmp");
    if (getClient().get(blobId) != null) {
      log.warning("File '" + blobId.getName() + "' exists. Skipping bucket write test.");
      return;
    }
    try {
      getClient().create(BlobInfo.newBuilder(blobId).build(), "Delete me!".getBytes(StandardCharsets.UTF_8));
    } finally {
      getClient().delete(blobId);
    }
  }
}

