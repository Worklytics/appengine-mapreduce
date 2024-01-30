// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
import com.google.appengine.api.appidentity.AppIdentityServiceFailureException;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
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
@SuperBuilder(toBuilder = true)
@ToString
public class MapReduceSettings extends MapSettings implements GcpCredentialOptions {

  private static final long serialVersionUID = 610088354289299175L;
  private static final Logger log = Logger.getLogger(MapReduceSettings.class.getName());
  public static final int DEFAULT_MAP_FANOUT = 32;
  public static final int DEFAULT_SORT_BATCH_PER_EMIT_BYTES = 32 * 1024;
  public static final int DEFAULT_SORT_READ_TIME_MILLIS = 180000;
  public static final int DEFAULT_MERGE_FANIN = 32;


  @Getter
  private final String bucketName;


  @Getter
  @Builder.Default
  private final int mapFanout = DEFAULT_MAP_FANOUT;

  @Getter
  private final Long maxSortMemory;

  @Builder.Default
  @Getter
  private final int sortReadTimeMillis = DEFAULT_SORT_READ_TIME_MILLIS;

  @Builder.Default
  @Getter
  private final int sortBatchPerEmitBytes = DEFAULT_SORT_BATCH_PER_EMIT_BYTES;
  @Builder.Default
  @Getter
  private final int mergeFanin = DEFAULT_MERGE_FANIN;

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
  private final String serviceAccountKey;

}

