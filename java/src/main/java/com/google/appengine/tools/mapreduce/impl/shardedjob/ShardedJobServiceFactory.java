// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;

/**
 * Provides {@link ShardedJobService} implementations.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class ShardedJobServiceFactory {

  private ShardedJobServiceFactory() {}

  public static ShardedJobService getShardedJobService(Datastore datastore) {
    return new ShardedJobServiceImpl(datastore);
  }

  public static ShardedJobService getShardedJobService() {
    return getShardedJobService(DatastoreOptions.getDefaultInstance().getService());
  }
}
