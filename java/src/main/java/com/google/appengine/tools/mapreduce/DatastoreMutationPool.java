// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import static java.util.concurrent.Executors.callable;

import com.github.rholder.retry.RetryerBuilder;
import com.google.appengine.api.datastore.CommittedButStillApplyingException;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.DatastoreTimeoutException;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityTranslator;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.apphosting.api.ApiProxy.ApiProxyException;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;

import java.io.Serializable;
import java.util.Collection;
import java.util.ConcurrentModificationException;

/**
 * DatastoreMutationPool allows you to pool datastore operations such that they
 * are applied in batches, requiring fewer datastore API calls.  Mutations are
 * accumulated until they reach a count limit on the number of unflushed
 * mutations, until they reach a size limit on the byte count of unflushed
 * mutations, or until a manual flush is requested.
 *
 * A typical use would be: <pre>   {@code
 *   class Example extends Mapper<...> {
 *     ...
 *     private transient DatastoreMutationPool pool;
 *     ...
 *
 *     public void beginSlice() {
 *       pool = DatastoreMutationPool.create();
 *     }
 *
 *     public void endSlice() {
 *       pool.flush();
 *     }
 *
 *     public void map(... value) {
 *       ...
 *       Entity entity = ...
 *       pool.put(entity);
 *       ...
 *     }
 *   }
 * }</pre>
 *
 */
public class DatastoreMutationPool {

  public static final int DEFAULT_COUNT_LIMIT = 100;
  public static final int DEFAULT_BYTES_LIMIT = 256 * 1024;


  private static final RetryerBuilder RETRYER_BUILDER = RetryerBuilder.newBuilder()
    .retryIfExceptionOfType(ApiProxyException.class)
    .retryIfExceptionOfType(ConcurrentModificationException.class)
    .retryIfExceptionOfType(CommittedButStillApplyingException.class)
    .retryIfExceptionOfType(DatastoreTimeoutException.class);

  public static final Params DEFAULT_PARAMS = new Params.Builder().build();

  private final Params params;
  private final DatastoreService ds;
  private final Collection<Entity> puts = Lists.newArrayList();
  private int putsBytes;
  private final Collection<Key> deletes = Lists.newArrayList();
  private int deletesBytes;

  /**
   * DatastoreMutationPool params.
   */
  public static class Params implements Serializable {

    private static final long serialVersionUID = -4072996713626072011L;

    private final int countLimit;
    private final int bytesLimit;

    private Params(Builder builder) {
      countLimit = builder.countLimit;
      bytesLimit = builder.bytesLimit;
    }

    public int getCountLimit() {
      return countLimit;
    }

    public int getBytesLimit() {
      return bytesLimit;
    }

    /**
     * DatastoreMutationPool Params builder.
     */
    public static class Builder {

      private int bytesLimit = DEFAULT_BYTES_LIMIT;
      private int countLimit = DEFAULT_COUNT_LIMIT;

      public Builder bytesLimit(int bytesLimit) {
        this.bytesLimit = bytesLimit;
        return this;
      }

      public Builder countLimit(int countLimit) {
        this.countLimit = countLimit;
        return this;
      }

      public Params build() {
        return new Params(this);
      }
    }
  }

  private DatastoreMutationPool(DatastoreService ds, Params params) {
    this.ds = ds;
    this.params = params;
  }

  public static DatastoreMutationPool create(DatastoreService ds, Params params) {
    return new DatastoreMutationPool(ds, params);
  }

  public static DatastoreMutationPool create() {
    Params.Builder paramBuilder = new Params.Builder();
    paramBuilder.countLimit(DEFAULT_COUNT_LIMIT);
    paramBuilder.bytesLimit(DEFAULT_BYTES_LIMIT);
    return create(DatastoreServiceFactory.getDatastoreService(), paramBuilder.build());
  }

  /**
   * Adds a mutation to put the given entity to the datastore.
   */
  public void delete(Key key) {
    // This is probably a serious overestimation, but I can't see a good
    // way to find the size in the public API.
    int bytesHere = KeyFactory.keyToString(key).length();

    // Do this before the add so that we guarantee that size is never > sizeLimit
    if (deletesBytes + bytesHere >= params.getBytesLimit()) {
      flushDeletes();
    }

    deletesBytes += bytesHere;
    deletes.add(key);

    if (deletes.size() >= params.getCountLimit()) {
      flushDeletes();
    }
  }

  /**
   * Adds a mutation deleting the entity with the given key.
   */
  public void put(Entity entity) {
    int bytesHere = EntityTranslator.convertToPb(entity).getSerializedSize();

    // Do this before the add so that we guarantee that size is never > sizeLimit
    if (putsBytes + bytesHere >= params.getBytesLimit()) {
      flushPuts();
    }

    putsBytes += bytesHere;
    puts.add(entity);

    if (puts.size() >= params.getCountLimit()) {
      flushPuts();
    }
  }

  /**
   * Performs all pending mutations.
   */
  public void flush() {
    if (!puts.isEmpty()) {
      flushPuts();
    }
    if (!deletes.isEmpty()) {
      flushDeletes();
    }
  }

  @SneakyThrows
  private void flushDeletes() {
    RETRYER_BUILDER.build().call(callable(() -> {
        ds.delete(deletes);
        deletes.clear();
        deletesBytes = 0;
      }));
  }

  @SneakyThrows
  private void flushPuts() {
    RETRYER_BUILDER.build().call(callable(() -> {
        ds.put(puts);
        puts.clear();
        putsBytes = 0;
      }));
  }
}
