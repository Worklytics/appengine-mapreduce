package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.MapJob;
import com.google.appengine.tools.mapreduce.MapReduceJob;
import com.google.appengine.tools.mapreduce.MapReduceServlet;
import com.google.appengine.tools.pipeline.DefaultDIModule;
import com.google.appengine.tools.pipeline.impl.servlets.PipelineServlet;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(
  injects = {
    MapJob.class,
    MapReduceJob.class,
    MapReduceJob.MapJob.class,
    MapReduceJob.SortJob.class,
    MapReduceJob.MergeJob.class,
    MapReduceJob.ReduceJob.class,

    MapReduceServlet.class,
    PipelineServlet.class,
  },
  includes = {
    DefaultDIModule.class,
  },
  complete = false,
  library = true
)
public class MapReduceDIModule {

  @Provides @Singleton
  DatastoreOptions datastoreOptions() {
    return DatastoreOptions.getDefaultInstance();
  }

  @Provides @Singleton
  Datastore provideDatastore(DatastoreOptions datastoreOptions) {
    return datastoreOptions.getService();
  }

}
