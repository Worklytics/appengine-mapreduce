package com.google.appengine.tools.mapreduce.testutil;

import com.google.appengine.api.utils.SystemProperty;
import com.google.appengine.tools.mapreduce.impl.MapReduceDIModule;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(
  includes = {
      MapReduceDIModule.class,
  },
  complete = false,
  library = true,
  overrides = true
)
public class EndToEndTestDIModule {

  public static DatastoreOptions datastoreOptions;
  public static AppEngineBackEnd appEngineBackEnd;

  @Provides @Singleton
  DatastoreOptions datastoreOptions() {
    return datastoreOptions;
  }

  @Provides
  @Singleton
  AppEngineBackEnd appEngineBackEndOptions() {
    return appEngineBackEnd;
  }

}
