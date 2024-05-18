package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineTaskQueue;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import org.junit.jupiter.api.extension.*;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.List;

@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith({
  DatastoreExtension.class,
  DatastoreExtension.ParameterResolver.class,
  //AppEngineEnvironmentExtension.class,
  PipelineComponentsExtension.class,
  PipelineComponentsExtension.ParameterResolver.class,
})
public @interface PipelineSetupExtensions {

}

class PipelineComponentsExtension implements BeforeEachCallback {

  Datastore datastore;

  DatastoreOptions datastoreOptions;

  protected PipelineService pipelineService;
  protected PipelineManager pipelineManager;
  protected AppEngineBackEnd appEngineBackend;

  enum ContextStoreKey {
    PIPELINE_SERVICE,
    PIPELINE_MANAGER,
    APP_ENGINE_BACKEND;
  }

  static final List<Class<?>> PARAMETER_CLASSES = Arrays.asList(
    PipelineManager.class,
    PipelineService.class,
    AppEngineBackEnd.class,
    DatastoreOptions.class
  );


  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    datastore = (Datastore) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(DatastoreExtension.DS_CONTEXT_KEY);

    // can be serialized, then used to re-constitute connection to datastore emulator on another thread/process
    datastoreOptions = (DatastoreOptions) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(DatastoreExtension.DS_OPTIONS_CONTEXT_KEY);

    extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
      .put(ContextStoreKey.PIPELINE_SERVICE.name(), pipelineService);
    extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
      .put(ContextStoreKey.PIPELINE_MANAGER.name(), pipelineManager);
    extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
      .put(ContextStoreKey.APP_ENGINE_BACKEND.name(), appEngineBackend);
  }

  public static class ParameterResolver implements org.junit.jupiter.api.extension.ParameterResolver {

    @Override
    public boolean supportsParameter(ParameterContext parameterContext,
                                     ExtensionContext extensionContext) throws ParameterResolutionException {
      return PARAMETER_CLASSES.contains(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext,
                                   ExtensionContext extensionContext) throws ParameterResolutionException {
      if (parameterContext.getParameter().getType() == PipelineManager.class) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
          .get(ContextStoreKey.PIPELINE_MANAGER.name());
      } else if (parameterContext.getParameter().getType() == PipelineService.class) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
          .get(ContextStoreKey.PIPELINE_SERVICE.name());
      } else if (parameterContext.getParameter().getType() == AppEngineBackEnd.class) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
          .get(ContextStoreKey.APP_ENGINE_BACKEND.name());
      } else if (parameterContext.getParameter().getType() == DatastoreOptions.class) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
          .get(DatastoreExtension.DS_OPTIONS_CONTEXT_KEY);
      }
      throw new Error("Shouldn't be reached");
    }
  }

}
