package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.CloudStorageIntegrationTestHelper;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.impl.BigQueryMarshallerByType;
import com.google.appengine.tools.mapreduce.testmodels.Child;
import com.google.appengine.tools.mapreduce.testmodels.Father;
import com.google.appengine.tools.pipeline.impl.util.SerializationUtils;
import com.google.common.collect.Lists;

import junit.framework.TestCase;

import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class BigQueryStoreResultTest extends TestCase {

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper();
  CloudStorageIntegrationTestHelper storageIntegrationTestHelper;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    helper.setUp();
    storageIntegrationTestHelper.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    helper.tearDown();
    storageIntegrationTestHelper.tearDown();
  }

  @Test
  public void testSerialization() throws IOException {
    BigQueryGoogleCloudStorageStoreOutput<Father> creator =
        new BigQueryGoogleCloudStorageStoreOutput<Father>(
            new BigQueryMarshallerByType<Father>(Father.class), storageIntegrationTestHelper.getBucket(), "testJob", GoogleCloudStorageFileOutput.BaseOptions.defaults()
        .withCredentials(storageIntegrationTestHelper.getCredentials())
        .withProjectId(storageIntegrationTestHelper.getProjectId()));

    List<MarshallingOutputWriter<Father>> writers = creator.createWriters(5);
    for (MarshallingOutputWriter<Father> writer : writers) {
      writer.beginShard();
      writer.beginSlice();
      writer.write(new Father(true, "Father",
          Lists.newArrayList(new Child("Childone", 1), new Child("childtwo", 2))));
      writer.endSlice();
      writer.endShard();
    }
    BigQueryStoreResult<GoogleCloudStorageFileSet> actual = creator.finish(writers);

    byte[] bytes = SerializationUtils.serialize(actual);
    @SuppressWarnings("unchecked")
    BigQueryStoreResult<GoogleCloudStorageFileSet> copy =
        (BigQueryStoreResult<GoogleCloudStorageFileSet>) SerializationUtils.deserialize(bytes);
     assertEquals(actual.getResult(), copy.getResult());
     assertEquals(actual.getSchema(), copy.getSchema());
  }
}
