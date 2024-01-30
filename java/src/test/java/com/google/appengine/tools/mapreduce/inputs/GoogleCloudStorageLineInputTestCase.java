package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

import com.google.appengine.tools.mapreduce.CloudStorageIntegrationTestHelper;
import com.google.appengine.tools.mapreduce.testutil.DatastoreExtension;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import junit.framework.TestCase;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 */
@ExtendWith(DatastoreExtension.class)
abstract class GoogleCloudStorageLineInputTestCase extends TestCase  {

  CloudStorageIntegrationTestHelper cloudStorageIntegrationTestHelper;

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper();

  @Override
  public void setUp() throws Exception {
    super.setUp();
    helper.setUp();
    cloudStorageIntegrationTestHelper = new CloudStorageIntegrationTestHelper();
    cloudStorageIntegrationTestHelper.setUp();
  }

  long createFile(String filename, String record, int recordsCount) throws IOException {
    Storage storage = cloudStorageIntegrationTestHelper.getStorage();
    try (WriteChannel writeChannel = storage.writer(BlobInfo.newBuilder(cloudStorageIntegrationTestHelper.getBucket(), filename).setContentType("application/bin").build())) {
      for (int i = 0; i < recordsCount; i++) {
        writeChannel.write(ByteBuffer.wrap(record.getBytes()));
      }
    }
    return cloudStorageIntegrationTestHelper.getStorage().get(BlobId.of(cloudStorageIntegrationTestHelper.getBucket(), filename)).getSize();
  }

  @Override
  public void tearDown() throws Exception {
    helper.tearDown();
    cloudStorageIntegrationTestHelper.tearDown();
    super.tearDown();
  }
}
