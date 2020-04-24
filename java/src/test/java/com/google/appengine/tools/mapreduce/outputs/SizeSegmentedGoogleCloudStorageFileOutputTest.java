package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.CloudStorageIntegrationTestHelper;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.BigQueryConstants;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;

import com.google.cloud.storage.Blob;
import junit.framework.TestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SizeSegmentedGoogleCloudStorageFileOutputTest extends TestCase {

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper();

  CloudStorageIntegrationTestHelper cloudStorageIntegrationTestHelper = new CloudStorageIntegrationTestHelper();

  private static final String BUCKET = "test-bigquery-loader";
  private static final String MIME_TYPE = "application/json";

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    helper.setUp();
    cloudStorageIntegrationTestHelper.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    helper.tearDown();
    cloudStorageIntegrationTestHelper.tearDown();
  }

  public void testFilesWritten() throws IOException {
    int segmentSizeLimit = 10;
    String fileNamePattern = String.format(BigQueryConstants.GCS_FILE_NAME_FORMAT, "testJob");
    SizeSegmentedGoogleCloudStorageFileOutput segmenter =
        new SizeSegmentedGoogleCloudStorageFileOutput(BUCKET, segmentSizeLimit, fileNamePattern,
            BigQueryConstants.MIME_TYPE);
    List<? extends OutputWriter<ByteBuffer>> writers = segmenter.createWriters(5);
    List<OutputWriter<ByteBuffer>> finished = new ArrayList<>();
    assertEquals(5, writers.size());
    for (OutputWriter<ByteBuffer> w : writers) {
      w.beginShard();
      w.beginSlice();
      w.write(ByteBuffer.wrap(new byte[9]));
      w.endSlice();
      w = SerializationUtil.clone(w);
      w.beginSlice();
      w.write(ByteBuffer.wrap(new byte[9]));
      w.endSlice();
      w = SerializationUtil.clone(w);
      w.beginSlice();
      w.write(ByteBuffer.wrap(new byte[9]));
      w.endSlice();
      w.endShard();
      finished.add(w);
    }
    GoogleCloudStorageFileSet filesWritten = segmenter.finish(finished);
    assertEquals(15, filesWritten.getNumFiles());
    for (int i = 0; i < filesWritten.getNumFiles(); i++) {
      Blob blob = cloudStorageIntegrationTestHelper.getStorage().get(filesWritten.getFile(i).asBlobId());
      assertNotNull(blob);
      assertEquals(MIME_TYPE, blob.getContentType());
    }
  }

  public void testSegmentation() throws IOException {
    int segmentSizeLimit = 10;
    SizeSegmentedGoogleCloudStorageFileOutput segmenter =
        new SizeSegmentedGoogleCloudStorageFileOutput(BUCKET, segmentSizeLimit, "testJob",
            BigQueryConstants.MIME_TYPE);
    List<? extends OutputWriter<ByteBuffer>> writers = segmenter.createWriters(5);
    int countFiles = 0;
    for (OutputWriter<ByteBuffer> w : writers) {
      writeMultipleValues(w, 3, 9);
      countFiles += 3;
    }
    GoogleCloudStorageFileSet filesWritten = segmenter.finish(writers);
    assertEquals(countFiles, filesWritten.getNumFiles());
    for (int i = 0; i < filesWritten.getNumFiles(); i++) {
      Blob blob = cloudStorageIntegrationTestHelper.getStorage().get(filesWritten.getFile(i).asBlobId());
      assertNotNull(blob);
      assertEquals(MIME_TYPE, blob.getContentType());
    }
  }

  /**
   * @param writer
   * @throws IOException
   */
  private void writeMultipleValues(OutputWriter<ByteBuffer> writer, int count, int size)
      throws IOException {
    writer.beginShard();
    writer.beginSlice();
    Random r = new Random(0);
    for (int i = 0; i < count; i++) {
      byte[] data = new byte[size];
      r.nextBytes(data);
      writer.write(ByteBuffer.wrap(data));
    }
    writer.endSlice();
    writer.endShard();
  }
}
