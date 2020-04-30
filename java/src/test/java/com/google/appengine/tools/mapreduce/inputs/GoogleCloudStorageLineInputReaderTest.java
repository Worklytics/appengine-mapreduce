package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.mapreduce.CloudStorageIntegrationTestHelper;
import com.google.appengine.tools.mapreduce.GcsFilename;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 */
public class GoogleCloudStorageLineInputReaderTest extends GoogleCloudStorageLineInputTestCase {

  private static final String FILENAME = "GoogleCloudStorageLineInputReaderTestFile";
  public static final String RECORD = "01234567890\n";
  public static final int RECORDS_COUNT = 10;

  GcsFilename filename;
  long fileSize;
  GoogleCloudStorageLineInput.BaseOptions inputOptions;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    filename = new GcsFilename(cloudStorageIntegrationTestHelper.getBucket(), FILENAME);
    fileSize = createFile(filename.getObjectName(), RECORD, RECORDS_COUNT);
    inputOptions = GoogleCloudStorageLineInput.BaseOptions.defaults().withCredentials(cloudStorageIntegrationTestHelper.getCredentials());
  }

  public void testSingleSplitPoint() throws Exception {
    List<GoogleCloudStorageLineInputReader> readers = new ArrayList<>();
    readers.add(new GoogleCloudStorageLineInputReader(filename, 0, RECORD.length(), (byte) '\n', inputOptions));
    readers.add(
        new GoogleCloudStorageLineInputReader(filename, RECORD.length(), fileSize, (byte) '\n', inputOptions));
    verifyReaders(readers, false);
  }

  public void testSingleSplitPointsWithSerialization() throws Exception {
    List<GoogleCloudStorageLineInputReader> readers = new ArrayList<>();
    readers.add(new GoogleCloudStorageLineInputReader(filename, 0, RECORD.length(), (byte) '\n', inputOptions));
    readers.add(
        new GoogleCloudStorageLineInputReader(filename, RECORD.length(), fileSize, (byte) '\n', inputOptions));
    verifyReaders(readers, true);
  }

  public void testAllSplitPoints() throws Exception {
    for (int splitPoint = 1; splitPoint < fileSize - 1; splitPoint++) {
      List<GoogleCloudStorageLineInputReader> readers = new ArrayList<>();
      readers.add(new GoogleCloudStorageLineInputReader(filename, 0, splitPoint, (byte) '\n', inputOptions));
      readers.add(
          new GoogleCloudStorageLineInputReader(filename, splitPoint, fileSize, (byte) '\n', inputOptions));
      verifyReaders(readers, false);
    }
  }

  public void testAllSplitPointsWithSerialization() throws Exception {
    for (int splitPoint = 1; splitPoint < fileSize - 1; splitPoint++) {
      List<GoogleCloudStorageLineInputReader> readers = new ArrayList<>();
      readers.add(new GoogleCloudStorageLineInputReader(filename, 0, splitPoint, (byte) '\n', inputOptions));
      readers.add(
          new GoogleCloudStorageLineInputReader(filename, splitPoint, fileSize, (byte) '\n', inputOptions));
      verifyReaders(readers, true);
    }
  }


  private void verifyReaders(
      List<GoogleCloudStorageLineInputReader> readers, boolean performSerialization)
      throws IOException {
    int recordsRead = 0;
    String recordWithoutSeparator = RECORD.substring(0, RECORD.length() - 1);

    for (GoogleCloudStorageLineInputReader reader : readers) {
      if (performSerialization) {
        reader = SerializationUtil.clone(reader);
      }
      reader.beginShard();
      if (performSerialization) {
        reader = SerializationUtil.clone(reader);
      }
      while (true) {
        reader.beginSlice();
        byte[] value;
        try {
          value = reader.next();
        } catch (NoSuchElementException e) {
          break;
        }
        assertEquals("Record mismatch", recordWithoutSeparator, new String(value));
        recordsRead++;

        reader.endSlice();
        if (performSerialization) {
          reader = SerializationUtil.clone(reader);
        }
      }
      reader.endShard();
    }

    assertEquals("Number of records read", RECORDS_COUNT, recordsRead);
  }
}
