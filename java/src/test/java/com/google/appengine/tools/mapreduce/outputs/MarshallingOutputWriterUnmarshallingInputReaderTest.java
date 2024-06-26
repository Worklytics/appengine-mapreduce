package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.inputs.InMemoryInput;
import com.google.appengine.tools.mapreduce.inputs.UnmarshallingInput;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Validates that data written by the marshallingOutputWriter can be read by the
 * unmarshallingInputWriter
 *
 */
public class MarshallingOutputWriterUnmarshallingInputReaderTest {

  @Test
  public void testReaderWriter() throws IOException {
    Marshaller<String> stringMarshaller = Marshallers.getStringMarshaller();
    MarshallingOutput<String, List<List<ByteBuffer>>> output =
        new MarshallingOutput<>(new InMemoryOutput<ByteBuffer>(), stringMarshaller);
    Collection<MarshallingOutputWriter<String>> writers = output.createWriters(1);
    assertEquals(1, writers.size());
    MarshallingOutputWriter<String> writer = writers.iterator().next();
    writer.beginShard();
    writer.beginSlice();
    writer.write("Foo");
    writer.write("Bar");
    writer.endSlice();
    writer.beginSlice();
    writer.write("Baz");
    writer.endSlice();
    writer.endShard();
    List<List<ByteBuffer>> data = output.finish(writers);
    UnmarshallingInput<String> input =
        new UnmarshallingInput<>(new InMemoryInput<>(data), stringMarshaller);
    List<InputReader<String>> readers = input.createReaders();
    assertEquals(1, readers.size());
    InputReader<String> reader = readers.get(0);
    reader.beginShard();
    reader.beginSlice();
    assertEquals(0.0, reader.getProgress());
    assertEquals("Foo", reader.next());
    assertEquals("Bar", reader.next());
    assertEquals("Baz", reader.next());
    assertEquals(1.0, reader.getProgress());
    try {
      reader.next();
    } catch (NoSuchElementException e) {
      // expected
    }
    reader.endSlice();
    reader.endShard();
  }

  @Test
  public void testInputOutput() throws IOException {
    int numShards = 10;
    Marshaller<String> stringMarshaller = Marshallers.getStringMarshaller();
    MarshallingOutput<String, List<List<ByteBuffer>>> output =
        new MarshallingOutput<>(new InMemoryOutput<ByteBuffer>(), stringMarshaller);
    Collection<MarshallingOutputWriter<String>> writers = output.createWriters(numShards);
    assertEquals(numShards, writers.size());
    List<List<ByteBuffer>> data = output.finish(writers);
    UnmarshallingInput<String> input =
        new UnmarshallingInput<>(new InMemoryInput<>(data), stringMarshaller);
    List<InputReader<String>> readers = input.createReaders();
    assertEquals(numShards, readers.size());
  }
}
