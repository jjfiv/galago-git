/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index;

import org.lemurproject.galago.core.index.GenericElement;
import org.lemurproject.galago.core.index.disk.IndexWriter;
import org.lemurproject.galago.core.index.disk.IndexReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import junit.framework.TestCase;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class IndexWriterTest extends TestCase {

  File temporary;

  public IndexWriterTest(String testName) {
    super(testName);
  }

  @Override
  public void setUp() {
    temporary = null;
  }

  @Override
  public void tearDown() {
    if (temporary != null) {
      temporary.delete();
    }
  }

  public void testSingleKeyValue() throws IOException {
  
    Parameters parameters = new Parameters();
    parameters.set("blockSize", 64);
    temporary = Utility.createTemporary();
    IndexWriter writer = new IndexWriter(temporary.getAbsolutePath(), parameters);
    writer.add(new GenericElement("key", "value"));
    writer.close();

    assertTrue(IndexReader.isIndexFile(temporary.getAbsolutePath()));
    IndexReader reader = new IndexReader(temporary.getAbsolutePath());

    assertEquals("value", reader.getValueString(Utility.fromString("key")));
    reader.close();
  }

  public void testSeek() throws IOException {
    Parameters parameters = new Parameters();
    parameters.set("blockSize", 64);
    temporary = Utility.createTemporary();
    IndexWriter writer = new IndexWriter(temporary.getAbsolutePath(), parameters);
    writer.add(new GenericElement("key", "value"));
    writer.add(new GenericElement("more", "value2"));
    writer.close();

    assertTrue(IndexReader.isIndexFile(temporary.getAbsolutePath()));
    IndexReader reader = new IndexReader(temporary.getAbsolutePath());
    IndexReader.Iterator iterator = reader.getIterator();

    // Skip to 'more'
    iterator.skipTo(new byte[]{(byte) 'm'});
    assertFalse(iterator.isDone());
    assertEquals("more", Utility.toString(iterator.getKey()));
    assertEquals("value2", iterator.getValueString());
    assertFalse(iterator.nextKey());

    // Start at the beginning
    iterator = reader.getIterator();
    assertFalse(iterator.isDone());
    assertEquals("key", Utility.toString(iterator.getKey()));
    assertTrue(iterator.nextKey());
    assertEquals("more", Utility.toString(iterator.getKey()));
    assertFalse(iterator.nextKey());

    // Start after all keys
    iterator = reader.getIterator();
    assertFalse(iterator.isDone());
    iterator.skipTo(new byte[]{(byte) 'z'});
    assertTrue(iterator.isDone());

    reader.close();
  }

  public void testSingleCompressedKeyValue() throws IOException {
    Parameters parameters = new Parameters();
    parameters.set("blockSize", 64);
    parameters.set("isCompressed", true);
    temporary = Utility.createTemporary();
    IndexWriter writer = new IndexWriter(temporary.getAbsolutePath(), parameters);
    writer.add(new GenericElement("key", "value"));
    writer.close();

    assertTrue(IndexReader.isIndexFile(temporary.getAbsolutePath()));
    IndexReader reader = new IndexReader(temporary.getAbsolutePath());

    assertEquals("value", reader.getValueString(Utility.fromString("key")));
    reader.close();
  }

  public void testSimpleWrite() throws FileNotFoundException, IOException {
    Parameters parameters = new Parameters();
    parameters.set("blockSize", 64);
    temporary = Utility.createTemporary();
    IndexWriter writer = new IndexWriter(temporary.getAbsolutePath(), parameters);

    for (int i = 0; i < 1000; ++i) {
      String key = String.format("%05d", i);
      String value = String.format("value%05d", i);
      writer.add(new GenericElement(key, value));
    }
    writer.close();

    assertTrue(IndexReader.isIndexFile(temporary.getAbsolutePath()));
    IndexReader reader = new IndexReader(temporary.getAbsolutePath());

    for (int i = 1000 - 1; i >= 0; i--) {
      String key = String.format("%05d", i);
      String value = String.format("value%05d", i);

      assertEquals(value, reader.getValueString(Utility.fromString(key)));
    }
    reader.close();
  }

  public void testSimpleWriteCompressed() throws FileNotFoundException, IOException {
    Parameters parameters = new Parameters();
    parameters.set("blockSize", 64);
    parameters.set("isCompressed", true);
    temporary = Utility.createTemporary();
    IndexWriter writer = new IndexWriter(temporary.getAbsolutePath(), parameters);

    for (int i = 0; i < 1000; ++i) {
      String key = String.format("%05d", i);
      String value = String.format("value%05d", i);
      writer.add(new GenericElement(key, value));
    }
    writer.close();

    assertTrue(IndexReader.isIndexFile(temporary.getAbsolutePath()));
    IndexReader reader = new IndexReader(temporary.getAbsolutePath());

    for (int i = 1000 - 1; i >= 0; i--) {
      String key = String.format("%05d", i);
      String value = String.format("value%05d", i);

      assertEquals(value, reader.getValueString(Utility.fromString(key)));
    }
    reader.close();
  }
}
