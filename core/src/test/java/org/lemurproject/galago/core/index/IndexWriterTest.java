/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.utility.btree.DiskBTreeReader;
import org.lemurproject.galago.core.btree.format.DiskBTreeWriter;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.*;

/**
 *
 * @author trevor
 */
public class IndexWriterTest {

  File temporary;

  @Before
  public void setUp() {
    temporary = null;
  }

  @After
  public void tearDown() {
    if (temporary != null) {
      temporary.delete();
    }
  }

  @Test
  public void testSingleKeyValue() throws IOException {

    Parameters parameters = Parameters.create();
    parameters.set("blockSize", 64);
    temporary = FileUtility.createTemporary();
    DiskBTreeWriter writer = new DiskBTreeWriter(temporary.getAbsolutePath(), parameters);
    writer.add(new GenericElement("key", "value"));
    writer.close();

    assertTrue(DiskBTreeReader.isBTree(temporary));
    DiskBTreeReader reader = new DiskBTreeReader(temporary.getAbsolutePath());

    assertEquals("value", reader.getValueString(ByteUtil.fromString("key")));
    reader.close();
  }

  @Test
  public void testLargeKey() throws IOException {
    Random rnd = new Random();

    StringBuilder builder = new StringBuilder();
    while(builder.length() < 13000){
      builder.append(rnd.nextLong());
    }
    String key = builder.toString();
        
    temporary = FileUtility.createTemporary();
    
    DiskBTreeWriter writer = new DiskBTreeWriter(temporary.getAbsolutePath(), Parameters.create());
    writer.add(new GenericElement(key, "value"));
    writer.close();

    assertTrue(DiskBTreeReader.isBTree(temporary));
    DiskBTreeReader reader = new DiskBTreeReader(temporary.getAbsolutePath());

    assertEquals("value", reader.getValueString(ByteUtil.fromString(key)));
    reader.close();
  }

  @Test
  public void testSeek() throws IOException {
    Parameters parameters = Parameters.create();
    parameters.set("blockSize", 64);
    temporary = FileUtility.createTemporary();
    DiskBTreeWriter writer = new DiskBTreeWriter(temporary.getAbsolutePath(), parameters);
    writer.add(new GenericElement("key", "value"));
    writer.add(new GenericElement("more", "value2"));
    writer.close();

    assertTrue(DiskBTreeReader.isBTree(temporary));
    DiskBTreeReader reader = new DiskBTreeReader(temporary.getAbsolutePath());
    DiskBTreeReader.Iterator iterator = reader.getIterator();

    // Skip to 'more'
    iterator.skipTo(new byte[]{(byte) 'm'});
    assertFalse(iterator.isDone());
    assertEquals("more", ByteUtil.toString(iterator.getKey()));
    assertEquals("value2", iterator.getValueString());
    assertFalse(iterator.nextKey());

    // Start at the beginning
    iterator = reader.getIterator();
    assertFalse(iterator.isDone());
    assertEquals("key", ByteUtil.toString(iterator.getKey()));
    assertTrue(iterator.nextKey());
    assertEquals("more", ByteUtil.toString(iterator.getKey()));
    assertFalse(iterator.nextKey());

    // Start after all keys
    iterator = reader.getIterator();
    assertFalse(iterator.isDone());
    iterator.skipTo(new byte[]{(byte) 'z'});
    assertTrue(iterator.isDone());

    reader.close();
  }

  @Test
  public void testSingleCompressedKeyValue() throws IOException {
    Parameters parameters = Parameters.create();
    parameters.set("blockSize", 64);
    parameters.set("isCompressed", true);
    temporary = FileUtility.createTemporary();
    DiskBTreeWriter writer = new DiskBTreeWriter(temporary.getAbsolutePath(), parameters);
    writer.add(new GenericElement("key", "value"));
    writer.close();

    assertTrue(DiskBTreeReader.isBTree(temporary));
    DiskBTreeReader reader = new DiskBTreeReader(temporary.getAbsolutePath());

    assertEquals("value", reader.getValueString(ByteUtil.fromString("key")));
    reader.close();
  }

  @Test
  public void testSimpleWrite() throws IOException {
    Parameters parameters = Parameters.create();
    parameters.set("blockSize", 64);
    temporary = FileUtility.createTemporary();
    DiskBTreeWriter writer = new DiskBTreeWriter(temporary.getAbsolutePath(), parameters);

    for (int i = 0; i < 1000; ++i) {
      String key = String.format("%05d", i);
      String value = String.format("value%05d", i);
      writer.add(new GenericElement(key, value));
    }
    writer.close();

    assertTrue(DiskBTreeReader.isBTree(temporary));
    DiskBTreeReader reader = new DiskBTreeReader(temporary.getAbsolutePath());

    for (int i = 1000 - 1; i >= 0; i--) {
      String key = String.format("%05d", i);
      String value = String.format("value%05d", i);

      assertEquals(value, reader.getValueString(ByteUtil.fromString(key)));
    }
    reader.close();
  }

  @Test
  public void testSimpleWriteCompressed() throws IOException {
    Parameters parameters = Parameters.create();
    parameters.set("blockSize", 64);
    parameters.set("isCompressed", true);
    temporary = FileUtility.createTemporary();
    DiskBTreeWriter writer = new DiskBTreeWriter(temporary.getAbsolutePath(), parameters);

    for (int i = 0; i < 1000; ++i) {
      String key = String.format("%05d", i);
      String value = String.format("value%05d", i);
      writer.add(new GenericElement(key, value));
    }
    writer.close();

    assertTrue(DiskBTreeReader.isBTree(temporary));
    DiskBTreeReader reader = new DiskBTreeReader(temporary.getAbsolutePath());

    for (int i = 1000 - 1; i >= 0; i--) {
      String key = String.format("%05d", i);
      String value = String.format("value%05d", i);

      assertEquals(value, reader.getValueString(ByteUtil.fromString(key)));
    }
    reader.close();
  }
}
