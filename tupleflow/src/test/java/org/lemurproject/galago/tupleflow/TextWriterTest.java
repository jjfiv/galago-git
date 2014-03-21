package org.lemurproject.galago.tupleflow;

import org.junit.Assert;
import org.junit.Test;
import org.lemurproject.galago.tupleflow.types.FileName;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author trevor
 */
public class TextWriterTest {
  @Test
  public void testWriter() throws Exception {
    File tempPath = null;
    try {
      tempPath = FileUtility.createTemporary();
      Parameters p = new Parameters();
      p.set("class", FileName.class.getName());
      p.set("filename", tempPath.getAbsolutePath());
      TextWriter<FileName> writer = new TextWriter<FileName>(new FakeParameters(p));

      writer.process(new FileName("hey"));
      writer.process(new FileName("you"));
      writer.close();

      BufferedReader reader = new BufferedReader(new FileReader(tempPath.getAbsolutePath()));
      String line;
      line = reader.readLine();
      assertEquals("hey", line);
      line = reader.readLine();
      assertEquals("you", line);
      line = reader.readLine();
      assertEquals(null, line);
    } finally {
      if (tempPath != null) {
        Assert.assertTrue(tempPath.delete());
      }
    }
  }
}

