package org.lemurproject.galago.tupleflow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import junit.framework.TestCase;
import org.lemurproject.galago.tupleflow.types.FileName;

/**
 *
 * @author trevor
 */
public class TextWriterTest extends TestCase {

  public TextWriterTest(String testName) {
    super(testName);
  }

  public void testWriter() throws Exception {
    File tempPath = null;
    try {
      tempPath = Utility.createTemporary();
      Parameters p = new Parameters();
      p.set("class", FileName.class.getName());
      p.set("filename", tempPath.getAbsolutePath());
      TextWriter writer = new TextWriter(new FakeParameters(p));

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
        tempPath.delete();
      }
    }
  }
}
