/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.tools;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.disk.CountIndexWriter;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class ManifestEditorTest extends TestCase {

  public ManifestEditorTest(String testName) {
    super(testName);
  }

  public void testManifestEditor() throws Exception {
    File indexFile = null;
    try {
      indexFile = Utility.createTemporary();
      Parameters p = new Parameters();
      p.set("filename", indexFile.getAbsolutePath());
      p.set("key-1", "init-value-1234");
      p.set("key-2", "init-value-2345");

      CountIndexWriter writer = new CountIndexWriter(new FakeParameters(p));
      writer.processExtentName(Utility.fromString("the"));
      writer.processNumber(0);
      writer.processBegin(0);
      writer.processTuple(10);
      writer.processTuple(11);
      writer.processBegin(12);
      writer.processTuple(20);
      writer.processBegin(1);
      writer.processTuple(10);
      writer.processBegin(11);
      writer.processTuple(20);
      writer.close();

      Parameters dump = new Parameters();
      dump.set("filename", indexFile.getAbsolutePath());

      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      PrintStream catcher = new PrintStream(stream);
      App.run("dump-index-manifest", dump, catcher);
      Parameters dumpped = Parameters.parse(stream.toByteArray());
      assertEquals(dumpped.getString("key-1"), "init-value-1234");
      assertEquals(dumpped.getString("key-2"), "init-value-2345");

      dump.set("key-1", "mod-value-1234");
      dump.set("key-2", "mod-value-2345");
      dump.set("key-3", "new-value-3456");

      App.run("overwrite-manifest", dump, System.err);

      stream = new ByteArrayOutputStream();
      catcher = new PrintStream(stream);
      App.run("dump-index-manifest", dump, catcher);
      dumpped = Parameters.parse(stream.toByteArray());
      assertEquals(dumpped.getString("key-1"), "mod-value-1234");
      assertEquals(dumpped.getString("key-2"), "mod-value-2345");
      assertEquals(dumpped.getString("key-3"), "new-value-3456");

    } finally {
      if (indexFile != null) {
        indexFile.delete();
      }
    }
  }
}
