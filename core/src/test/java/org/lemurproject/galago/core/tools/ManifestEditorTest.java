/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.tools;

import org.junit.Test;
import org.lemurproject.galago.core.index.disk.CountIndexWriter;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author sjh
 */
public class ManifestEditorTest {

  @Test
  public void testManifestEditor() throws Exception {
    File indexFile = null;
    try {
      indexFile = FileUtility.createTemporary();
      Parameters p = new Parameters();
      p.set("filename", indexFile.getAbsolutePath());
      p.set("key-1", "init-value-1234");
      p.set("key-2", "init-value-2345");

      CountIndexWriter writer = new CountIndexWriter(new FakeParameters(p));
      writer.processWord(Utility.fromString("the"));
      writer.processDocument(0);
      writer.processTuple(1);
      writer.processTuple(1);
      writer.processDocument(1);
      writer.processTuple(20);
      writer.processTuple(10);
      writer.processTuple(20);
      writer.close();

      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      PrintStream catcher = new PrintStream(stream);
      App.run(new String[]{"dump-index-manifest", indexFile.getAbsolutePath()} , catcher);
      Parameters dumpped = Parameters.parseBytes(stream.toByteArray());
      assertEquals(dumpped.getString("key-1"), "init-value-1234");
      assertEquals(dumpped.getString("key-2"), "init-value-2345");

      Parameters dump = new Parameters();
      dump.set("indexPath", indexFile.getAbsolutePath());
      dump.set("key-1", "mod-value-1234");
      dump.set("key-2", "mod-value-2345");
      dump.set("key-3", "new-value-3456");

      App.run("overwrite-manifest", dump, System.err);

      stream = new ByteArrayOutputStream();
      catcher = new PrintStream(stream);
      App.run(new String[]{"dump-index-manifest", indexFile.getAbsolutePath()} , catcher);
      dumpped = Parameters.parseBytes(stream.toByteArray());
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
