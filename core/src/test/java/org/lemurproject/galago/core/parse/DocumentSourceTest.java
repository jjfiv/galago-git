// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.junit.Test;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Processor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author trevor
 */
public class DocumentSourceTest {
  public static final class FakeProcessor implements Processor<DocumentSplit> {

    public ArrayList<DocumentSplit> splits = new ArrayList<DocumentSplit>();

    @Override
    public void process(DocumentSplit split) {
      splits.add(split);
    }

    @Override
    public void close() throws IOException {
    }
  }

  @Test
  public void testUnknownFile() throws Exception {
    Parameters p = new Parameters();
    p.set("filename", "foo.c");
    DocumentSource source = new DocumentSource(new FakeParameters(p));
    FakeProcessor processor = new FakeProcessor();
    source.setProcessor(processor);

    boolean threwException = false;
    try {
      source.run();
    } catch (Exception e) {
      threwException = true;
    }
    assertTrue(threwException);
  }

  @Test
  public void testUnknownExtension() throws Exception {
    File tempFile = FileUtility.createTemporary();
    Parameters p = new Parameters();
    p.set("filename", tempFile.getAbsolutePath());
    DocumentSource source = new DocumentSource(new FakeParameters(p));
    FakeProcessor processor = new FakeProcessor();
    source.setProcessor(processor);

    source.run();
    assertEquals(0, processor.splits.size());
    assertTrue(tempFile.delete());
  }
}
