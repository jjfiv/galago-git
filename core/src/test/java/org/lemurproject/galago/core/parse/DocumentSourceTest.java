// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import junit.framework.TestCase;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class DocumentSourceTest extends TestCase {

  public DocumentSourceTest(String testName) {
    super(testName);
  }

  public class FakeProcessor implements Processor<DocumentSplit> {

    public ArrayList<DocumentSplit> splits = new ArrayList<DocumentSplit>();

    @Override
    public void process(DocumentSplit split) {
      splits.add(split);
    }

    @Override
    public void close() throws IOException {
    }
  }

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

  public void testUnknownExtension() throws Exception {
    File tempFile = FileUtility.createTemporary();
    Parameters p = new Parameters();
    p.set("filename", tempFile.getAbsolutePath());
    DocumentSource source = new DocumentSource(new FakeParameters(p));
    FakeProcessor processor = new FakeProcessor();
    source.setProcessor(processor);

    source.run();
    assertEquals(0, processor.splits.size());
    tempFile.delete();
  }
}
