// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.File;
import java.io.IOException;
import junit.framework.TestCase;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class TrecWebParserTest extends TestCase {

  public TrecWebParserTest(String testName) {
    super(testName);
  }

  public void testParseNothing() throws IOException {
    File f = Utility.createTemporary();
    f.createNewFile();

    try {
      DocumentSplit split = new DocumentSplit();
      split.fileName = f.getAbsolutePath();
      TrecWebParser parser = new TrecWebParser(split, new Parameters());

      Document document = parser.nextDocument();
      assertNull(document);
    } finally {
      f.delete();
    }
  }

  public void testParseOneDocument() throws IOException {
    String fileText =
            "<DOC>\n"
            + "<DOCNO>CACM-0001</DOCNO>\n"
            + "<DOCHDR>\n"
            + "http://www.yahoo.com:80 some extra text here\n"
            + "even more text in this part\n"
            + "</DOCHDR>\n"
            + "This is some text in a document.\n"
            + "</DOC>\n";
    File f = Utility.createTemporary();
    try {

      Utility.copyStringToFile(fileText, f);
      DocumentSplit split = new DocumentSplit();
      split.fileName = f.getAbsolutePath();
      TrecWebParser parser = new TrecWebParser(split, new Parameters());

      Document document = parser.nextDocument();
      assertNotNull(document);
      assertEquals("CACM-0001", document.name);
      assertEquals("http://www.yahoo.com", document.metadata.get("url"));
      assertEquals("This is some text in a document.\n", document.text);

      document = parser.nextDocument();
      assertNull(document);
    } finally {
      f.delete();
    }
  }
}
