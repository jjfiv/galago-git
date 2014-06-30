// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.junit.Test;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 *
 * @author trevor
 */
public class TrecWebParserTest {
  @Test
  public void testParseNothing() throws IOException {
    File f = FileUtility.createTemporary();
    f.createNewFile();

    try {
      DocumentSplit split = new DocumentSplit();
      split.fileName = f.getAbsolutePath();
      TrecWebParser parser = new TrecWebParser(split, Parameters.instance());

      Document document = parser.nextDocument();
      assertNull(document);
    } finally {
      f.delete();
    }
  }

  @Test
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
    File f = FileUtility.createTemporary();
    try {

      Utility.copyStringToFile(fileText, f);
      DocumentSplit split = new DocumentSplit();
      split.fileName = f.getAbsolutePath();
      TrecWebParser parser = new TrecWebParser(split, Parameters.instance());

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
