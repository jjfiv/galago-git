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
public class TrecTextParserTest extends TestCase {

  public TrecTextParserTest(String testName) {
    super(testName);
  }

  public void testParseNothing() throws IOException {
    File f = Utility.createTemporary();
    f.createNewFile();
    try {
      DocumentSplit split = new DocumentSplit();
      split.fileName = f.getAbsolutePath();
      TrecTextParser parser = new TrecTextParser(split, new Parameters());

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
            + "<TEXT>\n"
            + "This is some text in a document.\n"
            + "</TEXT>\n"
            + "</DOC>\n";
    File f = Utility.createTemporary();
    try {
      Utility.copyStringToFile(fileText, f);
      DocumentSplit split = new DocumentSplit();
      split.fileName = f.getAbsolutePath();
      TrecTextParser parser = new TrecTextParser(split, new Parameters());

      Document document = parser.nextDocument();
      assertNotNull(document);
      assertEquals("CACM-0001", document.name);
      assertEquals("<TEXT>\nThis is some text in a document.\n</TEXT>\n", document.text);

      document = parser.nextDocument();
      assertNull(document);
    } finally {
      f.delete();
    }
  }

  public void testParseTwoDocuments() throws IOException {
    String fileText =
            "<DOC>\n"
            + "<DOCNO>CACM-0001</DOCNO>\n"
            + "<TEXT>\n"
            + "This is some text in a document.\n"
            + "</TEXT>\n"
            + "</DOC>\n"
            + "<DOC>\n"
            + "<DOCNO>CACM-0002</DOCNO>\n"
            + "<TEXT>\n"
            + "This is some text in a document.\n"
            + "</TEXT>\n"
            + "</DOC>\n";
    File f = Utility.createTemporary();
    try {
      Utility.copyStringToFile(fileText, f);
      DocumentSplit split = new DocumentSplit();
      split.fileName = f.getAbsolutePath();
      TrecTextParser parser = new TrecTextParser(split, new Parameters());

      Document document = parser.nextDocument();
      assertNotNull(document);
      assertEquals("CACM-0001", document.name);
      assertEquals("<TEXT>\nThis is some text in a document.\n</TEXT>\n", document.text);

      document = parser.nextDocument();
      assertNotNull(document);
      assertEquals("CACM-0002", document.name);
      assertEquals("<TEXT>\nThis is some text in a document.\n</TEXT>\n", document.text);

      document = parser.nextDocument();
      assertNull(document);
    } finally {
      f.delete();
    }
  }
}
