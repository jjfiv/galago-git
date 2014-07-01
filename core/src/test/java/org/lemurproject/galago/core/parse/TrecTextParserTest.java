// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.junit.Test;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.util.DocumentSplitFactory;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 *
 * @author trevor
 */
public class TrecTextParserTest {

  @Test
  public void testParseNothing() throws IOException {
    File f = FileUtility.createTemporary();
    f.createNewFile();
    try {
      DocumentSplit split = DocumentSplitFactory.file(f, "trectext");
      TrecTextParser parser = new TrecTextParser(split, Parameters.instance());

      Document document = parser.nextDocument();
      assertNull(document);
    } finally {
      f.delete();
    }
  }

  @Test
	public void testAppTestGenDoc() throws IOException {
		String fileText = AppTest.trecDocument("CACM-0001", "This is some text in a document.\n");
		
		File f = FileUtility.createTemporary();
    try {
      Utility.copyStringToFile(fileText, f);
      DocumentSplit split = DocumentSplitFactory.file(f, "trectext");
      TrecTextParser parser = new TrecTextParser(split, Parameters.instance());

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

  @Test
  public void testDocumentStreamParser() throws IOException {
    String fileText = AppTest.trecDocument("CACM-0001", "This is some text in a document.\n");

    File f = FileUtility.createTemporary();
    try {
      Utility.copyStringToFile(fileText, f);
      DocumentSplit split = DocumentSplitFactory.file(f, "trectext");
      DocumentStreamParser parser = DocumentStreamParser.instance(split, Parameters.instance());

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

  @Test
  public void testParseOneDocument() throws IOException {
    String fileText =
            "<DOC>\n"
            + "<DOCNO>CACM-0001</DOCNO>\n"
            + "<TEXT>\n"
            + "This is some text in a document.\n"
            + "</TEXT>\n"
            + "</DOC>\n";
    File f = FileUtility.createTemporary();
    try {
      Utility.copyStringToFile(fileText, f);
      DocumentSplit split = DocumentSplitFactory.file(f);
      TrecTextParser parser = new TrecTextParser(split, Parameters.instance());

      Document document = parser.nextDocument();
      assertNotNull(document);
      assertEquals("CACM-0001", document.name);
      assertEquals("<TEXT>\nThis is some text in a document.\n</TEXT>\n", document.text);

      document = parser.nextDocument();
      assertNull(document);
    } finally {
      assertTrue(f.delete());
    }
  }

  @Test
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
    File f = FileUtility.createTemporary();
    try {
      Utility.copyStringToFile(fileText, f);
      DocumentSplit split = DocumentSplitFactory.file(f);
      TrecTextParser parser = new TrecTextParser(split, Parameters.instance());

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
      assertTrue(f.delete());
    }
  }
}

