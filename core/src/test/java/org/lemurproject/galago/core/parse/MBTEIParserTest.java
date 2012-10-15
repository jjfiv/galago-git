// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.*;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.MBTEIBookParser;
import org.lemurproject.galago.core.parse.MBTEIPageParser;
import org.lemurproject.galago.core.types.DocumentSplit;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;
import junit.framework.TestCase;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Inspired by the TrecTextParserTest.
 *
 * @author irmarc
 */
public class MBTEIParserTest extends TestCase {

  public MBTEIParserTest(String testName) {
    super(testName);
  }

  public void testBookParseNothing() throws IOException {
    File f = Utility.createTemporary();
    try {
      Utility.copyStringToFile("test book", f);
      DocumentSplit split = new DocumentSplit();
      split.fileName = f.getAbsolutePath();
      MBTEIBookParser parser = new MBTEIBookParser(split, new Parameters());

      Document document = parser.nextDocument();
      assertNull(document);
    } finally {
      f.delete();
    }
  }

  public void testPageParseNothing() throws IOException {
    File f = Utility.createTemporary();
    try {
      Utility.copyStringToFile("test book", f);
      DocumentSplit split = new DocumentSplit();
      split.fileName = f.getAbsolutePath();
      MBTEIPageParser parser = new MBTEIPageParser(split, new Parameters());

      Document document = parser.nextDocument();
      assertNull(document);
    } finally {
      f.delete();
    }
  }

  public void testBookParse() throws IOException {
    File f = Utility.createTemporary();
    try {
      Utility.copyStringToFile(teiDocument, f);
      DocumentSplit split = new DocumentSplit();
      split.fileName = f.getAbsolutePath();

      MBTEIBookParser parser = new MBTEIBookParser(split, new Parameters());

      Document document = parser.nextDocument();

// sjh: parser actually extracts only <text> into the text field.      
//      String expected = "<TEI><metadata>"
//              + "<identifier>testDocument</identifier>"
//              + "<collection>galago</collection>"
//              + "<title>test1</title>"
//              + "<date>1916</date>"
//              + "<language>eng</language>"
//              + "</metadata>"
//              + "<text>The blue emerald , that ugly one </text></TEI>";
      String expected = "The blue emerald , that ugly one";

      // assertEquals("testdoc", document.name);
      assertEquals(expected, document.text);
      assertTrue(document.metadata.containsKey("title"));
      assertEquals("test1", document.metadata.get("title"));
      assertTrue(document.metadata.containsKey("date"));
      assertEquals("1916", document.metadata.get("date"));
      assertTrue(document.metadata.containsKey("language"));
      assertEquals("eng", document.metadata.get("language"));
      document = parser.nextDocument();
      assertNull(document);
    } finally {
      f.delete();
    }
  }

  public void testIncompleteBookParse() throws IOException {
    File f = Utility.createTemporary();
    try {
      Utility.copyStringToFile(badTEIDocument, f);
      DocumentSplit split = new DocumentSplit();
      split.fileName = f.getAbsolutePath();
      MBTEIBookParser parser = new MBTEIBookParser(split, new Parameters());

      Document document = parser.nextDocument();
      assertNull(document);
    } finally {
      f.delete();

    }
  }

  public void testEntityParse() throws IOException {
    File f = Utility.createTemporary();
    try {
      Utility.copyStringToFile(entityDocument, f);
      DocumentSplit split = new DocumentSplit();
      split.fileName = f.getAbsolutePath();

      MBTEIEntityParser parser = new MBTEIEntityParser(split, new Parameters());

      Document document = parser.nextDocument();
      // assertEquals("blue emerald", document.name);
      String expected = "<text>The blue emerald , that ugly one </text>";
      // ??

    } finally {
      f.delete();

    }
  }

  /*
   public void testPageParse() throws IOException {
   File f = Utility.createTemporary();
   try {
   Utility.copyStringToFile(teiDocument, f);
   DocumentSplit split = new DocumentSplit();
   split.fileName = f.getAbsolutePath();

   MBTEIPageParser parser = new MBTEIPageParser(split, new Parameters());

   Document document = parser.nextDocument();
   String expected = "<TEI><metadata>"
   + "<identifier>testDocument</identifier>"
   + "<collection>galago</collection>"
   + "<title>test1</title>"
   + "<date>1916</date>"
   + "<language>eng</language>"
   + "</metadata>"
   + "<text>The blue emerald</text></TEI>";
   //assertEquals("testdoc_2", document.name);
   assertEquals(expected, document.text);

   document = parser.nextDocument();
   expected = "<TEI><metadata>"
   + "<identifier>testDocument</identifier>"
   + "<collection>galago</collection>"
   + "<title>test1</title>"
   + "<date>1916</date>"
   + "<language>eng</language>"
   + "</metadata>"
   + "<text>, that ugly one</text></TEI>";
   // assertEquals("testdoc_3", document.name);
   assertEquals(expected, document.text);
   assertTrue(document.metadata.containsKey("title"));
   assertEquals("test1", document.metadata.get("title"));
   assertTrue(document.metadata.containsKey("date"));
   assertEquals("1916", document.metadata.get("date"));
   assertTrue(document.metadata.containsKey("language"));
   assertEquals("eng", document.metadata.get("language"));
   document = parser.nextDocument();
   assertNull(document);
   } finally {
   f.delete();

   }
   }
  public void testIncompletePageParse() throws IOException {
    File f = Utility.createTemporary();
    try {
      Utility.copyStringToFile(badTEIDocument, f);
      DocumentSplit split = new DocumentSplit();
      split.fileName = f.getAbsolutePath();

      MBTEIPageParser parser = new MBTEIPageParser(split, new Parameters());

      Document document = parser.nextDocument();
      String expected = "<TEI><metadata>"
              + "<identifier>testDocument</identifier>"
              + "<collection>galago</collection>"
              + "<title>test1</title>"
              + "<date>1916</date>"
              + "<language>eng</language>"
              + "</metadata>"
              + "<text>The blue emerald</text></TEI>";
      // assertEquals("testdoc_2", document.name);
      assertEquals(expected, document.text);
      assertTrue(document.metadata.containsKey("title"));
      assertEquals("test1", document.metadata.get("title"));
      assertTrue(document.metadata.containsKey("date"));
      assertEquals("1916", document.metadata.get("date"));
      assertTrue(document.metadata.containsKey("language"));
      assertEquals("eng", document.metadata.get("language"));

      document = parser.nextDocument();
      expected = "<TEI><metadata>"
              + "<identifier>testDocument</identifier>"
              + "<collection>galago</collection>"
              + "<title>test1</title>"
              + "<date>1916</date>"
              + "<language>eng</language>"
              + "</metadata>"
              + "<text>, that ugly one</text></TEI>";
      // assertEquals("testdoc_3", document.name);
      assertEquals(expected, document.text);
      assertTrue(document.metadata.containsKey("title"));
      assertEquals("test1", document.metadata.get("title"));
      assertTrue(document.metadata.containsKey("date"));
      assertEquals("1916", document.metadata.get("date"));
      assertTrue(document.metadata.containsKey("language"));
      assertEquals("eng", document.metadata.get("language"));
      document = parser.nextDocument();
      assertNull(document);
    } finally {
      f.delete();
    }
  }

  public void testEntityLinking() throws IOException {
    DocumentSplit split = new DocumentSplit();
    split.fileName = "testdoc";
    InputStream stream = new ByteArrayInputStream(entityDocument.getBytes());
    MBTEIPageEntityLinker linker = new MBTEIPageEntityLinker(split, new Parameters());

    ArrayList<Document> documents = new ArrayList<Document>();
    // These things will show up in pairs
    Document d1, d2;
    Tag t1, t2;
    // 2 for the book <-> blue emerald
    // 2 for the page <-> blue emerald
    for (int i = 0; i < 4; ++i) {
      d1 = linker.nextDocument();
      assertNotNull(d1);
      documents.add(d1);
    }

    // Concrete stuff
    assertEquals("PAGE-LINK", documents.get(0).name);
    assertEquals("OBJECT-LINK", documents.get(1).name);
    assertEquals("COLLECTION-LINK", documents.get(2).name);
    assertEquals("OBJECT-LINK", documents.get(3).name);

    // pairwise matching
    // Variable name pattern: (t|d)(1|2) 
    // - the (t|d) indicates tag or document form,
    // - the (1|2) indicates the order it first appeared from the linker
    for (int i = 0; i < 2; i += 2) {
      d1 = documents.get(i + 0);
      assertEquals(1, d1.tags.size());
      d2 = documents.get(i + 1);
      assertEquals(1, d2.tags.size());

      t1 = d2.tags.get(0);
      t2 = d1.tags.get(0);
      assertEquals(d1.name, t1.name);
      assertEquals(d2.name, t2.name);
      assertEquals(d1.metadata.get("id"),
              t1.attributes.get("id"));
      assertEquals(d2.metadata.get("id"),
              t2.attributes.get("id"));
      assertEquals(d1.metadata.get("type"),
              t1.attributes.get("type"));
      assertEquals(d2.metadata.get("type"),
              t2.attributes.get("type"));
      assertEquals(d1.metadata.get("pos"),
              t1.attributes.get("pos"));
      assertEquals(d2.metadata.get("pos"),
              t2.attributes.get("pos"));
    }
  }
  */
  public static String teiDocument =
          "<?xml version=\"1.0\" encoding=\"UTF-8\"?><TEI><metadata><identifier>testDocument</identifier><collection>galago</collection><title>test1</title><date>1916</date><language>eng</language></metadata><text lang=\"eng\"><pb n=\"1\" /><cb /><pb n=\"2\" /><w form=\"The\" deprel=\"ROOT\">The</w><w form=\"blue\">blue</w><w form=\"emerald\">emerald</w><pb n=\"3\"><w form=\",\">,</w><w form=\"that\" deprel=\"ROOT\">that</w><w form=\"ugly\">ugly</w><w form=\"one\">one</w></pb><pb n=\"4\"></pb></text></TEI>";
  public static String entityDocument =
          "<?xml version=\"1.0\" encoding=\"UTF-8\"?><TEI><metadata><identifier>testDocument</identifier><collection>galago</collection><title>test1</title><date>1916</date><language>eng</language></metadata><text lang=\"eng\"><pb n=\"1\" /><cb /><pb n=\"2\" /><w form=\"The\" deprel=\"ROOT\">The</w><name type=\"object\" name=\"blue emerald\"><w form=\"blue\">blue</w><w form=\"emerald\">emerald</w></name><pb n=\"3\"><w form=\",\">,</w><w form=\"that\" deprel=\"ROOT\">that</w><w form=\"ugly\">ugly</w><w form=\"one\">one</w></pb><pb n=\"4\"></pb></text></TEI>";
  public static String badTEIDocument =
          "<?xml version=\"1.0\" encoding=\"UTF-8\"?><TEI><metadata><identifier>testDocument</identifier><collection>galago</collection><title>test1</title><date>1916</date><language>eng</language></metadata><text lang=\"eng\"><pb n=\"1\" /><cb /><pb n=\"2\" /><w form=\"The\" deprel=\"ROOT\">The</w><w form=\"blue\">blue</w><w form=\"emerald\">emerald</w><pb n=\"3\"><w form=\",\">,</w><w form=\"that\" deprel=\"ROOT\">that</w><w form=\"ugly\">ugly</w><w form=\"one\">one</w></pb><pb n=\"4\"></pb><pb n=\"4\"><w coords=\"0234\" form=\"ha";
}
