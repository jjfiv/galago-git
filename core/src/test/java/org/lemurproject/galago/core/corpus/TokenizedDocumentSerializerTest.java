package org.lemurproject.galago.core.corpus;

import org.junit.Test;
import org.lemurproject.galago.core.index.corpus.CorpusReader;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.TagTokenizer;
import org.lemurproject.galago.core.tools.apps.BuildIndex;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class TokenizedDocumentSerializerTest {
  @Test
  public void testSerializeDocument() throws IOException {
    TagTokenizer tagTokenizer = new TagTokenizer();
    DocumentSerializer tds = new TokenizedDocumentSerializer(Parameters.create());

    Document doc = new Document();
    doc.metadata.put("meta-key", "value");
    doc.metadata.put("null-meta-key", null);
    doc.name = "doc-name";
    doc.text = "doc text goes here\nand <tag>continues. This is weird</tag>";
    tagTokenizer.tokenize(doc);

    byte[] docBytes = tds.toBytes(doc);
    assertNotNull(docBytes);

    Document doc2 = tds.fromBytes(docBytes, Document.DocumentComponents.All);
    assertEquals(doc.name, doc2.name);
    assertEquals(doc.text, doc2.text);
    assertNotNull(doc2.metadata);
    assertEquals(doc.metadata.get("meta-key"), doc2.metadata.get("meta-key"));
    assertTrue(doc.metadata.containsKey("null-meta-key"));
    assertNull(doc.metadata.get("null-meta-key"));
    assertEquals(doc.terms, doc2.terms);
    assertNull(doc2.tags);

    Document doc3 = tds.fromBytes(docBytes, new Document.DocumentComponents(false, true, true));
    assertEquals(doc.name, doc3.name);
    assertNull(doc3.text);
    assertNotNull(doc3.metadata);
    assertNotNull(doc3.metadata.get("meta-key"));
    assertEquals("value", doc3.metadata.get("meta-key"));
    assertNull(doc3.metadata.get("null-meta-key"));
    assertEquals(doc.terms, doc2.terms);
    assertEquals(null, doc2.tags);
  }

  @Test
  public void serializerClass() throws Exception {
    File tmpDir = FileUtility.createTemporaryDirectory();
    try {
      File inputTxt = new File(tmpDir, "input.txt");
      File testIndex = new File(tmpDir, "test.galago");
      Utility.copyStringToFile("this is a document of some kind", inputTxt);
      BuildIndex.execute(
          Parameters.parseArray(
              "inputPath", inputTxt,
              "indexPath", testIndex,
              "corpusParameters", Parameters.parseArray(
                  "documentSerializerClass", TokenizedDocumentSerializer.class.getName())),
          System.out);

      CorpusReader reader = new CorpusReader(new File(testIndex, "corpus").getAbsolutePath());
      assertEquals(TokenizedDocumentSerializer.class.getName(), reader.getManifest().getString("documentSerializerClass"));
      System.out.println(reader.serializer.getClass());
      Document document = reader.getIterator().getDocument(Document.DocumentComponents.JustTerms);
      assertNotNull(document);
      assertNull(document.text);
      assertNotNull(document.terms);
      assertEquals(7, document.terms.size());
      assertEquals("this", document.terms.get(0));
    } finally {
      FSUtil.deleteDirectory(tmpDir);
    }
  }
}