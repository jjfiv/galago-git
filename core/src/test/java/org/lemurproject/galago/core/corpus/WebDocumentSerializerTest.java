package org.lemurproject.galago.core.corpus;

import org.junit.Test;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.tupleflow.Parameters;

import java.io.IOException;

import static org.junit.Assert.*;

public class WebDocumentSerializerTest {

  @Test
  public void testSerializeDocument() throws IOException {
    WebDocumentSerializer wds = new WebDocumentSerializer(new Parameters());

    Document doc = new Document();
    doc.metadata.put("meta-key", "value");
    doc.name = "doc-name";
    doc.text = "doc text goes here\nand <tag>continues. This is weird</tag>";
    wds.tokenizer.tokenize(doc);

    byte[] docBytes = wds.toBytes(doc);
    assertNotNull(docBytes);

    Document doc2 = wds.fromBytes(docBytes, new Document.DocumentComponents(true, true, true));
    assertEquals(doc.name, doc2.name);
    assertEquals(doc.text, doc2.text);
    assertNotNull(doc2.metadata);
    assertEquals(doc.metadata.get("meta-key"), doc2.metadata.get("meta-key"));
    assertEquals(doc.terms, doc2.terms);
    assertEquals(doc.tags, doc2.tags);
  }

  @Test
  public void testDefaultSerializer() throws IOException {
    DocumentSerializer ds = DocumentSerializer.instance(new Parameters());
    assertTrue(ds instanceof WebDocumentSerializer);
  }

}