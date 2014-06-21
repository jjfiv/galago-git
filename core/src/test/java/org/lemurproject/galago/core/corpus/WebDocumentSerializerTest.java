package org.lemurproject.galago.core.corpus;

import org.junit.Test;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.tupleflow.*;

import java.io.*;

import static org.junit.Assert.*;

public class WebDocumentSerializerTest {

  @Test
  public void testSerializeDocument() throws IOException {
    WebDocumentSerializer wds = new WebDocumentSerializer(new Parameters());

    Document doc = new Document();
    doc.getMetadata().put("meta-key", "value");
    doc.getMetadata().put("null-meta-key", null);
    doc.name = "doc-name";
    doc.text = "doc text goes here\nand <tag>continues. This is weird</tag>";
    wds.tokenizer.tokenize(doc);

    byte[] docBytes = wds.toBytes(doc);
    assertNotNull(docBytes);

    Document doc2 = wds.fromBytes(docBytes, new Document.DocumentComponents(true, true, true));
    assertEquals(doc.name, doc2.name);
    assertEquals(doc.getText(), doc2.getText());
    assertNotNull(doc2.getMetadata());
    assertEquals(doc.getMetadata().get("meta-key"), doc2.getMetadata().get("meta-key"));
    assertTrue(doc.getMetadata().containsKey("null-meta-key"));
    assertNull(doc.getMetadata().get("null-meta-key"));
    assertEquals(doc.getTerms(), doc2.getTerms());
    assertEquals(doc.getTags(), doc2.getTags());
  }

  @Test
  public void testReadFullyVsRead() throws IOException {
    final byte[] fakeData = {1, 2, 3, 4, 5, 6, -1};
    File tmp = null;
    try {
      tmp = FileUtility.createTemporary();
      // save to file
      ByteArrayInputStream bais = new ByteArrayInputStream(fakeData);
      Utility.copyStreamToFile(bais, tmp);

      RandomAccessFile raf = StreamCreator.readFile(tmp.getCanonicalPath());
      BufferedFileDataStream bfds = new BufferedFileDataStream(raf, 0, raf.length());

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      while(true) {
        int x = bfds.read();
        if(x == -1) break;
        baos.write(x);
      }

      byte[] data =  baos.toByteArray();
      assertArrayEquals(fakeData, data);

      byte[] data2 = new byte[fakeData.length];
      bfds = new BufferedFileDataStream(raf, 0, raf.length());
      int amt = bfds.read(data2, 0, (int) raf.length());
      assertEquals(fakeData.length, amt);
      assertArrayEquals(fakeData, data2);

      bfds = new BufferedFileDataStream(raf, 1, 5);
      byte[] data3 = new byte[4];
      amt = bfds.read(data3, 0, 4);
      assertEquals(4, amt);
      assertArrayEquals(new byte[]{2,3,4,5}, data3);

    } finally {
      if(tmp != null) assertTrue(tmp.delete());
    }
  }

  @Test
  public void testDefaultSerializer() throws IOException {
    DocumentSerializer ds = DocumentSerializer.instance(new Parameters());
    assertTrue(ds instanceof WebDocumentSerializer);
  }

}