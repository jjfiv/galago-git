/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.index.corpus;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import junit.framework.TestCase;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.parse.Tag;
import org.lemurproject.galago.core.parse.TagTokenizer;
import org.lemurproject.galago.core.tokenize.Tokenizer;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class CorpusTest extends TestCase {

  public CorpusTest(String name) {
    super(name);
  }

  public void testCorpus() throws Exception {
    File corpus = null;
    try {
      List<Document> docs = new ArrayList();
      for (int i = 0; i < 100; i++) {
        Document d = new Document();
        d.identifier = i;
        d.name = "name-" + i;
        d.text = "<tag attr=\"value-"+i+"\">text"+i+"</tag>";
        d.metadata = new HashMap();
        d.metadata.put("meta", "data-" + i);
        d.terms = new ArrayList();
        d.terms.add("text" + i);
        d.tags = new ArrayList();
        d.tags.add(new Tag("tag", new HashMap(), 0, 1));
        d.tags.get(0).attributes.put("attr", "value-" + i);
        docs.add(d);
      }

      // TODO: add some massive document ids here.
      
      corpus = FileUtility.createTemporary();

      // test defaulty behaviour:
      Parameters p = new Parameters();
      p.set("filename", corpus.getAbsolutePath());
      p.set("tokenizer", new Parameters());
      p.getMap("tokenizer").set("fields", new ArrayList());
      p.getMap("tokenizer").getList("fields").add("tag");
      CorpusFileWriter writer = new CorpusFileWriter(new FakeParameters(p));
      for (Document d : docs) {
        writer.process(d);
      }
      writer.close();

      CorpusReader reader = new CorpusReader(corpus.getAbsolutePath());
			
      // assert that our parameters got put into the manifest appropriately.
      assertTrue(reader.getManifest().isMap("tokenizer"));
      assertTrue(reader.getManifest().getMap("tokenizer").isList("fields"));
      assertEquals((String) reader.getManifest().getMap("tokenizer").getList("fields").get(0), "tag");
			
      Document testDoc = reader.getDocument(11, new DocumentComponents(true, true, true));
      Document trueDoc = docs.get(11);
      assertEquals (testDoc.identifier,trueDoc.identifier);
      assertEquals (testDoc.name, trueDoc.name);
      assertEquals (testDoc.text, trueDoc.text);
      assertEquals (testDoc.metadata.get("meta"), trueDoc.metadata.get("meta"));
      assertEquals (testDoc.terms.size(), trueDoc.terms.size());
      assertEquals (testDoc.terms.get(0), trueDoc.terms.get(0));
      assertFalse (testDoc.tags.isEmpty());
      assertFalse (trueDoc.tags.isEmpty());
      assertEquals (testDoc.tags.get(0).name, trueDoc.tags.get(0).name);
      assertEquals (testDoc.tags.get(0).attributes.get("attr"), trueDoc.tags.get(0).attributes.get("attr"));


      // test <text> only
      p = new Parameters();
      p.set("filename", corpus.getAbsolutePath());
      writer = new CorpusFileWriter(new FakeParameters(p));
      for (Document d : docs) {
        writer.process(d);
      }
      writer.close();

      reader = new CorpusReader(corpus.getAbsolutePath());
      testDoc = reader.getDocument(11, new DocumentComponents(true, false, false));
      trueDoc = docs.get(11);
      assert (testDoc.identifier == trueDoc.identifier);
      assert (testDoc.name.equals(trueDoc.name));
      assert (testDoc.text.equals(trueDoc.text));      
      assert (testDoc.metadata.isEmpty());
      assert (testDoc.terms == null);
      assert (testDoc.tags == null);

    } finally {
      if (corpus != null) {
        corpus.delete();
      }
    }
  }
}
