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
import org.lemurproject.galago.core.parse.Tag;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

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
        d.text = "text-" + i;
        d.metadata = new HashMap();
        d.metadata.put("meta", "data-" + i);
        d.terms = new ArrayList();
        d.terms.add("term-" + i);
        d.tags = new ArrayList();
        d.tags.add(new Tag("tag", new HashMap(), i, i));
        d.tags.get(0).attributes.put("attr", "value-" + i);
        docs.add(d);
      }

      corpus = Utility.createTemporary();

      // test defaulty behaviour:
      Parameters p = new Parameters();
      p.set("filename", corpus.getAbsolutePath());
      CorpusFileWriter writer = new CorpusFileWriter(new FakeParameters(p));
      for (Document d : docs) {
        writer.process(d);
      }
      writer.close();

      CorpusReader reader = new CorpusReader(corpus.getAbsolutePath());
      Document testDoc = reader.getDocument(11);
      Document trueDoc = docs.get(11);
      assert (testDoc.identifier == trueDoc.identifier);
      assert (testDoc.name.equals(trueDoc.name));
      assert (testDoc.text.equals(trueDoc.text));
      assert (testDoc.metadata.get("meta").equals(trueDoc.metadata.get("meta")));
      assert (testDoc.terms.get(0).equals(trueDoc.terms.get(0)));
      assert (testDoc.tags.get(0).name.equals(trueDoc.tags.get(0).name));
      assert (testDoc.tags.get(0).attributes.get("attr").equals(trueDoc.tags.get(0).attributes.get("attr")));


      // test <text> only
      p = new Parameters();
      p.set("filename", corpus.getAbsolutePath());
      p.set("corpusTerms", false);
      p.set("corpusTags", false);
      p.set("corpusMetadata", false);
      writer = new CorpusFileWriter(new FakeParameters(p));
      for (Document d : docs) {
        writer.process(d);
      }
      writer.close();

      reader = new CorpusReader(corpus.getAbsolutePath());
      testDoc = reader.getDocument(11);
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
