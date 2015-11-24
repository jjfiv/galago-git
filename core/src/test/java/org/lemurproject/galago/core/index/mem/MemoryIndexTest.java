// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import org.junit.Test;
import org.lemurproject.galago.core.index.stats.CollectionAggregateIterator;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Tag;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 *
 * @author sjh
 */
public class MemoryIndexTest {
  @Test
  public void testProcessDocuments() throws Exception {
    Parameters p = Parameters.create();

    MemoryIndex index = new MemoryIndex(new FakeParameters(p));

    for (int i = 0; i < 200; i++) {
      Document d = new Document();
      d.name = "DOC-" + i;
      d.text = "this is sample document " + i;
      d.terms = Arrays.asList(d.text.split(" "));
      d.tags = new ArrayList<Tag>();
      d.metadata = new HashMap<String,String>();

      index.process(d);
    }

    CollectionAggregateIterator lengthsIterator = (CollectionAggregateIterator) index.getLengthsIterator();
    FieldStatistics collStats = lengthsIterator.getStatistics();
    assertEquals(collStats.collectionLength, 1000);
    assertEquals(collStats.documentCount, 200);
    assertEquals(collStats.fieldName, "document");
    assertEquals(collStats.maxLength, 5);
    assertEquals(collStats.minLength, 5);

    IndexPartStatistics is1 = index.getIndexPartStatistics("postings");
    assertEquals(is1.collectionLength, 1000);
    assertEquals(is1.vocabCount, 204);
    assertEquals(is1.highestFrequency, 200);
    assertEquals(is1.highestDocumentCount, 200);

    IndexPartStatistics is2 = index.getIndexPartStatistics("postings.krovetz");
    assertEquals(is2.collectionLength, 1000);
    assertEquals(is2.vocabCount, 204);
    assertEquals(is2.highestFrequency, 200);
    assertEquals(is2.highestDocumentCount, 200);

    Node n = StructuredQuery.parse("#counts:sample:part=postings()");
    CountIterator ci = (CountIterator) index.getIterator(n);
    ScoringContext sc = new ScoringContext();
    assertEquals(ci.currentCandidate(), 0);
    int total = 0;
    while (!ci.isDone()) {
      sc.document = ci.currentCandidate();
      total += ci.count(sc);
      ci.movePast(ci.currentCandidate());
    }
    assertEquals(total, 200);
  }

  @Test
  public void testDocumentOffset() throws Exception {
    File output = null;
    try {
      Parameters p = Parameters.create();
      p.set("documentNumberOffset", 101);
      MemoryIndex index = new MemoryIndex(new FakeParameters(p));

      for (int i = 0; i < 200; i++) {
        Document d = new Document();
        d.name = "DOC-" + i;
        d.text = "this is sample document " + i;
        d.terms = Arrays.asList(d.text.split(" "));
        d.tags = new ArrayList<>();
        d.metadata = new HashMap<>();

        index.process(d);
      }

      assertEquals(index.getLength(300), 5);
      assertEquals(index.getName(300), "DOC-199");

      NodeParameters np = new NodeParameters();
      np.set("part", "postings");
      np.set("default", "sample");
      CountIterator iterator = (CountIterator) index.getIterator(new Node("counts", np));
      assertEquals(iterator.currentCandidate(), 101);

      output = FileUtility.createTemporaryDirectory();
      FlushToDisk.flushMemoryIndex(index, output.getAbsolutePath(), false);

      Retrieval r = RetrievalFactory.instance(output.getAbsolutePath(), Parameters.create());
      FieldStatistics collStats = r.getCollectionStatistics("#lengths:part=lengths()");
      assertEquals(collStats.collectionLength, 1000);
      assertEquals(collStats.documentCount, 200);
      assertEquals(collStats.fieldName, "document");
      assertEquals(collStats.maxLength, 5);
      assertEquals(collStats.minLength, 5);

      IndexPartStatistics postingsStats = r.getIndexPartStatistics("postings");
      assertEquals(postingsStats.collectionLength, 1000);
      assertEquals(postingsStats.vocabCount, 204);
      assertEquals(postingsStats.highestDocumentCount, 200);
      assertEquals(postingsStats.highestFrequency, 200);

      IndexPartStatistics stemmedPostingsStats = r.getIndexPartStatistics("postings.krovetz");
      assertEquals(stemmedPostingsStats.collectionLength, 1000);
      assertEquals(stemmedPostingsStats.vocabCount, 204);
      assertEquals(stemmedPostingsStats.highestDocumentCount, 200);
      assertEquals(stemmedPostingsStats.highestFrequency, 200);


    } finally {
      if (output != null) {
        FSUtil.deleteDirectory(output);
      }
    }
  }

  @Test
  public void testDocRetrievalNoCorpus() throws Exception {
    Parameters p = Parameters.create();

    MemoryIndex index = new MemoryIndex();

    for (Integer i = 0; i < 10; i++) {
      Document d = new Document();
      d.name = "DOC-" + i;
      d.text = "this is sample document" + i;
      d.terms = Arrays.asList(d.text.split(" "));
      d.tags = new ArrayList<Tag>();
      d.metadata = new HashMap<String, String>();
      d.metadata.put("internal-doc-id", i.toString());

      index.process(d);
    }

    // getIdentifier test
    for (Integer i = 0; i < 10; i++) {
      assertEquals(i.longValue(), index.getIdentifier("DOC-" + i));
    }

    // test getDocument() - 2nd param doesn't matter because these should all return NULL

    Document doc = index.getDocument("DOC-0", null);
    // no doc to return if there is no corpus
    assertNull(doc);

    // non-existent doc
    doc = index.getDocument("I-DO-NOT-EXIST", null);
    assertNull(doc);

    // test getDocuments
    List<String> docList = new ArrayList<>();
    docList.add("DOC-0");
    docList.add("DOC-3");
    docList.add("DOC-ZX");

    Map<String, Document> docs = index.getDocuments(docList, null);
    assertEquals(3, docs.size());
    assertNull(docs.get("DOC-0"));
    assertNull(docs.get("DOC-3"));
    assertNull(docs.get("DOC-ZX"));

    // test retrieval

    p.set("requested", 10);

    LocalRetrieval retrieval = new LocalRetrieval(index);

    String query = "document5";
    Node root = StructuredQuery.parse(query);
    root = retrieval.transformQuery(root, p);

    List<ScoredDocument> results = retrieval.executeQuery(root, p).scoredDocuments;
    assertEquals(1, results.size());

    query = "document0";
    root = StructuredQuery.parse(query);
    root = retrieval.transformQuery(root, p);

    results = retrieval.executeQuery(root, p).scoredDocuments;
    assertEquals(1, results.size());

  }

  @Test
  public void testDocRetrievalWithCorpus() throws Exception {
    Parameters p = Parameters.create();
    p.put("makecorpus", true);
    MemoryIndex index = new MemoryIndex(p);

    for (Integer i = 0; i < 10; i++) {
      Document d = new Document();
      d.name = "DOC-" + i;
      d.text = "this is sample document" + i;
      d.terms = Arrays.asList(d.text.split(" "));
      d.tags = new ArrayList<Tag>();
      d.metadata = new HashMap<String, String>();
      d.metadata.put("internal-doc-id", i.toString());

      index.process(d);
    }

    // getIdentifier test
    for (Integer i = 0; i < 10; i++) {
      assertEquals(i.longValue(), index.getIdentifier("DOC-" + i));
    }

    // test getDocument()

    // get the first document
    Document doc = index.getDocument("DOC-0", Document.DocumentComponents.All);
    assertNotNull(doc);

    // get another document
    doc = index.getDocument("DOC-5", Document.DocumentComponents.All);
    //System.out.println(doc.toString());
    assertNotNull(doc);

    // test DocumentComponents parameter. Currently (11/2015) they are
    // ignored so all documents should be the same.
    Document refDoc = index.getDocument("DOC-5", Document.DocumentComponents.All);
    //System.out.println(refDoc.toString());
    assertNotNull(refDoc);
    doc = index.getDocument("DOC-5", Document.DocumentComponents.JustMetadata);
    //System.out.println(doc.toString());
    assertNotNull(doc);
    assertEquals(refDoc, doc);

    doc = index.getDocument("DOC-5", Document.DocumentComponents.JustTerms);
    //System.out.println(doc.toString());
    assertNotNull(doc);
    assertEquals(refDoc, doc);

    doc = index.getDocument("DOC-5", Document.DocumentComponents.JustText);
    //System.out.println(doc.toString());
    assertNotNull(doc);
    assertEquals(refDoc, doc);

    // non-existent doc
    doc = index.getDocument("I-DO-NOT-EXIST", null);
    assertNull(doc);

    // test getDocuments
    List<String> docList = new ArrayList<>();
    docList.add("DOC-0");
    docList.add("DOC-3");
    docList.add("DOC-ZX");

    Map<String, Document> docs = index.getDocuments(docList, null);
    assertEquals(3, docs.size());
    assertNotNull(docs.get("DOC-0"));
    assertNotNull(docs.get("DOC-3"));
    assertNull(docs.get("DOC-ZX"));

    // test retrieval

    p.set("requested", 10);

    LocalRetrieval retrieval = new LocalRetrieval(index);

    String query = "document5";
    Node root = StructuredQuery.parse(query);
    root = retrieval.transformQuery(root, p);

    List<ScoredDocument> results = retrieval.executeQuery(root, p).scoredDocuments;
    assertEquals(1, results.size());

    query = "document0";
    root = StructuredQuery.parse(query);
    root = retrieval.transformQuery(root, p);

    results = retrieval.executeQuery(root, p).scoredDocuments;
    assertEquals(1, results.size());

  }

}
