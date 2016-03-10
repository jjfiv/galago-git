// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import org.junit.Test;
import org.lemurproject.galago.core.index.stats.CollectionAggregateIterator;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Tag;
import org.lemurproject.galago.core.parse.TagTokenizer;
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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * @author sjh, MichaelZ
 */
public class MemoryIndexTest {
  @Test
  public void testProcessDocuments() throws Exception {
    Parameters p = Parameters.create();
    p.put("makecorpus", true);
    p.set("tokenizer", Parameters.create());
    String[] fields = {"thing", "loc"};
    p.getMap("tokenizer").set("fields", fields);
    p.getMap("tokenizer").set("class", TagTokenizer.class.getCanonicalName());

    MemoryIndex indexWithFields = new MemoryIndex(new FakeParameters(p));

    for (int i = 0; i < 200; i++) {
      Document d = new Document();
      d.name = "DOC-" + i;
      d.text = "this is sample <thing>document " + i + "</thing>";
      d.terms = Arrays.asList(d.text.split(" "));
      d.tags = new ArrayList<Tag>();
      d.metadata = new HashMap<String, String>();
      indexWithFields.process(d);
    }

    Document d = new Document();
    d.name = "DOC-201" ;
    d.text = "<loc>Amherst</loc> is located in <loc>Massachusetts</loc>";
    d.terms = Arrays.asList(d.text.split(" "));
    d.tags = new ArrayList<Tag>();
    d.metadata = new HashMap<String,String>();
    indexWithFields.process(d);

    CollectionAggregateIterator lengthsIterator = (CollectionAggregateIterator) indexWithFields.getLengthsIterator();
    FieldStatistics collStats = lengthsIterator.getStatistics();
    assertEquals(collStats.collectionLength, 1005);
    assertEquals(collStats.documentCount, 201);
    assertEquals(collStats.fieldName, "document");
    assertEquals(collStats.maxLength, 5);
    assertEquals(collStats.minLength, 5);

    IndexPartStatistics is1 = indexWithFields.getIndexPartStatistics("postings");
    assertEquals(is1.collectionLength, 1005);
    assertEquals(is1.vocabCount, 208);
    assertEquals(is1.highestFrequency, 201);
    assertEquals(is1.highestDocumentCount, 201);

    IndexPartStatistics is2 = indexWithFields.getIndexPartStatistics("postings.krovetz");
    assertEquals(is2.collectionLength, 1005);
    assertEquals(is2.vocabCount, 208);
    assertEquals(is2.highestFrequency, 201);
    assertEquals(is2.highestDocumentCount, 201);

    IndexPartStatistics is3 = indexWithFields.getIndexPartStatistics("field.thing");
    assertEquals(is3.collectionLength, 400);
    assertEquals(is3.vocabCount, 201);
    assertEquals(is3.highestFrequency, 200);
    assertEquals(is3.highestDocumentCount, 200);

    IndexPartStatistics is4 = indexWithFields.getIndexPartStatistics("field.loc");
    assertEquals(is4.collectionLength, 2);
    assertEquals(is4.vocabCount, 2);
    assertEquals(is4.highestFrequency, 1);
    assertEquals(is4.highestDocumentCount, 1);

    Node n = StructuredQuery.parse("#counts:sample:part=postings()");
    CountIterator ci = (CountIterator) indexWithFields.getIterator(n);
    ScoringContext sc = new ScoringContext();
    assertEquals(ci.currentCandidate(), 0);
    int total = 0;
    while (!ci.isDone()) {
      sc.document = ci.currentCandidate();
      total += ci.count(sc);
      ci.movePast(ci.currentCandidate());
    }
    assertEquals(total, 200);

    n = StructuredQuery.parse("#counts:is:part=postings()");
    ci = (CountIterator) indexWithFields.getIterator(n);
    sc = new ScoringContext();
    assertEquals(ci.currentCandidate(), 0);
    total = 0;
    while (!ci.isDone()) {
      sc.document = ci.currentCandidate();
      total += ci.count(sc);
      ci.movePast(ci.currentCandidate());
    }
    assertEquals(total, 201);

    n = StructuredQuery.parse("#counts:document:part=field.thing()");
    ci = (CountIterator) indexWithFields.getIterator(n);
    sc = new ScoringContext();
    assertEquals(ci.currentCandidate(), 0);
    total = 0;
    while (!ci.isDone()) {
      sc.document = ci.currentCandidate();
      total += ci.count(sc);
      ci.movePast(ci.currentCandidate());
    }
    assertEquals(total, 200);

    File output = FileUtility.createTemporaryDirectory();
    FlushToDisk.flushMemoryIndex(indexWithFields, output.getAbsolutePath(), false);

    // check that the field index part is used
    Retrieval retrieval = RetrievalFactory.instance(output.getAbsolutePath(), Parameters.create());
    Node tree = StructuredQuery.parse("#combine( #inside( #text:amherst() #field:loc() ) )");
    tree = retrieval.transformQuery(tree, Parameters.create());
    assertEquals("#combine:w=1.0( #dirichlet:collectionLength=1005:maximumCount=1:nodeFrequency=1:w=1.0( #lengths:document:part=lengths() #counts:amherst:part=field.krovetz.loc() ) )", tree.toString());
    List<ScoredDocument> results = retrieval.executeQuery(tree, Parameters.create()).scoredDocuments;
    assertEquals(results.size(), 1);
    assertEquals(results.get(0).documentName, "DOC-201");
    retrieval.close();

    // now remove the "loc" field parts, so retrieval should now use extents.
    File f = new File(output.getAbsolutePath() + File.separator + "field.loc");
    f.delete();
    f = new File(output.getAbsolutePath() + File.separator + "field.krovetz.loc");
    f.delete();

    retrieval = RetrievalFactory.instance(output.getAbsolutePath(), Parameters.create());
    tree = StructuredQuery.parse("#combine( #inside( #text:amherst() #field:loc() ) )");
    tree = retrieval.transformQuery(tree, Parameters.create());
    assertEquals("#combine:w=1.0( #dirichlet:collectionLength=1005:maximumCount=1:nodeFrequency=1:w=1.0( #lengths:document:part=lengths() #inside( #extents:amherst:part=postings.krovetz() #extents:loc:part=extents() ) ) )", tree.toString());
    results = retrieval.executeQuery(tree, Parameters.create()).scoredDocuments;
    assertEquals(results.size(), 1);
    assertEquals(results.get(0).documentName, "DOC-201");
    retrieval.close();

    // clean up...
    FSUtil.deleteDirectory(output);

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
