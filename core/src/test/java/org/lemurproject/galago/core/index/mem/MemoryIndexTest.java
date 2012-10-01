// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.AggregateReader.CollectionAggregateIterator;
import org.lemurproject.galago.core.index.AggregateReader.CollectionStatistics;
import org.lemurproject.galago.core.index.AggregateReader.IndexPartStatistics;
import org.lemurproject.galago.core.index.LengthsReader.LengthsIterator;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class MemoryIndexTest extends TestCase {

  public MemoryIndexTest(String testName) {
    super(testName);
  }

  public void testProcessDocuments() throws Exception {
    Parameters p = new Parameters();

    MemoryIndex index = new MemoryIndex(new FakeParameters(p));

    for (int i = 0; i < 200; i++) {
      Document d = new Document();
      d.name = "DOC-" + i;
      d.text = "this is sample document " + i;
      d.terms = Arrays.asList(d.text.split(" "));
      d.tags = new ArrayList();
      d.metadata = new HashMap();

      index.process(d);
    }

    CollectionAggregateIterator lengthsIterator = (CollectionAggregateIterator) index.getLengthsIterator();
    CollectionStatistics collStats = lengthsIterator.getStatistics();
    assertEquals(collStats.collectionLength, 1000);
    assertEquals(collStats.documentCount, 200);
    assertEquals(collStats.fieldName, "document");
    assertEquals(collStats.maxLength, 5);
    assertEquals(collStats.minLength, 5);


    assertEquals(index.getCollectionStatistics().collectionLength, 1000);
    assertEquals(index.getCollectionStatistics().vocabCount, 204);

    Node n = StructuredQuery.parse("#counts:sample:part=postings()");
    MovableCountIterator ci = (MovableCountIterator) index.getIterator(n);
    ci.setContext(new ScoringContext());
    ScoringContext sc = ci.getContext();
    assertEquals(ci.currentCandidate(), 0);
    int total = 0;
    while (!ci.isDone()) {
      sc.document = ci.currentCandidate();
      total += ci.count();
      ci.movePast(ci.currentCandidate());
    }
    assertEquals(total, 200);
  }

  public void testDocumentOffset() throws Exception {
    File output = null;
    try {
      Parameters p = new Parameters();
      p.set("documentNumberOffset", 101);
      MemoryIndex index = new MemoryIndex(new FakeParameters(p));

      for (int i = 0; i < 200; i++) {
        Document d = new Document();
        d.name = "DOC-" + i;
        d.text = "this is sample document " + i;
        d.terms = Arrays.asList(d.text.split(" "));
        d.tags = new ArrayList();
        d.metadata = new HashMap();

        index.process(d);
      }

      assertEquals(index.getLength(300), 5);
      assertEquals(index.getName(300), "DOC-199");
      assertTrue(index.getCollectionLength() == 1000);
      assertTrue(index.getDocumentCount() == 200);

      NodeParameters np = new NodeParameters();
      np.set("part", "postings");
      np.set("default", "sample");
      MovableCountIterator iterator = (MovableCountIterator) index.getIterator(new Node("counts", np));
      assertEquals(iterator.currentCandidate(), 101);

      output = Utility.createTemporaryDirectory();
      (new FlushToDisk()).flushMemoryIndex(index, output.getAbsolutePath(), false);

      Retrieval r = RetrievalFactory.instance(output.getAbsolutePath(), new Parameters());
      CollectionStatistics collStats = r.getCollectionStatistics("#lengths:part=lengths()");
      assertEquals(collStats.collectionLength, 1000);
      assertEquals(collStats.documentCount, 200);
      assertEquals(collStats.fieldName, "document");
      assertEquals(collStats.maxLength, 5);
      assertEquals(collStats.minLength, 5);

      IndexPartStatistics postingsStats = r.getIndexPartStatistics("postings");
      assertEquals(postingsStats.collectionLength, 1000);
      assertEquals(postingsStats.vocabCount, 204);
      assertEquals(postingsStats.highestDocumentCount, 0);
      assertEquals(postingsStats.highestFrequency, 0);

      IndexPartStatistics stemmedPostingsStats = r.getIndexPartStatistics("stemmedPostings");
      assertEquals(stemmedPostingsStats.collectionLength, 1000);
      assertEquals(stemmedPostingsStats.vocabCount, 204);
      assertEquals(stemmedPostingsStats.highestDocumentCount, 0);
      assertEquals(stemmedPostingsStats.highestFrequency, 0);


    } finally {
      if (output != null) {
        Utility.deleteDirectory(output);
      }
    }
  }
}
