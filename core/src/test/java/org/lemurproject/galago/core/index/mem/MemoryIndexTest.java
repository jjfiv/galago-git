// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.stats.CollectionAggregateIterator;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
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
    
    IndexPartStatistics is2 = index.getIndexPartStatistics("postings.porter");
    assertEquals(is2.collectionLength, 1000);
    assertEquals(is2.vocabCount, 204);
    assertEquals(is2.highestFrequency, 200);
    assertEquals(is2.highestDocumentCount, 200);

    Node n = StructuredQuery.parse("#counts:sample:part=postings()");
    CountIterator ci = (CountIterator) index.getIterator(n);
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

      NodeParameters np = new NodeParameters();
      np.set("part", "postings");
      np.set("default", "sample");
      CountIterator iterator = (CountIterator) index.getIterator(new Node("counts", np));
      assertEquals(iterator.currentCandidate(), 101);

      output = Utility.createTemporaryDirectory();
      (new FlushToDisk()).flushMemoryIndex(index, output.getAbsolutePath(), false);

      Retrieval r = RetrievalFactory.instance(output.getAbsolutePath(), new Parameters());
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

      IndexPartStatistics stemmedPostingsStats = r.getIndexPartStatistics("postings.porter");
      assertEquals(stemmedPostingsStats.collectionLength, 1000);
      assertEquals(stemmedPostingsStats.vocabCount, 204);
      assertEquals(stemmedPostingsStats.highestDocumentCount, 200);
      assertEquals(stemmedPostingsStats.highestFrequency, 200);


    } finally {
      if (output != null) {
        Utility.deleteDirectory(output);
      }
    }
  }
}
