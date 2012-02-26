// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.AggregateReader.CollectionStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
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

    assertEquals(index.getCollectionStatistics().collectionLength, 1000);
    assertEquals(index.getCollectionStatistics().documentCount, 200);
    assertEquals(index.getCollectionStatistics().vocabCount, 204);

    Node n = StructuredQuery.parse("#counts:sample:part=postings()");
    MovableCountIterator ci = (MovableCountIterator) index.getIterator(n);
    assertEquals(ci.currentCandidate(), 0);
    int total = 0;
    do {
      total += ci.count();
    } while (ci.next());
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
      CollectionStatistics postingsStats = r.getRetrievalStatistics();
      CollectionStatistics stemmedPostingsStats = r.getRetrievalStatistics("stemmedPostings");

      assertEquals(postingsStats.collectionLength, 1000);
      assertEquals(postingsStats.documentCount, 200);
      assertEquals(postingsStats.vocabCount, 204);

      assertEquals(stemmedPostingsStats.collectionLength, 1000);
      assertEquals(stemmedPostingsStats.documentCount, 200);
      assertEquals(stemmedPostingsStats.vocabCount, 204);
    } finally {
      if(output != null)
        Utility.deleteDirectory(output);
    }
  }
}
