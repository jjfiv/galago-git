/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class ThreadingTest extends TestCase {

  public ThreadingTest(String name) {
    super(name);
  }

  public void testLocalRetrievalThreading() throws Exception {
    final int vocab = 1000;
    final int docCount = 1000;
    final int docLen = 100;
    final int qCount = 100;
    final int qLen = 3;

    File index = null;

    try {
      index = makeIndex(docCount, docLen, vocab);
      final Parameters retParams = new Parameters();
      retParams.set("index", index.getAbsolutePath());


      final LocalRetrieval ret = (LocalRetrieval) RetrievalFactory.instance(retParams);
      final List<Exception> exceptions = Collections.synchronizedList(new ArrayList());

      // query generator:
      final Random r = new Random();
      final List<String> queries = new ArrayList();
      for (int qid = 0; qid < qCount; qid++) {
        String query = "#combine(";
        for (int tid = 0; tid < qLen; tid++) {
          query += " " + r.nextInt(vocab);
        }
        query += " )";
        queries.add(query);
      }

      // test that the test works in a single thread:
      final Map<Integer, Integer> trueCounts = new HashMap();
      for (int qid = 0; qid < qCount; qid++) {
        Node qnode = StructuredQuery.parse(queries.get(qid));
        qnode = ret.transformQuery(qnode, retParams);
        List<ScoredDocument> res = ret.executeQuery(qnode).scoredDocuments;
        trueCounts.put(qid, res.size());
      }

      // start 10 threads.
      List<Thread> runningThreads = new ArrayList();
      for (int threadId = 0; threadId < 10; threadId++) {
        Thread t = new Thread() {

          @Override
          public void run() {
            try {
              for (int qid = 0; qid < qCount; qid++) {
                int i = r.nextInt(qCount);
                Node qnode = StructuredQuery.parse(queries.get(i));
                qnode = ret.transformQuery(qnode, retParams);
                List<ScoredDocument> res = ret.executeQuery(qnode).scoredDocuments;
                assert(res.size() == trueCounts.get(i));
              }
            } catch (Exception e) {
              exceptions.add(e);
            }
          }
        };
        runningThreads.add(t);

        t.start();
      }

      for (Thread t : runningThreads) {
        t.join();
      }

      for (Exception e : exceptions) {
        System.err.println(e.getMessage());
        e.printStackTrace();
      }

      if (!exceptions.isEmpty()) {
        throw new RuntimeException("FAILED THREADING TEST.");
      }

    } finally {
      if (index != null) {
        Utility.deleteDirectory(index);
      }
    }
  }

  // index construction
  private File makeIndex(int docCount, int docLen, int vocab) throws Exception {
    File trecFile = Utility.createTemporary();
    File indexFolder = Utility.createTemporaryDirectory();

    Random r = new Random();
    BufferedWriter writer = new BufferedWriter(new FileWriter(trecFile));
    for (int doc = 0; doc < docCount; doc++) {
      StringBuilder sb = new StringBuilder();
      sb.append("document ").append(doc);
      for (int termCount = 2; termCount < docLen; termCount++) {
        sb.append(" ").append(r.nextInt(vocab));
      }
      writer.write(AppTest.trecDocument("doc-" + doc, sb.toString()));
    }
    writer.close();

    Parameters p = new Parameters();
    p.set("inputPath", trecFile.getAbsolutePath());
    p.set("indexPath", indexFolder.getAbsolutePath());
    p.set("stemmedPostings", false); // we just have numbers - no need to stem.
    App.run("build", p, System.err);

    // now remove the data file
    trecFile.delete();
    return indexFolder;
  }
}
