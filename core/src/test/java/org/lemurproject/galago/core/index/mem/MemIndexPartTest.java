/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.index.mem;

import java.io.File;
import java.util.ArrayList;
import java.util.Random;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskIterator;
import org.lemurproject.galago.core.index.disk.CountIndexReader;
import org.lemurproject.galago.core.index.disk.CountIndexWriter;
import org.lemurproject.galago.core.index.disk.SparseFloatListReader;
import org.lemurproject.galago.core.index.disk.SparseFloatListWriter;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.extents.FakeScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.types.NumberWordCount;
import org.lemurproject.galago.core.window.ReduceNumberWordCount;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Sorter;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class MemIndexPartTest extends TestCase {

  public MemIndexPartTest(String name) {
    super(name);
  }

  public void testCounts() throws Exception {
    // builds three count indexes
    // - the memoryindex test does not test this part.

    Random r = new Random();
    File diskCounts = Utility.createTemporary();
    try {
      MemoryCountIndex memcounts1 = new MemoryCountIndex(new Parameters());
      CountIndexWriter diskcounts = new CountIndexWriter(new FakeParameters(Parameters.parse("{\"filename\":\"" + diskCounts.getAbsolutePath() + "\"}")));
      ReduceNumberWordCount reducer = new ReduceNumberWordCount();
      Sorter sorter = new Sorter(new NumberWordCount.WordDocumentOrder());
      sorter.setProcessor(reducer);
      reducer.setProcessor(diskcounts);

      // three methods of filling it:
      // 1: documents - as in MemoryIndex
      Document d = new Document();
      d.terms = new ArrayList();
      for (int did = 0; did < 100; did++) {
        d.identifier = did;
        d.terms.clear();
        for (int tc = 0; tc < 100; tc++) {
          // randomly pick 100 terms from a vocab of 40
          String term = "term" + r.nextInt(40);
          d.terms.add(term);
          sorter.process(new NumberWordCount(Utility.fromString(term), did, 1));
        }
        memcounts1.addDocument(d);
      }

      sorter.close();

      // 2: iterator - as in CachedDiskIndex
      CountIndexReader diskreader = new CountIndexReader(diskCounts.getAbsolutePath());
      MemoryCountIndex memcounts2 = new MemoryCountIndex(diskreader.getManifest());
      KeyIterator dsk_ki = diskreader.getIterator();
      while (!dsk_ki.isDone()) {
        BaseIterator vi = dsk_ki.getValueIterator();
        vi.setContext(new ScoringContext());
        memcounts2.addIteratorData(dsk_ki.getKey(), vi);
        dsk_ki.nextKey();
      }

      KeyIterator mem1_ki = memcounts1.getIterator();
      KeyIterator mem2_ki = memcounts2.getIterator();
      dsk_ki = diskreader.getIterator();

      // disjunction to ensure we will throw an error if they are different
      while (!mem1_ki.isDone() || !mem2_ki.isDone() || !dsk_ki.isDone()) {
        CountIterator mem1_vi = (CountIterator) mem1_ki.getValueIterator();
        CountIterator mem2_vi = (CountIterator) mem2_ki.getValueIterator();
        CountIterator dsk_vi = (CountIterator) dsk_ki.getValueIterator();

        ScoringContext sc = new ScoringContext();
        mem1_vi.setContext(sc);
        mem2_vi.setContext(sc);
        dsk_vi.setContext(sc);

        while (!mem1_vi.isDone() || !mem2_vi.isDone() || !dsk_vi.isDone()) {
          assert (dsk_vi.currentCandidate() == mem1_vi.currentCandidate() && mem1_vi.currentCandidate() == mem2_vi.currentCandidate());
          sc.document = dsk_vi.currentCandidate();

          assert (dsk_vi.count() == mem1_vi.count() && mem1_vi.count() == mem2_vi.count());

          mem1_vi.movePast(mem1_vi.currentCandidate());
          mem2_vi.movePast(mem2_vi.currentCandidate());
          dsk_vi.movePast(dsk_vi.currentCandidate());
        }
        mem1_ki.nextKey();
        mem2_ki.nextKey();
        dsk_ki.nextKey();
      }

      memcounts1.close();
      memcounts2.close();
      diskreader.close();

    } finally {
      if (diskCounts != null) {
        diskCounts.delete();
      }
    }
  }

  public void testScores() throws Exception {
    File f = Utility.createTemporary();
    try {
      // builds a score index
      // compare it to the SparseFloatDiskIndex
      // - the memoryindex test does not test this part.

      MemorySparseDoubleIndex memScores = new MemorySparseDoubleIndex(new Parameters());

      int[] docs = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 52};
      double[] scores = {0.2, 1.1, 73, 0.01, -2, 7, 0, 0.01, 0.02, -1, -2};
      FakeScoreIterator scoreItr = new FakeScoreIterator(docs, scores);
      ScoringContext context = new ScoringContext();
      scoreItr.setContext(context);

      memScores.addIteratorData(Utility.fromString("key"), scoreItr);

      SparseFloatListWriter writer = new SparseFloatListWriter(new FakeParameters(Parameters.parse("{\"filename\":\"" + f.getAbsolutePath() + "\"}")));

      writer.processWord(Utility.fromString("key"));
      for (int i = 0; i < docs.length; i++) {
        writer.processNumber(docs[i]);
        writer.processTuple(scores[i]);
      }
      writer.close();

      SparseFloatListReader reader = new SparseFloatListReader(f.getAbsolutePath());

      ScoreIterator trueScoreItr = new FakeScoreIterator(docs, scores);
      ScoreIterator memScoreItr = memScores.getNodeScores(Utility.fromString("key"));
      ScoreIterator diskScoreItr = (ScoreIterator) reader.getIterator(new Node("scores", "key"));

      context = new ScoringContext();
      trueScoreItr.setContext(context);
      memScoreItr.setContext(context);
      diskScoreItr.setContext(context);

      while (!trueScoreItr.isDone() || !memScoreItr.isDone() || !diskScoreItr.isDone()) {
        int doc = trueScoreItr.currentCandidate();
        context.document = doc;

        assertEquals(trueScoreItr.currentCandidate(), memScoreItr.currentCandidate());
        assertEquals(trueScoreItr.currentCandidate(), diskScoreItr.currentCandidate());

        assertEquals(trueScoreItr.score(), memScoreItr.score(), 0.00000001);
        assertEquals(trueScoreItr.score(), diskScoreItr.score(), 0.0001);

        trueScoreItr.movePast(trueScoreItr.currentCandidate());
        memScoreItr.movePast(memScoreItr.currentCandidate());
        diskScoreItr.movePast(diskScoreItr.currentCandidate());
      }


    } finally {
      if (f != null) {
        f.delete();
      }
    }
  }
}
