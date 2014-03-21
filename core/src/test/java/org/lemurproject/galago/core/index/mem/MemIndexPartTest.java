/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.index.mem;

import org.junit.Test;
import org.lemurproject.galago.core.index.KeyIterator;
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
import org.lemurproject.galago.tupleflow.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author sjh
 */
public class MemIndexPartTest {
  @Test
  public void testCounts() throws Exception {
    // builds three count indexes
    // - the memoryindex test does not test this part.

    Random r = new Random();
    File diskCounts = FileUtility.createTemporary();
    try {
      MemoryCountIndex memcounts1 = new MemoryCountIndex(new Parameters());
      CountIndexWriter diskcounts = new CountIndexWriter(new FakeParameters(Parameters.parseString("{\"filename\":\"" + diskCounts.getAbsolutePath() + "\"}")));
      ReduceNumberWordCount reducer = new ReduceNumberWordCount();
      Sorter<NumberWordCount> sorter = new Sorter<NumberWordCount>(new NumberWordCount.WordDocumentOrder());
      sorter.setProcessor(reducer);
      reducer.setProcessor(diskcounts);

      // three methods of filling it:
      // 1: documents - as in MemoryIndex
      Document d = new Document();
      d.terms = new ArrayList<String>();
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

        while (!mem1_vi.isDone() || !mem2_vi.isDone() || !dsk_vi.isDone()) {
          assert (dsk_vi.currentCandidate() == mem1_vi.currentCandidate() && mem1_vi.currentCandidate() == mem2_vi.currentCandidate());
          sc.document = dsk_vi.currentCandidate();

          assert (dsk_vi.count(sc) == mem1_vi.count(sc) && mem1_vi.count(sc) == mem2_vi.count(sc));

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
        assertTrue(diskCounts.delete());
      }
    }
  }

  @Test
  public void testScores() throws Exception {
    File f = FileUtility.createTemporary();
    try {
      // builds a score index
      // compare it to the SparseFloatDiskIndex
      // - the memoryindex test does not test this part.

      MemorySparseDoubleIndex memScores = new MemorySparseDoubleIndex(new Parameters());

      int[] docs = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 52};
      double[] scores = {0.2, 1.1, 73, 0.01, -2, 7, 0, 0.01, 0.02, -1, -2};
      FakeScoreIterator scoreItr = new FakeScoreIterator(docs, scores);
      ScoringContext context = new ScoringContext();

      memScores.addIteratorData(Utility.fromString("key"), scoreItr);

      SparseFloatListWriter writer = new SparseFloatListWriter(new FakeParameters(Parameters.parseString("{\"filename\":\"" + f.getAbsolutePath() + "\"}")));

      writer.processWord(Utility.fromString("key"));
      for (int i = 0; i < docs.length; i++) {
        writer.processNumber(docs[i]);
        writer.processTuple(scores[i]);
      }
      writer.close();

      SparseFloatListReader reader = new SparseFloatListReader(f.getAbsolutePath());

      ScoreIterator trueScoreItr = new FakeScoreIterator(docs, scores);
      ScoreIterator memScoreItr = memScores.getNodeScores(Utility.fromString("key"));
      ScoreIterator diskScoreItr = reader.getIterator(new Node("scores", "key"));


      while (!trueScoreItr.isDone() || !memScoreItr.isDone() || !diskScoreItr.isDone()) {
        context.document = trueScoreItr.currentCandidate();

        assertEquals(trueScoreItr.currentCandidate(), memScoreItr.currentCandidate());
        assertEquals(trueScoreItr.currentCandidate(), diskScoreItr.currentCandidate());

        assertEquals(trueScoreItr.score(context), memScoreItr.score(context), 0.00000001);
        assertEquals(trueScoreItr.score(context), diskScoreItr.score(context), 0.0001);

        trueScoreItr.movePast(trueScoreItr.currentCandidate());
        memScoreItr.movePast(memScoreItr.currentCandidate());
        diskScoreItr.movePast(diskScoreItr.currentCandidate());
      }


    } finally {
      if (f != null) {
        assertTrue(f.delete());
      }
    }
  }
}
