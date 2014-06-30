/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval;

import org.junit.Test;
import org.lemurproject.galago.core.index.mem.MemoryCountIndexCountSource;
import org.lemurproject.galago.core.index.mem.MemorySparseDoubleIndexScoreSource;
import org.lemurproject.galago.core.index.mem.MemoryWindowIndexExtentSource;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.ExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskCountIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskScoreIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author sjh
 */
public class CachedRetrievalTest {
  @Test
  public void testBuildCache() throws Exception {
    File trecCorpusFile = null;
    File corpusFile = null;
    File indexFile = null;
    try {
      File[] files = LocalRetrievalTest.make10DocIndex();
      trecCorpusFile = files[0];
      corpusFile = files[1];
      indexFile = files[2];

      LocalRetrieval nonCacheRet = (LocalRetrieval) RetrievalFactory.instance(indexFile.getAbsolutePath(), Parameters.instance());

      Parameters p = Parameters.instance();
      p.set("cache", true);
      p.set("cacheScores", true);
      p.set("cacheStats", true);
      LocalRetrieval cacheRet = (LocalRetrieval) RetrievalFactory.instance(indexFile.getAbsolutePath(), p);

      // SCORE node
      Node score = StructuredQuery.parse("#dirichlet(#counts:is:part=postings())");
      score = cacheRet.transformQuery(score, p);
      cacheRet.addNodeToCache(score);

      ScoringContext sc = new ScoringContext();

      ScoreIterator diskScoreIterator = (ScoreIterator) nonCacheRet.createIterator(Parameters.instance(), score);
      ScoreIterator cachedScoreIterator = (ScoreIterator) cacheRet.createIterator(Parameters.instance(), score);
      assert (((DiskScoreIterator) cachedScoreIterator).getSource() instanceof MemorySparseDoubleIndexScoreSource);

      while (!diskScoreIterator.isDone() && !cachedScoreIterator.isDone()) {
        assertEquals(diskScoreIterator.currentCandidate(), cachedScoreIterator.currentCandidate());
        sc.document = diskScoreIterator.currentCandidate();
        assertEquals(diskScoreIterator.score(sc), cachedScoreIterator.score(sc), 0.000001);
        diskScoreIterator.movePast(sc.document);
        cachedScoreIterator.movePast(sc.document);
      }


      // COUNT node
      Node count = StructuredQuery.parse("#counts:is:part=postings()");
      cacheRet.addNodeToCache(count);

      NodeStatistics diskNS = nonCacheRet.getNodeStatistics(count); // from disk
      NodeStatistics cachedNS = cacheRet.getNodeStatistics(count);  // from memory-cached-node
      NodeStatistics cachedNS2 = cacheRet.getNodeStatistics(count); // stored in statistic cache

      assertEquals(diskNS.nodeDocumentCount, cachedNS.nodeDocumentCount);
      assertEquals(diskNS.nodeFrequency, cachedNS.nodeFrequency);
      assertEquals(diskNS.nodeDocumentCount, cachedNS2.nodeDocumentCount);
      assertEquals(diskNS.nodeFrequency, cachedNS2.nodeFrequency);

      CountIterator diskCountIterator = (CountIterator) nonCacheRet.createIterator(Parameters.instance(), count);
      CountIterator cachedCountIterator = (CountIterator) cacheRet.createIterator(Parameters.instance(), count);
      assert (((DiskCountIterator) cachedCountIterator).getSource() instanceof MemoryCountIndexCountSource);

      while (!diskCountIterator.isDone() && !cachedCountIterator.isDone()) {
        assertEquals(diskCountIterator.currentCandidate(), cachedCountIterator.currentCandidate());
        sc.document = diskCountIterator.currentCandidate();
        assertEquals(diskCountIterator.count(sc), cachedCountIterator.count(sc));
        diskCountIterator.movePast(diskCountIterator.currentCandidate());
        cachedCountIterator.movePast(cachedCountIterator.currentCandidate());
      }

      // EXTENT node
      Node extent = StructuredQuery.parse("#extents:sample:part=postings()");
      cacheRet.addNodeToCache(extent);

      diskNS = nonCacheRet.getNodeStatistics(extent);
      cachedNS = cacheRet.getNodeStatistics(extent);

      assertEquals(diskNS.nodeDocumentCount, cachedNS.nodeDocumentCount);
      assertEquals(diskNS.nodeFrequency, cachedNS.nodeFrequency);

      ExtentIterator diskExtentIterator = (ExtentIterator) nonCacheRet.createIterator(Parameters.instance(), extent);
      ExtentIterator cachedExtentIterator = (ExtentIterator) cacheRet.createIterator(Parameters.instance(), extent);
      assert (((DiskExtentIterator) cachedExtentIterator).getSource() instanceof MemoryWindowIndexExtentSource);

      while (!diskExtentIterator.isDone() && !cachedExtentIterator.isDone()) {
        assertEquals(diskExtentIterator.currentCandidate(), cachedExtentIterator.currentCandidate());
        sc.document = cachedExtentIterator.currentCandidate();
        assertEquals(diskExtentIterator.count(sc), cachedExtentIterator.count(sc));
        ExtentArray de = diskExtentIterator.extents(sc);
        ExtentArray ce = cachedExtentIterator.extents(sc);
        assertEquals(de.begin(0), ce.begin(0));
        assertEquals(de.end(0), ce.end(0));

        diskExtentIterator.movePast(diskExtentIterator.currentCandidate());
        cachedExtentIterator.movePast(cachedExtentIterator.currentCandidate());
      }

      // check leaf node caching
      Parameters p2 = Parameters.instance();
      p2.set("cache", true);
      p2.set("cacheScores", true);
      p2.set("cacheLeafNodes", false);
      LocalRetrieval cacheRet2 = (LocalRetrieval) RetrievalFactory.instance(indexFile.getAbsolutePath(), p2);

      extent = StructuredQuery.parse("#extents:sample:part=postings()");
      cacheRet2.addNodeToCache(extent);

      Node extent2 = StructuredQuery.parse("#unordered:8( #extents:sample:part=postings() #extents:document:part=postings() )");
      cacheRet2.addNodeToCache(extent2);

      cachedExtentIterator = (ExtentIterator) cacheRet2.createIterator(Parameters.instance(), extent);
      assertFalse(((DiskExtentIterator) cachedExtentIterator).getSource() instanceof MemoryWindowIndexExtentSource);

      cachedExtentIterator = (ExtentIterator) cacheRet2.createIterator(Parameters.instance(), extent2);
      System.err.println(((DiskExtentIterator) cachedExtentIterator).getSource().getClass().toString());
      assertTrue(((DiskExtentIterator) cachedExtentIterator).getSource() instanceof MemoryWindowIndexExtentSource);

    } finally {
      if (trecCorpusFile != null) {
        trecCorpusFile.delete();
      }
      if (corpusFile != null) {
        Utility.deleteDirectory(corpusFile);
      }
      if (indexFile != null) {
        Utility.deleteDirectory(indexFile);
      }
    }
  }
}
