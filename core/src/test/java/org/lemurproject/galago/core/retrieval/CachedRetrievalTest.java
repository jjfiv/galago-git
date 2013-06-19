/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval;

import java.io.File;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.mem.MemoryCountIndex;
import org.lemurproject.galago.core.index.mem.MemorySparseDoubleIndex;
import org.lemurproject.galago.core.index.mem.MemoryWindowIndex;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.ExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableScoreIterator;
import org.lemurproject.galago.core.retrieval.processing.ProcessingModel;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class CachedRetrievalTest extends TestCase {

  public CachedRetrievalTest(String name) {
    super(name);
  }

  public void testBuildCache() throws Exception {
    File trecCorpusFile = null;
    File corpusFile = null;
    File indexFile = null;
    try {
      File[] files = LocalRetrievalTest.make10DocIndex();
      trecCorpusFile = files[0];
      corpusFile = files[1];
      indexFile = files[2];

      LocalRetrieval nonCacheRet = (LocalRetrieval) RetrievalFactory.instance(indexFile.getAbsolutePath(), new Parameters());

      Parameters p = new Parameters();
      p.set("cache", true);
      p.set("cacheScores", true);
      p.set("cacheStats", true);
      LocalRetrieval cacheRet = (LocalRetrieval) RetrievalFactory.instance(indexFile.getAbsolutePath(), p);

      // SCORE node
      Node score = StructuredQuery.parse("#feature:dirichlet(#counts:is:part=postings())");
      score = cacheRet.transformQuery(score, p);
      cacheRet.addNodeToCache(score);

      ScoringContext sc = new ScoringContext();
      ProcessingModel.initializeLengths(nonCacheRet, sc);

      MovableScoreIterator diskScoreIterator = (MovableScoreIterator) nonCacheRet.createIterator(new Parameters(), score, sc);
      MovableScoreIterator cachedScoreIterator = (MovableScoreIterator) cacheRet.createIterator(new Parameters(), score, sc);
      assert (cachedScoreIterator instanceof MemorySparseDoubleIndex.ScoresIterator);

      while (!diskScoreIterator.isDone() && !cachedScoreIterator.isDone()) {
        assertEquals(diskScoreIterator.currentCandidate(), cachedScoreIterator.currentCandidate());
        sc.document = diskScoreIterator.currentCandidate();
        sc.moveLengths(diskScoreIterator.currentCandidate());
        assertEquals(diskScoreIterator.score(), cachedScoreIterator.score(), 0.000001);
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

      CountIterator diskCountIterator = (CountIterator) nonCacheRet.createIterator(new Parameters(), count, sc);
      CountIterator cachedCountIterator = (CountIterator) cacheRet.createIterator(new Parameters(), count, sc);
      assert (cachedCountIterator instanceof MemoryCountIndex.CountsIterator);

      while (!diskCountIterator.isDone() && !cachedCountIterator.isDone()) {
        assertEquals(diskCountIterator.currentCandidate(), cachedCountIterator.currentCandidate());
        sc.document = diskCountIterator.currentCandidate();
        assertEquals(diskCountIterator.count(), cachedCountIterator.count());
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

      ExtentIterator diskExtentIterator = (ExtentIterator) nonCacheRet.createIterator(new Parameters(), extent, sc);
      ExtentIterator cachedExtentIterator = (ExtentIterator) cacheRet.createIterator(new Parameters(), extent, sc);
      assert (cachedExtentIterator instanceof MemoryWindowIndex.MemExtentIterator);

      while (!diskExtentIterator.isDone() && !cachedExtentIterator.isDone()) {
        assertEquals(diskExtentIterator.currentCandidate(), cachedExtentIterator.currentCandidate());
        sc.document = cachedExtentIterator.currentCandidate();
        assertEquals(diskExtentIterator.count(), cachedExtentIterator.count());
        ExtentArray de = diskExtentIterator.extents();
        ExtentArray ce = cachedExtentIterator.extents();
        assertEquals(de.begin(0), ce.begin(0));
        assertEquals(de.end(0), ce.end(0));

        diskExtentIterator.movePast(diskExtentIterator.currentCandidate());
        cachedExtentIterator.movePast(cachedExtentIterator.currentCandidate());
      }

      // check leaf node caching
      Parameters p2 = new Parameters();
      p2.set("cache", true);
      p2.set("cacheScores", true);
      p2.set("cacheLeafNodes", false);
      LocalRetrieval cacheRet2 = (LocalRetrieval) RetrievalFactory.instance(indexFile.getAbsolutePath(), p2);

      extent = StructuredQuery.parse("#extents:sample:part=postings()");
      cacheRet2.addNodeToCache(extent);

      Node extent2 = StructuredQuery.parse("#unordered:8( #extents:sample:part=postings() #extents:document:part=postings() )");
      cacheRet2.addNodeToCache(extent2);
      
      cachedExtentIterator = (ExtentIterator) cacheRet2.createIterator(new Parameters(), extent, sc);
      assertFalse(cachedExtentIterator instanceof MemoryWindowIndex.MemExtentIterator);

      cachedExtentIterator = (ExtentIterator) cacheRet2.createIterator(new Parameters(), extent2, sc);
      assertTrue(cachedExtentIterator instanceof MemoryWindowIndex.MemExtentIterator);

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
