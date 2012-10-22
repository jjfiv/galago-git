/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval;

import java.io.File;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.index.mem.MemoryCountIndex;
import org.lemurproject.galago.core.index.mem.MemorySparseDoubleIndex;
import org.lemurproject.galago.core.index.mem.MemoryWindowIndex;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableExtentIterator;
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

      NodeStatistics diskNS = nonCacheRet.getNodeStatistics(count);
      NodeStatistics cachedNS = cacheRet.getNodeStatistics(count);

      assertEquals(diskNS.nodeDocumentCount, cachedNS.nodeDocumentCount);
      assertEquals(diskNS.nodeFrequency, cachedNS.nodeFrequency);

      MovableCountIterator diskCountIterator = (MovableCountIterator) nonCacheRet.createIterator(new Parameters(), count, sc);
      MovableCountIterator cachedCountIterator = (MovableCountIterator) cacheRet.createIterator(new Parameters(), count, sc);
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

      MovableExtentIterator diskExtentIterator = (MovableExtentIterator) nonCacheRet.createIterator(new Parameters(), extent, sc);
      MovableExtentIterator cachedExtentIterator = (MovableExtentIterator) cacheRet.createIterator(new Parameters(), extent, sc);
      assert (cachedExtentIterator instanceof MemoryWindowIndex.ExtentIterator);

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
