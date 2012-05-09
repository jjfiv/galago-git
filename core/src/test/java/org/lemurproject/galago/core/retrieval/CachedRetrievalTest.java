/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval;

import java.io.File;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.index.mem.MemoryCountIndex;
import org.lemurproject.galago.core.index.mem.MemorySparseFloatIndex;
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

      LocalRetrieval ret = (LocalRetrieval) RetrievalFactory.instance(indexFile.getAbsolutePath(), new Parameters());

      Parameters p = new Parameters();
      p.set("caching", true);
      CachedRetrieval cachedRet = (CachedRetrieval) RetrievalFactory.instance(indexFile.getAbsolutePath(), p);

      // SCORE node
      Node score = StructuredQuery.parse("#feature:dirichlet(#counts:is:part=postings())");
      score = cachedRet.transformQuery(score, p);
      cachedRet.addToCache(score);

      ScoringContext sc = new ScoringContext();
      ProcessingModel.initializeLengths(ret, sc);

      MovableScoreIterator diskScoreIterator = (MovableScoreIterator) ret.createIterator(new Parameters(), score, sc);
      MovableScoreIterator cachedScoreIterator = (MovableScoreIterator) cachedRet.createIterator(new Parameters(), score, sc);
      assert(cachedScoreIterator instanceof MemorySparseFloatIndex.ScoresIterator);

      while (!diskScoreIterator.isDone() && !cachedScoreIterator.isDone()) {
        assertEquals(diskScoreIterator.currentCandidate(), cachedScoreIterator.currentCandidate());
        sc.document = diskScoreIterator.currentCandidate();
        sc.moveLengths(diskScoreIterator.currentCandidate());
        assertEquals(diskScoreIterator.score(), cachedScoreIterator.score(), 0.000001);
        diskScoreIterator.next();
        cachedScoreIterator.next();
      }


      // COUNT node
      Node count = StructuredQuery.parse("#counts:is:part=postings()");
      cachedRet.addToCache(count);

      NodeStatistics diskNS = ret.nodeStatistics(count);
      NodeStatistics cachedNS = cachedRet.nodeStatistics(count);

      assertEquals(diskNS.collectionLength, cachedNS.collectionLength);
      assertEquals(diskNS.documentCount, cachedNS.documentCount);
      assertEquals(diskNS.nodeDocumentCount, cachedNS.nodeDocumentCount);
      assertEquals(diskNS.nodeFrequency, cachedNS.nodeFrequency);

      MovableCountIterator diskCountIterator = (MovableCountIterator) ret.createIterator(new Parameters(), count, null);
      MovableCountIterator cachedCountIterator = (MovableCountIterator) cachedRet.createIterator(new Parameters(), count, null);
      assert(cachedCountIterator instanceof MemoryCountIndex.CountsIterator);

      while (!diskCountIterator.isDone() && !cachedCountIterator.isDone()) {
        assertEquals(diskCountIterator.currentCandidate(), cachedCountIterator.currentCandidate());
        assertEquals(diskCountIterator.count(), cachedCountIterator.count());
        diskCountIterator.next();
        cachedCountIterator.next();
      }

      // EXTENT node
      Node extent = StructuredQuery.parse("#extents:sample:part=postings()");
      cachedRet.addToCache(extent);

      diskNS = ret.nodeStatistics(extent);
      cachedNS = cachedRet.nodeStatistics(extent);

      assertEquals(diskNS.collectionLength, cachedNS.collectionLength);
      assertEquals(diskNS.documentCount, cachedNS.documentCount);
      assertEquals(diskNS.nodeDocumentCount, cachedNS.nodeDocumentCount);
      assertEquals(diskNS.nodeFrequency, cachedNS.nodeFrequency);

      MovableExtentIterator diskExtentIterator = (MovableExtentIterator) ret.createIterator(new Parameters(), extent, null);
      MovableExtentIterator cachedExtentIterator = (MovableExtentIterator) cachedRet.createIterator(new Parameters(), extent, null);
      assert(cachedExtentIterator instanceof MemoryWindowIndex.ExtentIterator);

      while (!diskExtentIterator.isDone() && !cachedExtentIterator.isDone()) {
        assertEquals(diskExtentIterator.currentCandidate(), cachedExtentIterator.currentCandidate());
        assertEquals(diskExtentIterator.count(), cachedExtentIterator.count());
        ExtentArray de = diskExtentIterator.extents();
        ExtentArray ce = cachedExtentIterator.extents();
        assertEquals(de.begin(0), ce.begin(0));
        assertEquals(de.end(0), ce.end(0));

        diskExtentIterator.next();
        cachedExtentIterator.next();
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
