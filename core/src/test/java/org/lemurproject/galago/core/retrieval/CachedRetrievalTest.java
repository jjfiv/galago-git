/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval;

import java.io.File;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
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


      Parameters p = new Parameters();
      p.set("caching", true);
      Retrieval ret = RetrievalFactory.instance(indexFile.getAbsolutePath(), p);

      assert ret instanceof CachedRetrieval;
      CachedRetrieval cachedRet = (CachedRetrieval) ret;

      Node count = StructuredQuery.parse("#counts:sample:part=postings()");
      MovableCountIterator diskIterator = (MovableCountIterator) cachedRet.createIterator(new Parameters(), count, null);
      cachedRet.addToCache(count);
      MovableCountIterator cacheIterator = (MovableCountIterator) cachedRet.createIterator(new Parameters(), count, null);

      while (!diskIterator.isDone() && !cacheIterator.isDone()) {
        assertEquals(diskIterator.getEntry(), cacheIterator.getEntry());
        diskIterator.next();
        cacheIterator.next();
      }


      //Node score = StructuredQuery.parse("#combine(#counts:sample:part=postings())");
      //score = cachedRet.transformQuery(score, p);
      //System.err.println(score.toString());
      //cachedRet.addToCache(score);

      //Node extent = StructuredQuery.parse("#extents:sample:part=postings()");
      //cachedRet.addToCache(extent);

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
