/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.index.disk;

import java.io.File;
import java.util.Arrays;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.LengthsReader;
import org.lemurproject.galago.core.index.NamesReader;
import org.lemurproject.galago.core.retrieval.LocalRetrievalTest;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class CachedDiskIndexTest extends TestCase {

  public CachedDiskIndexTest(String testName) {
    super(testName);
  }

  public void testCacheDiskIndex() throws Exception {
    File trecCorpusFile = null,
            corpusFile = null,
            indexFile = null;

    try {
      // make a normal index
      File[] files = LocalRetrievalTest.make10DocIndex();
      trecCorpusFile = files[0];
      corpusFile = files[1];
      indexFile = files[2];

      // open cache index -- check that names and lengths work
      CachedDiskIndex cachedIndex = new CachedDiskIndex(indexFile.getAbsolutePath());
      DiskIndex diskIndex = new DiskIndex(indexFile.getAbsolutePath());
      // check the lengths and names
      LengthsReader.Iterator mls = cachedIndex.getLengthsIterator();
      NamesReader.Iterator mns = cachedIndex.getNamesIterator();
      LengthsReader.Iterator dls = diskIndex.getLengthsIterator();
      NamesReader.Iterator dns = diskIndex.getNamesIterator();
      do {
        assertEquals(mls.getCurrentIdentifier(), dls.getCurrentIdentifier());
        assertEquals(mls.getCurrentLength(), dls.getCurrentLength());
        assertEquals(mns.getCurrentIdentifier(), dns.getCurrentIdentifier());
        assertEquals(mns.getCurrentName(), dns.getCurrentName());
        mls.next();
        mns.next();
        dls.next();
        dns.next();
      } while (!mls.isDone() && !mns.isDone() && !dls.isDone() && !dns.isDone());

      assert (mls.isDone() && mns.isDone() && dls.isDone() && dns.isDone());

      Node query = StructuredQuery.parse("#extents:document:part=postings()");
      cachedIndex.cacheQueryData(query);

      MovableIterator di = diskIndex.getIterator(query);
      MovableIterator mi = cachedIndex.getIterator(query);

      do {
        assertEquals(di.getEntry(), mi.getEntry());
        di.next();
        mi.next();
      } while (!di.isDone() && !mi.isDone());

      assert(di.isDone() && mi.isDone());

      cachedIndex.close();

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

  public void testCacheRetrieval() throws Exception {
    File trecCorpusFile = null,
            corpusFile = null,
            indexFile = null;

    try {
      // make a normal index
      File[] files = LocalRetrievalTest.make10DocIndex();
      trecCorpusFile = files[0];
      corpusFile = files[1];
      indexFile = files[2];

      // open a disk retrieval
      Retrieval diskRetrieval = RetrievalFactory.instance(indexFile.getAbsolutePath(), new Parameters());

      // queries to be cached :
      String[] queries = {
        "#combine( cat document )",
        "#combine( moon document )",
        "#seqdep( the document )"};
      Parameters p = new Parameters();
      p.set("cacheQueries", Arrays.asList(queries));

      // open a cachedDisk retrieval
      Retrieval cachedDiskRetrieval = RetrievalFactory.instance(indexFile.getAbsolutePath(), p);

      for (String query : queries) {
        Node q = StructuredQuery.parse(query);
        Parameters q1p = new Parameters();
        Node diskQ = diskRetrieval.transformQuery(q, q1p);
        ScoredDocument[] diskResults = diskRetrieval.runQuery(diskQ);

        Parameters q2p = new Parameters();
        Node cachedQ = cachedDiskRetrieval.transformQuery(q, q2p);
        ScoredDocument[] cachedDiskResults = cachedDiskRetrieval.runQuery(cachedQ);
        
        assertEquals(diskResults.length, cachedDiskResults.length);

        for (int i = 0; i < diskResults.length; i++) {
          assertEquals(diskResults[i].document, cachedDiskResults[i].document);
          assertEquals(diskResults[i].rank, cachedDiskResults[i].rank);
          assertEquals(diskResults[i].score, cachedDiskResults[i].score);
        }
      }

      Node oov = StructuredQuery.parse("everything");
      Parameters oqp = new Parameters();
      oov = cachedDiskRetrieval.transformQuery(oov, oqp);
      ScoredDocument[] diskResults = diskRetrieval.runQuery(oov);
      ScoredDocument[] cachedDiskResults = cachedDiskRetrieval.runQuery(oov);

      assertEquals(diskResults.length, cachedDiskResults.length);

      for (int i = 0; i < diskResults.length; i++) {
        assertEquals(diskResults[i].document, cachedDiskResults[i].document);
        assertEquals(diskResults[i].rank, cachedDiskResults[i].rank);
        assertEquals(diskResults[i].score, cachedDiskResults[i].score);
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
