/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.tools;

import java.io.File;
import java.util.Collections;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.disk.CountIndexReader;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.disk.WindowIndexReader;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.ExtentIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class BuildWindowIndexTest extends TestCase {

  public BuildWindowIndexTest(String name) {
    super(name);
  }

  public static String trecDocument(String docno, String text) {
    return "<DOC>\n<DOCNO>" + docno + "</DOCNO>\n"
            + "<TEXT>\n" + text + "</TEXT>\n</DOC>\n";
  }

  public void testVariousODWindowIndexes() throws Exception {
    File trecFolder = null;
    File index = null;
    try {
      trecFolder = FileUtility.createTemporary();
      trecFolder.delete();
      trecFolder.mkdir();
      index = FileUtility.createTemporary();
      index.delete();
      index.mkdir();

      Utility.copyStringToFile(trecDocument("d1", "<f>a b b b a c a c b b a a c a a c b a</f> z z z"), new File(trecFolder, "one.trectext"));
      Utility.copyStringToFile(trecDocument("d2", "<f>b a c a c b b a a c a a c b a c a a</f> z z z"), new File(trecFolder, "two.trectext"));
      Utility.copyStringToFile(trecDocument("d3", "<f>a c b b a a c a a c b a c a a b a a</f> z z z"), new File(trecFolder, "three.trectext"));

      Parameters indexParams = new Parameters();
      indexParams.set("inputPath", Collections.singletonList(trecFolder.getAbsolutePath()));
      indexParams.set("indexPath", index.getAbsolutePath());
      indexParams.set("stemmedPostings", false);
      indexParams.set("server", false);
      indexParams.set("tokenizer", new Parameters());
      indexParams.set("fieldIndex", false);
      indexParams.getMap("tokenizer").set("fields", Collections.singletonList("f"));
      App.run("build", indexParams, System.out);

      Parameters windowParams_count_NSE = new Parameters();
      windowParams_count_NSE.set("inputPath", trecFolder.getAbsolutePath());
      windowParams_count_NSE.set("indexPath", index.getAbsolutePath());
      windowParams_count_NSE.set("stemming", false);
      windowParams_count_NSE.set("server", false);
      windowParams_count_NSE.set("n", 3);
      windowParams_count_NSE.set("threshold", 2);
      windowParams_count_NSE.set("spaceEfficient", false);
      windowParams_count_NSE.set("positionalIndex", false);
      windowParams_count_NSE.set("outputIndexName", "count.nse.3.index");
      windowParams_count_NSE.set("fields", Collections.singleton("f"));
      App.run("build-window", windowParams_count_NSE, System.out);

      Parameters windowParams_count_SE = new Parameters();
      windowParams_count_SE.set("inputPath", trecFolder.getAbsolutePath());
      windowParams_count_SE.set("indexPath", index.getAbsolutePath());
      windowParams_count_SE.set("stemming", false);
      windowParams_count_SE.set("server", false);
      windowParams_count_SE.set("n", 3);
      windowParams_count_SE.set("threshold", 2);
      windowParams_count_SE.set("spaceEfficient", true);
      windowParams_count_SE.set("positionalIndex", false);
      windowParams_count_SE.set("outputIndexName", "count.se.3.index");
      windowParams_count_SE.set("fields", Collections.singleton("f"));
      App.run("build-window", windowParams_count_SE, System.out);

      Parameters windowParams_posit_NSE = new Parameters();
      windowParams_posit_NSE.set("inputPath", trecFolder.getAbsolutePath());
      windowParams_posit_NSE.set("indexPath", index.getAbsolutePath());
      windowParams_posit_NSE.set("stemming", false);
      windowParams_posit_NSE.set("server", false);
      windowParams_posit_NSE.set("n", 3);
      windowParams_posit_NSE.set("threshold", 2);
      windowParams_posit_NSE.set("spaceEfficient", false);
      windowParams_posit_NSE.set("positionalIndex", true);
      windowParams_posit_NSE.set("outputIndexName", "pos.nse.3.index");
      windowParams_posit_NSE.set("fields", Collections.singleton("f"));
      App.run("build-window", windowParams_posit_NSE, System.out);

      Parameters windowParams_posit_SE = new Parameters();
      windowParams_posit_SE.set("inputPath", trecFolder.getAbsolutePath());
      windowParams_posit_SE.set("indexPath", index.getAbsolutePath());
      windowParams_posit_SE.set("stemming", false);
      windowParams_posit_SE.set("server", false);
      windowParams_posit_SE.set("n", 3);
      windowParams_posit_SE.set("threshold", 2);
      windowParams_posit_SE.set("spaceEfficient", true);
      windowParams_posit_SE.set("positionalIndex", true);
      windowParams_posit_SE.set("outputIndexName", "pos.se.3.index");
      windowParams_posit_SE.set("fields", Collections.singleton("f"));
      App.run("build-window", windowParams_posit_SE, System.out);

      assert (new File(index, "count.se.3.index").exists());
      assert (new File(index, "count.nse.3.index").exists());
      assert (new File(index, "pos.se.3.index").exists());
      assert (new File(index, "pos.nse.3.index").exists());

      CountIndexReader counts_notSE = (CountIndexReader) DiskIndex.openIndexPart(new File(index, "count.nse.3.index").getAbsolutePath());
      CountIndexReader counts_SE = (CountIndexReader) DiskIndex.openIndexPart(new File(index, "count.se.3.index").getAbsolutePath());
      WindowIndexReader pos_notSE = (WindowIndexReader) DiskIndex.openIndexPart(new File(index, "pos.nse.3.index").getAbsolutePath());
      WindowIndexReader pos_SE = (WindowIndexReader) DiskIndex.openIndexPart(new File(index, "pos.se.3.index").getAbsolutePath());

      long vocab = 10;
      long collectionLength = 44;
      long highestFrequency = 7;
      long highestDocumentCount = 3;

      assertEquals(vocab, counts_notSE.getManifest().getLong("statistics/vocabCount"));
      assertEquals(vocab, counts_SE.getManifest().getLong("statistics/vocabCount"));
      assertEquals(vocab, pos_notSE.getManifest().getLong("statistics/vocabCount"));
      assertEquals(vocab, pos_SE.getManifest().getLong("statistics/vocabCount"));
      assertEquals(collectionLength, counts_notSE.getManifest().getLong("statistics/collectionLength"));
      assertEquals(collectionLength, counts_SE.getManifest().getLong("statistics/collectionLength"));
      assertEquals(collectionLength, pos_notSE.getManifest().getLong("statistics/collectionLength"));
      assertEquals(collectionLength, pos_notSE.getManifest().getLong("statistics/collectionLength"));
      assertEquals(highestFrequency, counts_notSE.getManifest().getLong("statistics/highestFrequency"));
      assertEquals(highestFrequency, counts_SE.getManifest().getLong("statistics/highestFrequency"));
      assertEquals(highestFrequency, pos_notSE.getManifest().getLong("statistics/highestFrequency"));
      assertEquals(highestFrequency, pos_notSE.getManifest().getLong("statistics/highestFrequency"));
      assertEquals(highestDocumentCount, counts_notSE.getManifest().getLong("statistics/highestDocumentCount"));
      assertEquals(highestDocumentCount, counts_SE.getManifest().getLong("statistics/highestDocumentCount"));
      assertEquals(highestDocumentCount, pos_notSE.getManifest().getLong("statistics/highestDocumentCount"));
      assertEquals(highestDocumentCount, pos_notSE.getManifest().getLong("statistics/highestDocumentCount"));


      KeyIterator counts_NSE_ki = counts_notSE.getIterator();
      KeyIterator counts__SE_ki = counts_SE.getIterator();
      KeyIterator posits_NSE_ki = pos_notSE.getIterator();
      KeyIterator posits__SE_ki = pos_SE.getIterator();

      // now verify that each of these indexes is essentially identical.
      while (!counts_NSE_ki.isDone()
              || !counts__SE_ki.isDone()
              || !posits_NSE_ki.isDone()
              || !posits__SE_ki.isDone()) {
        byte[] key = counts_NSE_ki.getKey();
        assert Utility.compare(key, counts_NSE_ki.getKey()) == 0;
        assert Utility.compare(key, counts__SE_ki.getKey()) == 0;
        assert Utility.compare(key, posits_NSE_ki.getKey()) == 0;
        assert Utility.compare(key, posits__SE_ki.getKey()) == 0;

        CountIterator counts_NSE_ci = (CountIterator) counts_NSE_ki.getValueIterator();
        CountIterator counts__SE_ci = (CountIterator) counts__SE_ki.getValueIterator();
        ExtentIterator posits_NSE_ei = (ExtentIterator) posits_NSE_ki.getValueIterator();
        ExtentIterator posits__SE_ei = (ExtentIterator) posits__SE_ki.getValueIterator();

        ScoringContext sc = new ScoringContext();

        while (!counts_NSE_ci.isDone()
                || !counts__SE_ci.isDone()
                || !posits_NSE_ei.isDone()
                || !posits__SE_ei.isDone()) {
          long doc = counts_NSE_ci.currentCandidate();
          assertEquals(doc, counts_NSE_ci.currentCandidate());
          assertEquals(doc, counts__SE_ci.currentCandidate());
          assertEquals(doc, posits_NSE_ei.currentCandidate());
          assertEquals(doc, posits__SE_ei.currentCandidate());

          sc.document = doc;

          int count = counts_NSE_ci.count(sc);
          assertEquals(count, counts_NSE_ci.count(sc));
          assertEquals(count, counts__SE_ci.count(sc));
          assertEquals(count, posits_NSE_ei.count(sc));
          assertEquals(count, posits__SE_ei.count(sc));

          ExtentArray extents_NSE = posits_NSE_ei.extents(sc);
          ExtentArray extents__SE = posits__SE_ei.extents(sc);
          for (int p = 0; p < Math.max(extents_NSE.size(), extents__SE.size()); p++) {
            assertEquals(extents_NSE.begin(p), extents__SE.begin(p));
            assertEquals(extents_NSE.end(p), extents__SE.end(p));
          }

          counts_NSE_ci.movePast(doc);
          counts__SE_ci.movePast(doc);
          posits_NSE_ei.movePast(doc);
          posits__SE_ei.movePast(doc);
        }
        counts_NSE_ki.nextKey();
        counts__SE_ki.nextKey();
        posits_NSE_ki.nextKey();
        posits__SE_ki.nextKey();
      }

    } finally {
      if (trecFolder != null) {
        Utility.deleteDirectory(trecFolder);
      }
      if (index != null) {
        Utility.deleteDirectory(index);
      }
    }
  }

  public void testVariousUWWindowIndexes() throws Exception {
    File trecFolder = null;
    File index = null;
    try {
      trecFolder = FileUtility.createTemporary();
      trecFolder.delete();
      trecFolder.mkdir();
      index = FileUtility.createTemporary();
      index.delete();
      index.mkdir();

      Utility.copyStringToFile(trecDocument("d1", "<f>a b b b a c a c b b a a c a a c b a</f> z z z"), new File(trecFolder, "one.trectext"));
      Utility.copyStringToFile(trecDocument("d2", "<f>b a c a c b b a a c a a c b a c a a</f> z z z"), new File(trecFolder, "two.trectext"));
      Utility.copyStringToFile(trecDocument("d3", "<f>a c b b a a c a a c b a c a a b a a</f> z z z"), new File(trecFolder, "three.trectext"));

      Parameters indexParams = new Parameters();
      indexParams.set("inputPath", Collections.singletonList(trecFolder.getAbsolutePath()));
      indexParams.set("indexPath", index.getAbsolutePath());
      indexParams.set("stemmedPostings", false);
      indexParams.set("server", false);
      indexParams.set("tokenizer", new Parameters());
      indexParams.set("fieldIndex", false);
      indexParams.getMap("tokenizer").set("fields", Collections.singletonList("f"));
      App.run("build", indexParams, System.out);

      Parameters windowParams_count_NSE = new Parameters();
      windowParams_count_NSE.set("inputPath", trecFolder.getAbsolutePath());
      windowParams_count_NSE.set("indexPath", index.getAbsolutePath());
      windowParams_count_NSE.set("stemming", false);
      windowParams_count_NSE.set("server", false);
      windowParams_count_NSE.set("n", 3);
      windowParams_count_NSE.set("ordered", false);
      windowParams_count_NSE.set("width", 12);
      windowParams_count_NSE.set("threshold", 2);
      windowParams_count_NSE.set("spaceEfficient", false);
      windowParams_count_NSE.set("positionalIndex", false);
      windowParams_count_NSE.set("outputIndexName", "count.nse.3.index");
      windowParams_count_NSE.set("fields", Collections.singleton("f"));
      App.run("build-window", windowParams_count_NSE, System.out);

      Parameters windowParams_count_SE = new Parameters();
      windowParams_count_SE.set("inputPath", trecFolder.getAbsolutePath());
      windowParams_count_SE.set("indexPath", index.getAbsolutePath());
      windowParams_count_SE.set("stemming", false);
      windowParams_count_SE.set("server", false);
      windowParams_count_SE.set("n", 3);
      windowParams_count_SE.set("ordered", false);
      windowParams_count_SE.set("width", 12);
      windowParams_count_SE.set("threshold", 2);
      windowParams_count_SE.set("spaceEfficient", true);
      windowParams_count_SE.set("positionalIndex", false);
      windowParams_count_SE.set("outputIndexName", "count.se.3.index");
      windowParams_count_SE.set("fields", Collections.singleton("f"));
      App.run("build-window", windowParams_count_SE, System.out);

      Parameters windowParams_posit_NSE = new Parameters();
      windowParams_posit_NSE.set("inputPath", trecFolder.getAbsolutePath());
      windowParams_posit_NSE.set("indexPath", index.getAbsolutePath());
      windowParams_posit_NSE.set("stemming", false);
      windowParams_count_SE.set("server", false);
      windowParams_posit_NSE.set("n", 3);
      windowParams_posit_NSE.set("ordered", false);
      windowParams_posit_NSE.set("width", 12);
      windowParams_posit_NSE.set("threshold", 2);
      windowParams_posit_NSE.set("spaceEfficient", false);
      windowParams_posit_NSE.set("positionalIndex", true);
      windowParams_posit_NSE.set("outputIndexName", "pos.nse.3.index");
      windowParams_posit_NSE.set("fields", Collections.singleton("f"));
      App.run("build-window", windowParams_posit_NSE, System.out);

      Parameters windowParams_posit_SE = new Parameters();
      windowParams_posit_SE.set("inputPath", trecFolder.getAbsolutePath());
      windowParams_posit_SE.set("indexPath", index.getAbsolutePath());
      windowParams_posit_SE.set("stemming", false);
      windowParams_count_SE.set("server", false);
      windowParams_posit_SE.set("n", 3);
      windowParams_posit_SE.set("ordered", false);
      windowParams_posit_SE.set("width", 12);
      windowParams_posit_SE.set("threshold", 2);
      windowParams_posit_SE.set("spaceEfficient", true);
      windowParams_posit_SE.set("positionalIndex", true);
      windowParams_posit_SE.set("outputIndexName", "pos.se.3.index");
      windowParams_posit_SE.set("fields", Collections.singleton("f"));
      App.run("build-window", windowParams_posit_SE, System.out);

      assert (new File(index, "count.se.3.index").exists());
      assert (new File(index, "count.nse.3.index").exists());
      assert (new File(index, "pos.se.3.index").exists());
      assert (new File(index, "pos.nse.3.index").exists());

      CountIndexReader counts_notSE = (CountIndexReader) DiskIndex.openIndexPart(new File(index, "count.nse.3.index").getAbsolutePath());
      CountIndexReader counts_SE = (CountIndexReader) DiskIndex.openIndexPart(new File(index, "count.se.3.index").getAbsolutePath());
      WindowIndexReader pos_notSE = (WindowIndexReader) DiskIndex.openIndexPart(new File(index, "pos.nse.3.index").getAbsolutePath());
      WindowIndexReader pos_SE = (WindowIndexReader) DiskIndex.openIndexPart(new File(index, "pos.se.3.index").getAbsolutePath());

      long vocab = 10;
      long collectionLength = 1650;
      long highestFrequency = 374;
      long highestDocumentCount = 3;
      assertEquals(vocab, counts_notSE.getManifest().getLong("statistics/vocabCount"));
      assertEquals(vocab, counts_SE.getManifest().getLong("statistics/vocabCount"));
      assertEquals(vocab, pos_notSE.getManifest().getLong("statistics/vocabCount"));
      assertEquals(vocab, pos_SE.getManifest().getLong("statistics/vocabCount"));
      assertEquals(collectionLength, counts_notSE.getManifest().getLong("statistics/collectionLength"));
      assertEquals(collectionLength, counts_SE.getManifest().getLong("statistics/collectionLength"));
      assertEquals(collectionLength, pos_notSE.getManifest().getLong("statistics/collectionLength"));
      assertEquals(collectionLength, counts_notSE.getManifest().getLong("statistics/collectionLength"));
      assertEquals(highestFrequency, counts_notSE.getManifest().getLong("statistics/highestFrequency"));
      assertEquals(highestFrequency, counts_SE.getManifest().getLong("statistics/highestFrequency"));
      assertEquals(highestFrequency, pos_notSE.getManifest().getLong("statistics/highestFrequency"));
      assertEquals(highestFrequency, pos_notSE.getManifest().getLong("statistics/highestFrequency"));
      assertEquals(highestDocumentCount, counts_notSE.getManifest().getLong("statistics/highestDocumentCount"));
      assertEquals(highestDocumentCount, counts_SE.getManifest().getLong("statistics/highestDocumentCount"));
      assertEquals(highestDocumentCount, pos_notSE.getManifest().getLong("statistics/highestDocumentCount"));
      assertEquals(highestDocumentCount, pos_notSE.getManifest().getLong("statistics/highestDocumentCount"));


      KeyIterator counts_NSE_ki = counts_notSE.getIterator();
      KeyIterator counts__SE_ki = counts_SE.getIterator();
      KeyIterator posits_NSE_ki = pos_notSE.getIterator();
      KeyIterator posits__SE_ki = pos_SE.getIterator();

      // now verify that each of these indexes is essentially identical.
      while (!counts_NSE_ki.isDone()
              || !counts__SE_ki.isDone()
              || !posits_NSE_ki.isDone()
              || !posits__SE_ki.isDone()) {
        byte[] key = counts_NSE_ki.getKey();
        assert Utility.compare(key, counts_NSE_ki.getKey()) == 0;
        assert Utility.compare(key, counts__SE_ki.getKey()) == 0;
        assert Utility.compare(key, posits_NSE_ki.getKey()) == 0;
        assert Utility.compare(key, posits__SE_ki.getKey()) == 0;

        CountIterator counts_NSE_ci = (CountIterator) counts_NSE_ki.getValueIterator();
        CountIterator counts__SE_ci = (CountIterator) counts__SE_ki.getValueIterator();
        ExtentIterator posits_NSE_ei = (ExtentIterator) posits_NSE_ki.getValueIterator();
        ExtentIterator posits__SE_ei = (ExtentIterator) posits__SE_ki.getValueIterator();

        ScoringContext sc = new ScoringContext();

        while (!counts_NSE_ci.isDone()
                || !counts__SE_ci.isDone()
                || !posits_NSE_ei.isDone()
                || !posits__SE_ei.isDone()) {
          long doc = counts_NSE_ci.currentCandidate();
          assertEquals(doc, counts_NSE_ci.currentCandidate());
          assertEquals(doc, counts__SE_ci.currentCandidate());
          assertEquals(doc, posits_NSE_ei.currentCandidate());
          assertEquals(doc, posits__SE_ei.currentCandidate());

          sc.document = doc;

          int count = counts_NSE_ci.count(sc);
          assertEquals(count, counts_NSE_ci.count(sc));
          assertEquals(count, counts__SE_ci.count(sc));
          assertEquals(count, posits_NSE_ei.count(sc));
          assertEquals(count, posits__SE_ei.count(sc));

          ExtentArray extents_NSE = posits_NSE_ei.extents(sc);
          ExtentArray extents__SE = posits__SE_ei.extents(sc);
          for (int p = 0; p < Math.max(extents_NSE.size(), extents__SE.size()); p++) {
            assertEquals(extents_NSE.begin(p), extents__SE.begin(p));
            assertEquals(extents_NSE.end(p), extents__SE.end(p));
          }

          counts_NSE_ci.movePast(doc);
          counts__SE_ci.movePast(doc);
          posits_NSE_ei.movePast(doc);
          posits__SE_ei.movePast(doc);
        }
        counts_NSE_ki.nextKey();
        counts__SE_ki.nextKey();
        posits_NSE_ki.nextKey();
        posits__SE_ki.nextKey();
      }

    } finally {
      if (trecFolder != null) {
        Utility.deleteDirectory(trecFolder);
      }
      if (index != null) {
        Utility.deleteDirectory(index);
      }
    }
  }
}
