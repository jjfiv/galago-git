/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.tools;

import org.junit.Assert;
import org.junit.Test;
import org.lemurproject.galago.core.tools.apps.BuildIndex;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.File;

import static org.lemurproject.galago.contrib.util.TestingUtils.trecDocument;

/**
 *
 * @author sjh
 */
public class BuildSketchIndexTest {
  @Test
  public void testSomeMethod() throws Exception {
    File corpus = FileUtility.createTemporary();
    File index = FileUtility.createTemporaryDirectory();
    try {
      makeCorpus(corpus);

      Parameters idxParams = Parameters.instance();
      idxParams.set("inputPath", corpus.getAbsolutePath());
      idxParams.set("indexPath", index.getAbsolutePath());
      (new BuildIndex()).run(idxParams, System.out);

      Parameters sketchParams = Parameters.instance();
      sketchParams.set("inputPath", corpus.getAbsolutePath());
      sketchParams.set("indexPath", index.getAbsolutePath());
      sketchParams.set("sketchIndexName", "sketch-od1-e1-d2");
      sketchParams.set("n", 2);
      sketchParams.set("width", 1);
      sketchParams.set("ordered", true);
      sketchParams.set("error", 1.0);
      sketchParams.set("depth", 2);
      (new BuildSketchIndex()).run(sketchParams, System.out);

//      LocalRetrieval ret = new LocalRetrieval(index.getAbsolutePath(), Parameters.instance());
//      InvertedSketchIndexReader sketchIdx =
//              (InvertedSketchIndexReader) ret.getIndex().getIndexPart("sketch-od1-e1-d2");
//
//      long count = 0;
//      KeyIterator iterator = sketchIdx.getIterator();
//      while (!iterator.isDone()) {
//        count += 1;
//        iterator.nextKey();
//      }
//      assert (count <= 198);
//
//      Node indexed = StructuredQuery.parse("#counts:2~3:part=sketch-od1-e1-d2()");
//      CountIterator iter = (CountIterator) sketchIdx.getIterator(indexed);
//      assert (iter != null);
//
//      ScoringContext sc = new ScoringContext();
//      iter.setContext(sc);
//      while (!iter.isDone()) {
//        sc.document = iter.currentCandidate();
//        if (iter.hasMatch(sc.document)) {
//          assert (iter.count() >= 1);
//        }
//        iter.syncTo(sc.document + 1);
//      }
//
//      ret.close();

      sketchParams = Parameters.instance();
      sketchParams.set("inputPath", corpus.getAbsolutePath());
      sketchParams.set("indexPath", index.getAbsolutePath());
      sketchParams.set("sketchIndexName", "sketch-od1-e100-d2");
      sketchParams.set("n", 2);
      sketchParams.set("width", 1);
      sketchParams.set("ordered", true);
      sketchParams.set("error", 100.0);
      sketchParams.set("depth", 2);
      (new BuildSketchIndex()).run(sketchParams, System.out);

//      ret = new LocalRetrieval(index.getAbsolutePath(), Parameters.instance());
//      sketchIdx =
//              (InvertedSketchIndexReader) ret.getIndex().getIndexPart("sketch-od1-e100-d2");
//
//      count = 0;
//      iterator = sketchIdx.getIterator();
//      while (!iterator.isDone()) {
//        //System.err.println(iterator.getKeyString());
//        count += 1;
//        iterator.nextKey();
//      }
//
//      assert (count < 198);
//
//      indexed = StructuredQuery.parse("#counts:2~3:part=sketch-od1-e1-d2()");
//      iter = (CountIterator) sketchIdx.getIterator(indexed);
//      assert (iter != null);
//
//      sc = new ScoringContext();
//      iter.setContext(sc);
//      while (!iter.isDone()) {
//        sc.document = iter.currentCandidate();
//        if (iter.hasMatch(sc.document)) {
//          assert (iter.count() >= 1);
//        }
//        iter.syncTo(sc.document + 1);
//      }
//
//      ret.close();

    } finally {
      Assert.assertTrue(corpus.delete());
      Utility.deleteDirectory(index);
    }
  }

  private static void makeCorpus(File corpusFile) throws Exception {
    StringBuilder corpus = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      StringBuilder text = new StringBuilder();
      for (int j = 0; j < 100; j++) {
        text.append(" ").append(j);
      }
      corpus.append(trecDocument("doc-" + i, text.toString()));
    }
    Utility.copyStringToFile(corpus.toString(), corpusFile);
  }
}
