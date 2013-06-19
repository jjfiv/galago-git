/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.tools;

import java.io.File;
import java.util.Random;
import junit.framework.TestCase;
import org.lemurproject.galago.contrib.index.InvertedSketchIndexReader;
import org.lemurproject.galago.contrib.index.InvertedSketchIndexReader.KeyIterator;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.BuildIndex;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class BuildSketchIndexTest extends TestCase {

  public BuildSketchIndexTest(String testName) {
    super(testName);
  }

  public void testSomeMethod() throws Exception {
    File corpus = Utility.createTemporary();
    File index = Utility.createTemporaryDirectory();
    try {
      makeCorpus(corpus);

      Parameters idxParams = new Parameters();
      idxParams.set("inputPath", corpus.getAbsolutePath());
      idxParams.set("indexPath", index.getAbsolutePath());
      (new BuildIndex()).run(idxParams, System.out);

      Parameters sketchParams = new Parameters();
      sketchParams.set("inputPath", corpus.getAbsolutePath());
      sketchParams.set("indexPath", index.getAbsolutePath());
      sketchParams.set("sketchIndexName", "sketch-od1-e1-d2");
      sketchParams.set("n", 2);
      sketchParams.set("width", 1);
      sketchParams.set("ordered", true);
      sketchParams.set("error", 1.0);
      sketchParams.set("depth", 2);
      (new BuildSketchIndex()).run(sketchParams, System.out);

      LocalRetrieval ret = new LocalRetrieval(index.getAbsolutePath(), new Parameters());
      InvertedSketchIndexReader sketchIdx =
              (InvertedSketchIndexReader) ret.getIndex().getIndexPart("sketch-od1-e1-d2");

      long count = 0;
      KeyIterator iterator = sketchIdx.getIterator();
      while (!iterator.isDone()) {
        count += 1;
        iterator.nextKey();
      }
      assert (count <= 198);

      Node indexed = StructuredQuery.parse("#counts:2~3:part=sketch-od1-e1-d2()");
      CountIterator iter = (CountIterator) sketchIdx.getIterator(indexed);
      assert (iter != null);

      ScoringContext sc = new ScoringContext();
      iter.setContext(sc);
      while (!iter.isDone()) {
        sc.document = iter.currentCandidate();
        if (iter.hasMatch(sc.document)) {
          assert (iter.count() >= 1);
        }
        iter.syncTo(sc.document + 1);
      }

      ret.close();

      sketchParams = new Parameters();
      sketchParams.set("inputPath", corpus.getAbsolutePath());
      sketchParams.set("indexPath", index.getAbsolutePath());
      sketchParams.set("sketchIndexName", "sketch-od1-e100-d2");
      sketchParams.set("n", 2);
      sketchParams.set("width", 1);
      sketchParams.set("ordered", true);
      sketchParams.set("error", 100.0);
      sketchParams.set("depth", 2);
      (new BuildSketchIndex()).run(sketchParams, System.out);

      ret = new LocalRetrieval(index.getAbsolutePath(), new Parameters());
      sketchIdx =
              (InvertedSketchIndexReader) ret.getIndex().getIndexPart("sketch-od1-e100-d2");

      count = 0;
      iterator = sketchIdx.getIterator();
      while (!iterator.isDone()) {
        //System.err.println(iterator.getKeyString());
        count += 1;
        iterator.nextKey();
      }

      assert (count < 198);

      indexed = StructuredQuery.parse("#counts:2~3:part=sketch-od1-e1-d2()");
      iter = (CountIterator) sketchIdx.getIterator(indexed);
      assert (iter != null);

      sc = new ScoringContext();
      iter.setContext(sc);
      while (!iter.isDone()) {
        sc.document = iter.currentCandidate();
        if (iter.hasMatch(sc.document)) {
          assert (iter.count() >= 1);
        }
        iter.syncTo(sc.document + 1);
      }

      ret.close();

    } finally {
      corpus.delete();
      Utility.deleteDirectory(index);
    }
  }

  private void makeCorpus(File corpusFile) throws Exception {
    Random r = new Random();
    StringBuilder corpus = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      StringBuilder text = new StringBuilder();
      for (int j = 0; j < 100; j++) {
//        text.append(" ").append( r.nextInt( 100 ));
        text.append(" ").append(j);
      }
      corpus.append(trecDocument("doc-" + i, text.toString()));
    }
    Utility.copyStringToFile(corpus.toString(), corpusFile);
  }

  public static String trecDocument(String docno, String text) {
    return "<DOC>\n<DOCNO>" + docno + "</DOCNO>\n"
            + "<TEXT>\n" + text + "</TEXT>\n</DOC>\n";
  }
}
