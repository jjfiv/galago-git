/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index;

import org.lemurproject.galago.core.index.disk.WindowIndexWriter;
import java.io.File;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.disk.WindowIndexReader;
import org.lemurproject.galago.core.index.disk.WindowIndexReader.KeyIterator;
import org.lemurproject.galago.core.index.disk.WindowIndexReader.TermExtentIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class WindowIndexTest extends TestCase {

  public WindowIndexTest(String name) {
    super(name);
  }

  public void testWindowIndex() throws Exception {
    File index = Utility.createTemporary();
    try {
      Parameters p = new Parameters();
      p.set("filename", index.getAbsolutePath());
      WindowIndexWriter writer = new WindowIndexWriter(new FakeParameters(p));

      writer.processExtentName(Utility.fromString("word1"));
      for (int doc = 0; doc < 10; doc += 2) {
        writer.processNumber(doc);
        for (int begin = 0; begin < 21; begin += 5) {
          writer.processBegin(begin);
          writer.processTuple(begin + 1);
          writer.processTuple(begin + 2);
        }
      }
      writer.processExtentName(Utility.fromString("word2"));
      for (int doc = 0; doc < 10; doc += 2) {
        writer.processNumber(doc);
        for (int begin = 0; begin < 21; begin += 5) {
          writer.processBegin(begin);
          writer.processTuple(begin + 1);
          writer.processTuple(begin + 2);
        }
      }

      writer.close();

      WindowIndexReader reader = new WindowIndexReader(index.getAbsolutePath());
      KeyIterator iterator = reader.getIterator();
      while (!iterator.isDone()) {
        iterator.getValueIterator();
        WindowIndexReader.TermExtentIterator valueIterator = (TermExtentIterator) iterator.getValueIterator();
        valueIterator.setContext(new ScoringContext());
        ScoringContext sc = valueIterator.getContext();
        int doccount = 0;
        int windowcount = 0;
        while (!valueIterator.isDone()) {
          doccount++;
          sc.document = valueIterator.currentCandidate();
          windowcount += valueIterator.extents().size();
          valueIterator.movePast(valueIterator.currentCandidate());
        }
        assert doccount == 5;
        assert windowcount == 50;

        iterator.nextKey();
      }

    } finally {
      index.delete();
    }
  }
}
