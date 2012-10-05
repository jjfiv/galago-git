/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.merge;

import java.io.File;
import java.util.ArrayList;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.mem.FlushToDisk;
import org.lemurproject.galago.core.index.mem.MemoryIndex;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.TagTokenizer;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class MergeIndexTest extends TestCase {

  public MergeIndexTest(String name) {
    super(name);
  }

  public void testMergeDiskIndexes() throws Exception {
    File trecData1 = null;
    File trecData2 = null;
    File index1 = null;
    File index2 = null;
    File indexmerged = null;
    try {
      StringBuilder docs1 = new StringBuilder();
      StringBuilder docs2 = new StringBuilder();
      for (int i = 0; i < 100; i++) {
        docs1.append(AppTest.trecDocument("DOCS1-" + i, "This is sample document " + i));
        docs2.append(AppTest.trecDocument("DOCS2-" + i, "This is a different document " + i));
      }
      trecData1 = Utility.createTemporary();
      trecData2 = Utility.createTemporary();
      Utility.copyStringToFile(docs1.toString(), trecData1);
      Utility.copyStringToFile(docs2.toString(), trecData2);

      index1 = Utility.createTemporaryDirectory();
      App.main(new String[]{"build", "--inputPath=" + trecData1.getAbsolutePath(),
                "--indexPath=" + index1.getAbsolutePath(),
                "--corpus=false"});

      index2 = Utility.createTemporaryDirectory();
      App.main(new String[]{"build", "--inputPath=" + trecData2.getAbsolutePath(),
                "--indexPath=" + index2.getAbsolutePath(),
                "--corpus=false"});

      AppTest.verifyIndexStructures(index1);
      AppTest.verifyIndexStructures(index2);

      indexmerged = Utility.createTemporaryDirectory();

      Parameters mergeParams = new Parameters();
      mergeParams.set("indexPath", indexmerged.getAbsolutePath());
      ArrayList<String> inputs = new ArrayList();
      inputs.add(index1.getAbsolutePath());
      inputs.add(index2.getAbsolutePath());
      mergeParams.set("inputPath", inputs);
      App.run("merge-index", mergeParams, System.out);

      AppTest.verifyIndexStructures(indexmerged);

      DiskIndex di_index1 = new DiskIndex(index1.getAbsolutePath());
      DiskIndex di_index2 = new DiskIndex(index2.getAbsolutePath());
      DiskIndex di_merged = new DiskIndex(indexmerged.getAbsolutePath());

      assertEquals(di_index1.getIndexPartStatistics("postings").collectionLength, 500);
      assertEquals(di_index2.getIndexPartStatistics("postings").collectionLength, 600);
      assertEquals(di_merged.getIndexPartStatistics("postings").collectionLength, 1100);

      assertEquals(di_index1.getIndexPartStatistics("postings").vocabCount, 104);
      assertEquals(di_index2.getIndexPartStatistics("postings").vocabCount, 105);
      assertEquals(di_merged.getIndexPartStatistics("postings").vocabCount, 106);

      assertEquals(di_index1.getIndexPartStatistics("postings").highestDocumentCount, 100);
      assertEquals(di_index2.getIndexPartStatistics("postings").highestDocumentCount, 100);
      assertEquals(di_merged.getIndexPartStatistics("postings").highestDocumentCount, 200);

      assertEquals(di_index1.getIndexPartStatistics("postings").highestFrequency, 100);
      assertEquals(di_index2.getIndexPartStatistics("postings").highestFrequency, 100);
      assertEquals(di_merged.getIndexPartStatistics("postings").highestFrequency, 200);
      
      assertEquals(di_merged.getName(50), "DOCS1-50");
      assertEquals(di_merged.getName(150), "DOCS2-50");

    } finally {
      if (trecData1 != null) {
        trecData1.delete();
      }
      if (trecData2 != null) {
        trecData2.delete();
      }
      if (index1 != null) {
        Utility.deleteDirectory(index1);
      }
      if (index2 != null) {
        Utility.deleteDirectory(index2);
      }
      if (indexmerged != null) {
        Utility.deleteDirectory(indexmerged);
      }
    }
  }

  public void testMergeFlushedSequentialIndexes() throws Exception {
    File index1 = null;
    File index2 = null;
    File indexmerged = null;
    try {
      Parameters p1 = Parameters.parse("{\"documentNumberOffset\":0}");
      Parameters p2 = Parameters.parse("{\"documentNumberOffset\":1000}");
      MemoryIndex mi1 = new MemoryIndex(new FakeParameters(p1));
      MemoryIndex mi2 = new MemoryIndex(new FakeParameters(p2));

      TagTokenizer tok = new TagTokenizer();
      for (int i = 0; i < 100; i++) {
        Document d1 = new Document("DOCS1-" + i, "this is sample document " + i);
        Document d2 = new Document("DOCS2-" + i, "this is a different document " + i);
        tok.tokenize(d1);
        tok.tokenize(d2);
        mi1.process(d1);
        mi2.process(d2);
      }

      index1 = Utility.createTemporaryDirectory();
      (new FlushToDisk()).flushMemoryIndex(mi1, index1.getAbsolutePath());

      index2 = Utility.createTemporaryDirectory();
      (new FlushToDisk()).flushMemoryIndex(mi2, index2.getAbsolutePath());

      AppTest.verifyIndexStructures(index1);
      AppTest.verifyIndexStructures(index2);

      indexmerged = Utility.createTemporaryDirectory();

      Parameters mergeParams = new Parameters();
      mergeParams.set("indexPath", indexmerged.getAbsolutePath());
      ArrayList<String> inputs = new ArrayList();
      inputs.add(index1.getAbsolutePath());
      inputs.add(index2.getAbsolutePath());
      mergeParams.set("inputPath", inputs);
      mergeParams.set("renumberDocuments", false);
      App.run("merge-index", mergeParams, System.out);

      AppTest.verifyIndexStructures(indexmerged);

      DiskIndex di_index1 = new DiskIndex(index1.getAbsolutePath());
      DiskIndex di_index2 = new DiskIndex(index2.getAbsolutePath());
      DiskIndex di_merged = new DiskIndex(indexmerged.getAbsolutePath());

      assertEquals(di_index1.getIndexPartStatistics("postings").collectionLength, 500);
      assertEquals(di_index2.getIndexPartStatistics("postings").collectionLength, 600);
      assertEquals(di_merged.getIndexPartStatistics("postings").collectionLength, 1100);

      assertEquals(di_index1.getIndexPartStatistics("postings").vocabCount, 104);
      assertEquals(di_index2.getIndexPartStatistics("postings").vocabCount, 105);
      assertEquals(di_merged.getIndexPartStatistics("postings").vocabCount, 106);

      assertEquals(di_index1.getIndexPartStatistics("postings").highestDocumentCount, 100);
      assertEquals(di_index2.getIndexPartStatistics("postings").highestDocumentCount, 100);
      assertEquals(di_merged.getIndexPartStatistics("postings").highestDocumentCount, 200);

      assertEquals(di_index1.getIndexPartStatistics("postings").highestFrequency, 100);
      assertEquals(di_index2.getIndexPartStatistics("postings").highestFrequency, 100);
      assertEquals(di_merged.getIndexPartStatistics("postings").highestFrequency, 200);
      
      assertEquals(di_merged.getName(50), mi1.getName(50));
      assertEquals(di_merged.getName(1050), mi2.getName(1050));

    } finally {
      if (index1 != null) {
        Utility.deleteDirectory(index1);
      }
      if (index2 != null) {
        Utility.deleteDirectory(index2);
      }
      if (indexmerged != null) {
        Utility.deleteDirectory(indexmerged);
      }
    }
  }
}
