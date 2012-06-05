/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.tools;

import java.io.File;
import java.util.Collections;
import junit.framework.TestCase;
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

  public void testVariousWindowIndexes() throws Exception {
    File trecFolder = null;
    File index = null;
    try {
      trecFolder = Utility.createTemporary();
      trecFolder.delete();
      trecFolder.mkdir();
      index = Utility.createTemporary();
      index.delete();
      index.mkdir();
      
      Utility.copyStringToFile(trecDocument("1", "<f>a b b b a c a c b b a a c a a c b a</f> z z z"), new File(trecFolder, "one"));
      Utility.copyStringToFile(trecDocument("2", "<f>b a c a c b b a a c a a c b a c a a</f> z z z"), new File(trecFolder, "two"));
      Utility.copyStringToFile(trecDocument("3", "<f>a c b b a a c a a c b a c a a b a a</f> z z z"), new File(trecFolder, "three"));
      
      Parameters indexParams = new Parameters();
      indexParams.set("inputPath", Collections.singletonList(trecFolder.getAbsolutePath()));
      indexParams.set("indexPath", index.getAbsolutePath());
      indexParams.set("stemmedPostings", false);
      indexParams.set("tokenizer", new Parameters());
      indexParams.set("fieldIndex", false);
      indexParams.getMap("tokenizer").set("fields", Collections.singletonList("f"));
      App.run("build", indexParams, System.out);
      
      for(File f : index.listFiles()){
        System.err.println(f);
      }
      
//      Parameters windowParams_1 = new Parameters();
//      windowParams_1.set("inputPath", trecFolder.getAbsolutePath());
//      windowParams_1.set("indexPath", index.getAbsolutePath());
//      windowParams_1.set("n", 3);
//      windowParams_1.set("spaceEfficient", false);
//      windowParams_1.set("positionalIndex", false);
//      windowParams_1.set("outputIndexName", "count.nse.3.index");
//      windowParams_1.set("fields", Collections.singleton("f"));
//      BuildWindowIndex bwi = new BuildWindowIndex();
//      bwi.run(windowParams_1, System.out);

//      assert( new File(index, "count.nse.3.index").exists() );
      
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
