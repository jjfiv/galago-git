/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import junit.framework.TestCase;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class GenerateWorkingSetQueriesTest extends TestCase {

  File dataFolder, corpus, index;

  public GenerateWorkingSetQueriesTest(String testName) {
    super(testName);
  }

  public static String trecDocument(String docno, String text) {
    return "<DOC>\n<DOCNO>" + docno + "</DOCNO>\n"
            + "<TEXT>\n" + text + "</TEXT>\n</DOC>\n";
  }

  @Override
  public void setUp() throws Exception {
    dataFolder = Utility.createTemporaryDirectory();

    corpus = new File(dataFolder, "corpus.trectext");
    corpus.createNewFile();

    index = new File(dataFolder, "index");
    index.mkdirs();

    Random r = new Random();
    StringBuilder c = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      StringBuilder data = new StringBuilder();
      for (int j = 0; j < (i + 10); j++) {
        data.append(" t").append(r.nextInt(i + 10));
      }
      c.append(trecDocument("d-" + i, "Test text" + data.toString()));
    }
    Utility.copyStringToFile(c.toString(), corpus);

    Parameters p = new Parameters();
    p.set("inputPath", corpus.getAbsolutePath());
    p.set("indexPath", index.getAbsolutePath());
    p.set("distrib", 1);
    App.run("build", p, System.out);
  }

  @Override
  public void tearDown() throws IOException {
    if (dataFolder != null) {
      Utility.deleteDirectory(dataFolder);
    }
  }

  public void testSomeMethod() throws Exception {

    File out = new File(dataFolder, "out1");

    Parameters p = new Parameters();
    p.set("index", index.getAbsolutePath());
    p.set("topK", 10);
    p.set("output", out.getAbsolutePath());
    p.set("wsNumders", true);
    p.set("queries", new ArrayList());
    p.getList("queries").add(Parameters.parse("{\"number\" : \"q1\", \"text\" : \"#combine(t1 t2)\"}"));
    p.getList("queries").add(Parameters.parse("{\"number\" : \"q2\", \"text\" : \"#combine(t3 t4)\"}"));
    p.getList("queries").add(Parameters.parse("{\"number\" : \"q3\", \"text\" : \"#combine(t5 t6)\"}"));

    GenerateWorkingSetQueries generator = new GenerateWorkingSetQueries();
    generator.run(p, System.err);

    Parameters outputParams = Parameters.parse(out);

    List<Parameters> outQueries = outputParams.getList("queries");
    assertEquals( outQueries.get(0).getList("working").size(), 10 );
    assertEquals( outQueries.get(1).getList("working").size(), 10 );
    assertEquals( outQueries.get(2).getList("working").size(), 10 );
            
  }
}
