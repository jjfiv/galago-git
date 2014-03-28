/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.tools;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.lemurproject.galago.contrib.util.TestingUtils.trecDocument;

/**
 *
 * @author sjh
 */
public class GenerateWorkingSetQueriesTest {
  private File dataFolder;
  private File index;

  @Before
  public void setUp() throws Exception {
    dataFolder = FileUtility.createTemporaryDirectory();

    File corpus = new File(dataFolder, "corpus.trectext");
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

  @After
  public void tearDown() throws IOException {
    if (dataFolder != null) {
      Utility.deleteDirectory(dataFolder);
    }
  }

  @Test
  public void testSomeMethod() throws Exception {

    File out = new File(dataFolder, "out1");

    Parameters p = new Parameters();
    p.set("index", index.getAbsolutePath());
    p.set("topK", 10);
    p.set("output", out.getAbsolutePath());
    p.set("wsNumders", true);
    ArrayList<Parameters> qs = new ArrayList<Parameters>();
    qs.add(Parameters.parseString("{\"number\" : \"q1\", \"text\" : \"#combine(t1 t2)\"}"));
    qs.add(Parameters.parseString("{\"number\" : \"q2\", \"text\" : \"#combine(t3 t4)\"}"));
    qs.add(Parameters.parseString("{\"number\" : \"q3\", \"text\" : \"#combine(t5 t6)\"}"));
    p.set("queries", qs);

    GenerateWorkingSetQueries generator = new GenerateWorkingSetQueries();
    generator.run(p, System.err);

    Parameters outputParams = Parameters.parseFile(out);

    List<Parameters> outQueries = outputParams.getList("queries", Parameters.class);
    assertEquals( outQueries.get(0).getList("working").size(), 10 );
    assertEquals( outQueries.get(1).getList("working").size(), 10 );
    assertEquals( outQueries.get(2).getList("working").size(), 10 );
            
  }
}
