/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.processing;

import java.io.File;
import java.util.Arrays;
import junit.framework.TestCase;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredPassage;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class WorkingSetTest extends TestCase {
  
  File corpus = null;
  File index = null;
  
  public WorkingSetTest(String name) {
    super(name);
  }
  
  @Override
  public void setUp() {
    try {
      corpus = Utility.createTemporary();
      index = Utility.createTemporaryDirectory();
      makeIndex(corpus, index);
    } catch (Exception e) {
      tearDown();
    }
  }
  
  @Override
  public void tearDown() {
    try {
      if (corpus != null) {
        corpus.delete();
      }
      if (index != null) {
        Utility.deleteDirectory(index);
      }
    } catch (Exception e) {
    }
  }
  
  public void testWhiteList() throws Exception {
    Parameters globals = new Parameters();
    globals.set("passageQuery", true);
    LocalRetrieval ret = new LocalRetrieval(index.getAbsolutePath(), globals);
    
    Parameters queryParams = new Parameters();
    queryParams.set("requested", 100);
    queryParams.set("extentQuery", true);
    queryParams.set("extent", "sent");
    queryParams.set("passageSize", 1);
    queryParams.set("passageShift", 1);
    
    Node query = StructuredQuery.parse("#combine( test text 0 1 s0 )");
    query = ret.transformQuery(query, queryParams);
    
    WorkingSetExtentModel model = new WorkingSetExtentModel(ret);
    queryParams.set("working", Arrays.asList(new Integer[]{2, 3, 4, 5, 6, 7, 8, 9, 10, 11}));
    
    ScoredPassage[] results = (ScoredPassage[]) model.execute(query, queryParams);
    
    assertEquals(results.length, 30); // working set * 3
    
    for (int i = 0; i < 10; i++) {
      assertEquals(results[i].document, i + 2);
      assertEquals(results[i].begin, 2);
      assertEquals(results[i].end, 15 + i);
      assertEquals(results[i].score, -4.74, 0.01);
    }


    query = StructuredQuery.parse("#combine( s0 80 )");
    query = ret.transformQuery(query, queryParams);

    queryParams.set("working",
            Arrays.asList(new Integer[]{0, 1, 2, 3, 4, 89, 90, 91, 92, 93}));
    queryParams.set("passageSize", 3);
    queryParams.set("passageShift", 1);
    results = (ScoredPassage[]) model.execute(query, queryParams);

    // first 5 documents do not contain term '80', other 5 contain 3 sentences each
    assertEquals(results.length, 10); 

    for(int i=0; i<5; i++){
//      System.out.println(results[i].toString());
      assertEquals(results[i].document, i + 89);
      assertEquals(results[i].begin, 2);
      assertEquals(results[i].end, 302 + 3*i);
      assertEquals(results[i].score, -5.23, 0.01);
    }
    
    for(int i=0; i<5; i++){
      System.out.println(results[i+5].toString());
      assertEquals(results[i+5].document, i);
      assertEquals(results[i+5].begin, 2);
      assertEquals(results[i+5].end, 35 + 3*i);
      assertEquals(results[i+5].score, -5.25, 0.01);
    }
  }
  
  private void makeIndex(File corpus, File index) throws Exception {
    StringBuilder c = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      StringBuilder data = new StringBuilder();
      // three sent fields
      for (int j = 0; j < 3; j++) {
        data.append("\n<sent> s").append(j);
        for (int k = 0; k < (i + 10); k++) {
          data.append(" ").append(k);
        }
        data.append("</sent>");
      }
      data.append("\n");
      c.append(AppTest.trecDocument("d-" + i, "Test text" + data.toString()));
    }
    Utility.copyStringToFile(c.toString(), corpus);
    
    Parameters p = new Parameters();
    p.set("inputPath", corpus.getAbsolutePath());
    p.set("indexPath", index.getAbsolutePath());
    p.set("corpus", true);
    p.set("tokenizer", new Parameters());
    p.getMap("tokenizer").set("fields", Arrays.asList(new String[]{"sent"}));
    App.run("build", p, System.out);
  }
}
