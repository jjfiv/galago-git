/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.processing;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredPassage;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author sjh
 */
public class WorkingSetTest {
  
  File corpus = null;
  File index = null;
  
  @Before
  public void setUp() throws Exception {
    corpus = FileUtility.createTemporary();
    index = FileUtility.createTemporaryDirectory();
    makeIndex(corpus, index);
  }
  
  @After
  public void tearDown() throws IOException {
    if (corpus != null) {
      corpus.delete();
    }
    if (index != null) {
      FSUtil.deleteDirectory(index);
    }
  }

  @Test
  public void testWhiteList() throws Exception {
    Parameters globals = Parameters.create();
    globals.set("passageQuery", true);
    LocalRetrieval ret = new LocalRetrieval(index.getAbsolutePath(), globals);
    
    Parameters queryParams = Parameters.create();
    queryParams.set("requested", 100);
    queryParams.set("extentQuery", true);
    queryParams.set("extent", "sent");
    queryParams.set("extentCount", 1);
    queryParams.set("extentShift", 1);
    
    Node query = StructuredQuery.parse("#combine( test text 0 1 s0 )");
    query = ret.transformQuery(query, queryParams);
    
    WorkingSetExtentModel model = new WorkingSetExtentModel(ret);
    queryParams.set("working", Arrays.asList(new Long[]{2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l, 10l, 11l}));
    
    ScoredPassage[] results = (ScoredPassage[]) model.execute(query, queryParams);
    
    assertEquals(results.length, 30); // working set * 3
    
    for (int i = 0; i < 10; i++) {
      assertEquals(results[i].document, i + 2);
      assertEquals(results[i].begin, 2);
      assertEquals(results[i].end, 15 + i);
      assertEquals(results[i].score, -4.74, 0.01);
    }

    // QUERY 2 ; 3:1
    query = StructuredQuery.parse("#combine( s0 80 )");
    query = ret.transformQuery(query, queryParams);
    
    queryParams.set("working",
            Arrays.asList(new Long[]{0l, 1l, 2l, 3l, 4l, 89l, 90l, 91l, 92l, 93l}));
    queryParams.set("extentCount", 3);
    queryParams.set("extentShift", 1);
    results = (ScoredPassage[]) model.execute(query, queryParams);

    // first 5 documents do not contain term '80', other 5 contain 3 sentences each
    assertEquals(results.length, 10);    
    
    for (int i = 0; i < 5; i++) {
//      System.out.println(results[i].toString());
      assertEquals(results[i].document, i + 89);
      assertEquals(results[i].begin, 2);
      assertEquals(results[i].end, 302 + 3 * i);
      assertEquals(results[i].score, -5.23, 0.01);
    }
    
    for (int i = 0; i < 5; i++) {
      //System.out.println(results[i + 5].toString());
      assertEquals(results[i + 5].document, i);
      assertEquals(results[i + 5].begin, 2);
      assertEquals(results[i + 5].end, 35 + 3 * i);
      assertEquals(results[i + 5].score, -5.25, 0.01);
    }

    // QUERY 3; 2:2 (overhang)
    query = StructuredQuery.parse("#combine( s0 s1 s2 2 3 )");
    query = ret.transformQuery(query, queryParams);
    
    queryParams.set("working", Arrays.asList(new Long[]{2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l, 10l, 11l}));
    queryParams.set("extentCount", 2);
    queryParams.set("extentShift", 2);
    results = (ScoredPassage[]) model.execute(query, queryParams);
    
    assertEquals(results.length, 20);
    
    for (int i = 0; i < 10; i++) {
      assertEquals(results[i].document, i + 2);
      assertEquals(results[i].begin, 2);
      assertEquals(results[i].end, 28 + 2 * i);
      
      assertEquals(results[i + 10].document, i + 2);
      assertEquals(results[i + 10].begin, results[i].end);
      assertEquals(results[i + 10].end, 41 + 3 * i);
    }
  }

  private static void makeIndex(File corpus, File index) throws Exception {
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
    
    Parameters p = Parameters.create();
    p.set("inputPath", corpus.getAbsolutePath());
    p.set("indexPath", index.getAbsolutePath());
    p.set("corpus", true);
    p.set("tokenizer", Parameters.create());
    p.getMap("tokenizer").set("fields", Arrays.asList(new String[]{"sent"}));
    App.run("build", p, System.out);
  }
}
