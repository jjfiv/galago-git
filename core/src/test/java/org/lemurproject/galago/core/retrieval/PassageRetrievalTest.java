/*
 * BSD License (http://lemurproject.org/galago-license)

 */
package org.lemurproject.galago.core.retrieval;

import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredPassage;
import java.io.File;
import java.io.IOException;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author irmarc
 */
public class PassageRetrievalTest extends TestCase {

  File trecCorpusFile, corpusFile, indexFile;

  public PassageRetrievalTest(String testName) {
    super(testName);
  }

  @Override
  public void setUp() throws Exception {
    File[] files = LocalRetrievalTest.make10DocIndex();
    trecCorpusFile = files[0];
    corpusFile = files[1];
    indexFile = files[2];
  }

  @Override
  public void tearDown() throws IOException {
    trecCorpusFile.delete();
    Utility.deleteDirectory(indexFile);
    Utility.deleteDirectory(corpusFile);
  }

  public void testPassageRetrieval() throws Exception {
    Parameters p = new Parameters();
    p.set("passageSize", 4);
    p.set("passageShift", 2);

    LocalRetrieval retrieval = new LocalRetrieval(indexFile.toString(), p);

    String query = "#combine( cat document )";
    Node root = StructuredQuery.parse(query);
    root = retrieval.transformQuery(root, p);

    p.set("requested", 10);
    System.out.printf("Query: %s\n", root.toString());
    ScoredPassage[] result = (ScoredPassage[]) retrieval.runQuery(root, p);
    assertEquals(6, result.length);
        
    // First entry
    assertEquals("9", result[0].documentName);
    assertEquals(0, result[0].begin);
    assertEquals(4, result[0].end);
    assertEquals(result[0].score, -2.623394, 0.001);
    
    // Second entry
    assertEquals("8", result[1].documentName);
    assertEquals(2, result[1].begin);
    assertEquals(6, result[1].end);
    assertEquals(result[1].score, -2.624723, 0.001);
    
    // Third entry
    assertEquals("8", result[2].documentName);
    assertEquals(0, result[2].begin);
    assertEquals(4, result[2].end);
    assertEquals(result[2].score, -2.624723, 0.001);
    
    // Fourth entry
    assertEquals("1", result[3].documentName);
    assertEquals(0, result[3].begin);
    assertEquals(4, result[3].end);
    assertEquals(result[3].score, -2.637740, 0.001);
    
    // Fifth entry
    assertEquals("2", result[4].documentName);
    assertEquals(2, result[4].begin);
    assertEquals(6, result[4].end);
    assertEquals(result[4].score, -2.638404, 0.001);
    
    // Sixth entry
    assertEquals("2", result[5].documentName);
    assertEquals(0, result[5].begin);
    assertEquals(4, result[5].end);
    assertEquals(result[5].score, -2.638404, 0.001);
  }
}
