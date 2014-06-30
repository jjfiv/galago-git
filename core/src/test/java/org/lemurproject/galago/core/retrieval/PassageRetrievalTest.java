/*
 * BSD License (http://lemurproject.org/galago-license)

 */
package org.lemurproject.galago.core.retrieval;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author irmarc
 */
public class PassageRetrievalTest {

  File trecCorpusFile, corpusFile, indexFile;

  @Before
  public void setUp() throws Exception {
    File[] files = LocalRetrievalTest.make10DocIndex();
    trecCorpusFile = files[0];
    corpusFile = files[1];
    indexFile = files[2];
  }

  @After
  public void tearDown() throws IOException {
    trecCorpusFile.delete();
    Utility.deleteDirectory(indexFile);
    Utility.deleteDirectory(corpusFile);
  }

  @Test
  public void testPassageRetrieval() throws Exception {
    Parameters p = Parameters.instance();
    p.set("passageSize", 4);
    p.set("passageShift", 2);
    p.set("passageQuery", true);

    LocalRetrieval retrieval = new LocalRetrieval(indexFile.toString(), p);

    String query = "#combine( cat document )";
    Node root = StructuredQuery.parse(query);
    root = retrieval.transformQuery(root, p);
    
    p.set("requested", 10);
    List<ScoredDocument> results = retrieval.executeQuery(root, p).scoredDocuments;
    
    assertEquals(7, results.size());

    ScoredPassage psg;
    // First entry
    psg = (ScoredPassage)results.get(0);
    assertEquals("9", psg.documentName);
    assertEquals(0, psg.begin);
    assertEquals(4, psg.end);
    assertEquals(psg.score, -2.62339379, 0.00001);

    // Second entry
    psg = (ScoredPassage)results.get(1);
    assertEquals("8", psg.documentName);
    assertEquals(2, psg.begin);
    assertEquals(6, psg.end);
    assertEquals(psg.score, -2.62791286, 0.00001);

    // Third entry
    psg = (ScoredPassage)results.get(2);
    assertEquals("8", psg.documentName);
    assertEquals(0, psg.begin);
    assertEquals(4, psg.end);
    assertEquals(psg.score, -2.63247316, 0.00001);

    // Fourth entry
    psg = (ScoredPassage)results.get(3);
    assertEquals("1", psg.documentName);
    assertEquals(2, psg.begin);
    assertEquals(5, psg.end);
    assertEquals(psg.score, -2.63641031, 0.00001);

    // Fifth entry
    psg = (ScoredPassage)results.get(4);
    assertEquals("2", psg.documentName);
    assertEquals(0, psg.begin);
    assertEquals(4, psg.end);
    assertEquals(psg.score, -2.63707542, 0.00001);
    
    // final 2 entries are background scores (no extents to score)
    
  }

}
