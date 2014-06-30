// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.structured;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.iterator.ExistentialIndicatorIterator;
import org.lemurproject.galago.core.retrieval.iterator.RequireIterator;
import org.lemurproject.galago.core.retrieval.iterator.UniversalIndicatorIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.File;

import static org.junit.Assert.*;

/**
 *
 * @author irmarc
 */
public class IndicatorIteratorTest {

  File relsFile = null;
  File queryFile = null;
  File scoresFile = null;
  File trecCorpusFile = null;
  File indexFile = null;

  // Build an index based on 10 short docs
  @Before
  public void setUp() throws Exception {

    // create a simple doc file, trec format:
    StringBuilder trecCorpus = new StringBuilder();
    trecCorpus.append(AppTest.trecDocument("0", "This is a sample document"));
    trecCorpus.append(AppTest.trecDocument("1", "If the shoe fits, it's ugly"));
    trecCorpus.append(AppTest.trecDocument("2", "The cat jumped over third sample document"));
    trecCorpus.append(AppTest.trecDocument("3", "To be trusted is a greater compliment than to be loved"));
    trecCorpus.append(AppTest.trecDocument("4", "Though a sample program be but three lines long, someday it will have to be maintained via document."));
    trecCorpusFile = FileUtility.createTemporary();
    Utility.copyStringToFile(trecCorpus.toString(), trecCorpusFile);

    // now, try to build an index from that
    indexFile = FileUtility.createTemporary();
    indexFile.delete();
    App.main(new String[]{"build", "--stemming=false", "--indexPath=" + indexFile.getAbsolutePath(),
      "--inputPath=" + trecCorpusFile.getAbsolutePath()});

    AppTest.verifyIndexStructures(indexFile);
  }

  @After
  public void tearDown() throws Exception {
    if (relsFile != null) {
      relsFile.delete();
    }
    if (queryFile != null) {
      queryFile.delete();
    }
    if (scoresFile != null) {
      scoresFile.delete();
    }
    if (trecCorpusFile != null) {
      trecCorpusFile.delete();
    }
    if (indexFile != null) {
      Utility.deleteDirectory(indexFile);
    }
  }

  @Test
  public void testExistentialIndicator() throws Exception {
    // Create a retrieval object for use by the traversal
    Parameters p = Parameters.instance();
    p.set("retrievalGroup", "all");
    p.set("index", indexFile.getAbsolutePath());
    LocalRetrieval retrieval = (LocalRetrieval) RetrievalFactory.instance(p);

    Node parsedTree = StructuredQuery.parse("#any( #counts:cat:part=postings() #counts:program:part=postings() )");

    ExistentialIndicatorIterator eii = (ExistentialIndicatorIterator) retrieval.createIterator(Parameters.instance(), parsedTree);

    ScoringContext sc = new ScoringContext();

    // initial state
    assertEquals(2, eii.currentCandidate());
    sc.document = 2;
    assertTrue(eii.indicator(sc));
    assertEquals(true, eii.hasMatch(eii.currentCandidate()));

    eii.syncTo(3);
    assertEquals(4, eii.currentCandidate());
    sc.document = 4;
    assertEquals(true, eii.indicator(sc));

    eii.movePast(eii.currentCandidate());
    assertTrue(eii.isDone());

    retrieval.close();
  }

  @Test
  public void testUniversalIndicator() throws Exception {
    // Create a retrieval object for use by the traversal
    Parameters p = Parameters.instance();
    p.set("retrievalGroup", "all");
    p.set("index", indexFile.getAbsolutePath());
    LocalRetrieval retrieval = (LocalRetrieval) RetrievalFactory.instance(p);

    Node parsedTree = StructuredQuery.parse("#all( #counts:document:part=postings() #counts:sample:part=postings() )");
    UniversalIndicatorIterator uii = (UniversalIndicatorIterator) retrieval.createIterator(Parameters.instance(), parsedTree);

    ScoringContext sc = new ScoringContext();

    // initial state
    assertEquals(0, uii.currentCandidate());
    sc.document = 0;
    assertEquals(true, uii.indicator(sc));
    assertEquals(true, uii.hasMatch(uii.currentCandidate()));

    uii.syncTo(1);
    assertEquals(2, uii.currentCandidate());
    sc.document = 2;
    assertEquals(true, uii.indicator(sc));
    assertEquals(true, uii.hasMatch(uii.currentCandidate()));

    uii.movePast(uii.currentCandidate());
    assertEquals(4, uii.currentCandidate());

    uii.movePast(uii.currentCandidate());
    assertTrue(uii.isDone());

    retrieval.close();
  }

  @Test
  public void testIteratorPair() throws Exception {
    // Create a retrieval object for use by the traversal
    Parameters p = Parameters.instance();
    p.set("retrievalGroup", "all");
    p.set("index", indexFile.getAbsolutePath());
    LocalRetrieval retrieval = (LocalRetrieval) RetrievalFactory.instance(p);

    Node existTree = StructuredQuery.parse("#any( #counts:document:part=postings() )");
    ScoringContext sc = new ScoringContext();
    ExistentialIndicatorIterator eii = (ExistentialIndicatorIterator) retrieval.createIterator(Parameters.instance(), existTree);

    Node universeTree = StructuredQuery.parse("#all( #counts:document:part=postings() )");
    UniversalIndicatorIterator uii = (UniversalIndicatorIterator) retrieval.createIterator(Parameters.instance(), universeTree);

    // Initialization
    assertFalse(eii.isDone());
    assertFalse(uii.isDone());
    assertEquals(eii.currentCandidate(), uii.currentCandidate());
    assertEquals(true, eii.hasMatch(eii.currentCandidate()));
    assertEquals(true, uii.hasMatch(uii.currentCandidate()));
    sc.document = uii.currentCandidate();
    assertEquals(true, eii.indicator(sc));
    assertEquals(true, uii.indicator(sc));

    // First step to doc 2
    uii.movePast(uii.currentCandidate());
    eii.movePast(eii.currentCandidate());
    assertEquals(true, eii.hasMatch(eii.currentCandidate()));
    assertEquals(true, uii.hasMatch(uii.currentCandidate()));
    assertEquals(eii.currentCandidate(), uii.currentCandidate());
    sc.document = uii.currentCandidate();
    assertEquals(true, eii.indicator(sc));
    assertEquals(true, uii.indicator(sc));

    // Now on the doc 4
    uii.movePast(uii.currentCandidate());
    eii.movePast(eii.currentCandidate());
    assertEquals(true, eii.hasMatch(eii.currentCandidate()));
    assertEquals(true, uii.hasMatch(uii.currentCandidate()));
    assertEquals(eii.currentCandidate(), uii.currentCandidate());
    sc.document = uii.currentCandidate();
    assertEquals(true, eii.indicator(sc));
    assertEquals(true, uii.indicator(sc));

    // Should be done now
    uii.movePast(uii.currentCandidate());
    eii.movePast(eii.currentCandidate());
    assertTrue(uii.isDone());
    assertTrue(eii.isDone());
  }

  @Test
  public void testComplexIterator() throws Exception {
    // Create a retrieval object for use by the traversal
    Parameters p = Parameters.instance();
    p.set("index", indexFile.getAbsolutePath());
    LocalRetrieval retrieval = (LocalRetrieval) RetrievalFactory.instance(p);

    Node root = StructuredQuery.parse("#require(#all( #counts:document:part=postings() ) #counts:document:part=postings() )");
    root = retrieval.transformQuery(root, p);

    ScoringContext dc1 = new ScoringContext();
    RequireIterator mi = (RequireIterator) retrieval.createIterator(Parameters.instance(), root);

    assertEquals(0, mi.currentCandidate());
    dc1.document = 0;
    assertFalse(mi.isDone());

    mi.movePast(mi.currentCandidate());
    dc1.document = 2;
    assertEquals(2, mi.currentCandidate());
    assertFalse(mi.isDone());

    mi.movePast(mi.currentCandidate());
    dc1.document = 4;
    assertEquals(4, mi.currentCandidate());
    assertFalse(mi.isDone());

    mi.movePast(mi.currentCandidate());
    assertTrue(mi.isDone());
  }
}
