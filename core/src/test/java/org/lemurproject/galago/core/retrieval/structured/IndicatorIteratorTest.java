// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.structured;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.iterator.RequireIterator;
import org.lemurproject.galago.core.retrieval.iterator.ExistentialIndicatorIterator;
import org.lemurproject.galago.core.retrieval.iterator.UniversalIndicatorIterator;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import java.io.File;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author irmarc
 */
public class IndicatorIteratorTest extends TestCase {

  File relsFile = null;
  File queryFile = null;
  File scoresFile = null;
  File trecCorpusFile = null;
  File indexFile = null;

  public IndicatorIteratorTest(String testName) {
    super(testName);
  }

  // Build an index based on 10 short docs
  @Override
  public void setUp() throws Exception {

    // create a simple doc file, trec format:
    StringBuilder trecCorpus = new StringBuilder();
    trecCorpus.append(AppTest.trecDocument("0", "This is a sample document"));
    trecCorpus.append(AppTest.trecDocument("1", "If the shoe fits, it's ugly"));
    trecCorpus.append(AppTest.trecDocument("2", "The cat jumped over third sample document"));
    trecCorpus.append(AppTest.trecDocument("3", "To be trusted is a greater compliment than to be loved"));
    trecCorpus.append(AppTest.trecDocument("4", "Though a sample program be but three lines long, someday it will have to be maintained via document."));
    trecCorpusFile = Utility.createTemporary();
    Utility.copyStringToFile(trecCorpus.toString(), trecCorpusFile);

    // now, try to build an index from that
    indexFile = Utility.createTemporary();
    indexFile.delete();
    App.main(new String[]{"build", "--stemming=false", "--indexPath=" + indexFile.getAbsolutePath(),
              "--inputPath=" + trecCorpusFile.getAbsolutePath()});

    AppTest.verifyIndexStructures(indexFile);
  }

  @Override
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

  public void testExistentialIndicator() throws Exception {
    // Create a retrieval object for use by the traversal
    Parameters p = new Parameters();
    p.set("retrievalGroup", "all");
    p.set("index", indexFile.getAbsolutePath());
    LocalRetrieval retrieval = (LocalRetrieval) RetrievalFactory.instance(p);

    Node parsedTree = StructuredQuery.parse("#any( #counts:cat:part=postings() #counts:program:part=postings() )");
    ScoringContext context = new ScoringContext();

    ExistentialIndicatorIterator eii = (ExistentialIndicatorIterator) retrieval.createIterator(parsedTree, context);

    // initial state
    assertEquals(2, eii.currentCandidate());
    assertTrue(eii.atCandidate(2));
    assertEquals(true, eii.atCandidate(eii.currentCandidate()));

    eii.moveTo(3);
    assertEquals(4, eii.currentCandidate());
    assertEquals(true, eii.atCandidate(eii.currentCandidate()));

    eii.next();
    assertTrue(eii.isDone());

    retrieval.close();
  }

  public void testUniversalIndicator() throws Exception {
    // Create a retrieval object for use by the traversal
    Parameters p = new Parameters();
    p.set("retrievalGroup", "all");
    p.set("index", indexFile.getAbsolutePath());
    LocalRetrieval retrieval = (LocalRetrieval) RetrievalFactory.instance(p);

    Node parsedTree = StructuredQuery.parse("#all( #counts:document:part=postings() #counts:sample:part=postings() )");
    ScoringContext context = new ScoringContext();
    UniversalIndicatorIterator uii = (UniversalIndicatorIterator) retrieval.createIterator(parsedTree, context);

    // initial state
    assertEquals(0, uii.currentCandidate());
    assertTrue(uii.atCandidate(0));
    assertEquals(true, uii.atCandidate(uii.currentCandidate()));

    uii.moveTo(1);
    assertEquals(2, uii.currentCandidate());
    assertEquals(true, uii.atCandidate(uii.currentCandidate()));

    uii.next();
    assertEquals(4, uii.currentCandidate());

    uii.next();
    assertTrue(uii.isDone());

    retrieval.close();
  }

  public void testIteratorPair() throws Exception {
    // Create a retrieval object for use by the traversal
    Parameters p = new Parameters();
    p.set("retrievalGroup", "all");
    p.set("index", indexFile.getAbsolutePath());
    LocalRetrieval retrieval = (LocalRetrieval) RetrievalFactory.instance(p);

    Node existTree = StructuredQuery.parse("#any( #counts:document:part=postings() )");
    ScoringContext dc1 = new ScoringContext();
    ExistentialIndicatorIterator eii = (ExistentialIndicatorIterator) retrieval.createIterator(existTree,
            dc1);

    Node universeTree = StructuredQuery.parse("#all( #counts:document:part=postings() )");
    UniversalIndicatorIterator uii = (UniversalIndicatorIterator) retrieval.createIterator(universeTree, dc1);

    // Initialization
    assertFalse(eii.isDone());
    assertFalse(uii.isDone());
    assertEquals(eii.currentCandidate(), uii.currentCandidate());
    assertEquals(true, eii.atCandidate(eii.currentCandidate()));
    assertEquals(true, uii.atCandidate(uii.currentCandidate()));

    // First step to doc 2
    uii.next();
    eii.next();
    assertEquals(true, eii.atCandidate(eii.currentCandidate()));
    assertEquals(true, uii.atCandidate(uii.currentCandidate()));
    assertEquals(eii.currentCandidate(), uii.currentCandidate());

    // Now on the doc 4
    uii.next();
    eii.next();
    assertEquals(true, eii.atCandidate(eii.currentCandidate()));
    assertEquals(true, uii.atCandidate(uii.currentCandidate()));
    assertEquals(eii.currentCandidate(), uii.currentCandidate());

    // Should be done now
    uii.next();
    eii.next();
    assertTrue(uii.isDone());
    assertTrue(eii.isDone());
  }

  public void testComplexIterator() throws Exception {
    // Create a retrieval object for use by the traversal
    Parameters p = new Parameters();
    p.set("index", indexFile.getAbsolutePath());
    LocalRetrieval retrieval = (LocalRetrieval) RetrievalFactory.instance(p);

    Node root = StructuredQuery.parse("#require(#all( #counts:document:part=postings() ) #counts:document:part=postings() )");
    root = retrieval.transformQuery(root, p);

    ScoringContext dc1 = new ScoringContext();
    RequireIterator mi = (RequireIterator) retrieval.createIterator(root, dc1);

    assertEquals(0, mi.currentCandidate());
    dc1.document = 0;
    assertFalse(mi.isDone());

    mi.next();
    dc1.document = 2;
    assertEquals(2, mi.currentCandidate());
    assertFalse(mi.isDone());

    mi.next();
    dc1.document = 4;
    assertEquals(4, mi.currentCandidate());
    assertFalse(mi.isDone());

    mi.next();
    assertTrue(mi.isDone());
  }
}
