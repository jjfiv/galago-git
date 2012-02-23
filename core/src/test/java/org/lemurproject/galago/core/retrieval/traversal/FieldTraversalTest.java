// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.disk.DiskLengthsWriter;
import org.lemurproject.galago.core.index.disk.DiskNameWriter;
import org.lemurproject.galago.core.index.disk.PositionIndexWriter;
import org.lemurproject.galago.core.index.disk.WindowIndexWriter;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Tests for correctness of field-based retrieval models.
 * Currently this consists of BM25F, PRMS, PL2F
 * 
 * For each model we test:
 * - the transformation correctness of model.
 * - the correctness of the math involved in the model.
 *
 * @author irmarc
 */
public class FieldTraversalTest extends TestCase {

  File indexPath;

  public FieldTraversalTest(String testName) {
    super(testName);
  }

  @Override
  public void setUp() throws FileNotFoundException, IOException {
    indexPath = makeFieldIndexes();
  }

  @Override
  public void tearDown() throws IOException {
    Utility.deleteDirectory(indexPath);
  }

  // We pull statistics directly from the index to make sure they are
  // generated correctly.
  public void testPRMSTraversalCorrectness() throws Exception {
    DiskIndex index = new DiskIndex(indexPath.getAbsolutePath());

    // set fields
    String[] fields = {"title", "author", "anchor"};
    Parameters global = new Parameters();
    global.set("fields", Arrays.asList(fields));

    LocalRetrieval retrieval = new LocalRetrieval(index, global);
    PRMS2Traversal traversal = new PRMS2Traversal(retrieval);
    Node q1 = StructuredQuery.parse("#prms2(#text:cat() #text:dog() #text:donkey())");
    Node q2 = StructuredQuery.copy(traversal, q1);

    StringBuilder transformed = new StringBuilder();
    transformed.append("#combine:norm=false( ");
    transformed.append("#feature:log ( #combine:0=0.3333333333333333:1=0.3333333333333333:2=0.3333333333333333 ( ");
    transformed.append("#feature:dirichlet-raw:lengths=title( #counts:cat:part=field.title() ) ");
    transformed.append("#feature:dirichlet-raw:lengths=author( #counts:cat:part=field.author() ) ");
    transformed.append("#feature:dirichlet-raw:lengths=anchor( #counts:cat:part=field.anchor() ) ) ) ");
    transformed.append("#feature:log ( #combine:0=0.3333333333333333:1=0.3333333333333333:2=0.3333333333333333 ( ");
    transformed.append("#feature:dirichlet-raw:lengths=title( #counts:dog:part=field.title() ) ");
    transformed.append("#feature:dirichlet-raw:lengths=author( #counts:dog:part=field.author() ) ");
    transformed.append("#feature:dirichlet-raw:lengths=anchor( #counts:dog:part=field.anchor() ) ) ) ");
    transformed.append("#feature:log ( #combine:0=0.3333333333333333:1=0.3333333333333333:2=0.3333333333333333 ( ");
    transformed.append("#feature:dirichlet-raw:lengths=title( #counts:donkey:part=field.title() ) ");
    transformed.append("#feature:dirichlet-raw:lengths=author( #counts:donkey:part=field.author() ) ");
    transformed.append("#feature:dirichlet-raw:lengths=anchor( #counts:donkey:part=field.anchor() ) ) ) ");
    transformed.append(" )");

    Node expected = StructuredQuery.parse(transformed.toString());
    //System.err.printf("Expected: %s\nGot: %s\n", expected.toString(), q2.toString() );
    assertEquals(expected.toString(), q2.toString());
  }

  public void testBM25FTraversalCorrectness() throws Exception {
    DiskIndex index = new DiskIndex(indexPath.getAbsolutePath());

    Parameters wMap = new Parameters();
    Parameters bMap = new Parameters();
    Parameters fMap = new Parameters();

    fMap.set("weights", wMap);
    fMap.set("smoothing", bMap);
    fMap.set("K", 1.2);

    wMap.set("anchor", 3.7);

    // Set smoothing per field
    bMap.set("title", 0.3);
    bMap.set("author", 0.8);

    // Now set it to the retrieval
    Parameters p = new Parameters();
    p.set("bm25f", fMap);

    String[] fields = {"title", "author", "anchor"};
    p.set("fields", Arrays.asList(fields));
    LocalRetrieval retrieval = new LocalRetrieval(index, p);

    BM25FTraversal traversal = new BM25FTraversal(retrieval);
    Node q1 = StructuredQuery.parse("#bm25f(#text:cat() #text:dog() #text:donkey())");
    Node q2 = StructuredQuery.copy(traversal, q1);

    StringBuilder transformed = new StringBuilder();

    transformed.append("#bm25fcomb:norm=false:K=1.2( ");
    transformed.append("#combine:2=3.7:1=0.5:0=0.5:norm=false( #feature:bm25f:b=0.3:lengths=title( #extents:cat:part=field.title() ) ");
    transformed.append("#feature:bm25f:b=0.8:lengths=author( #extents:cat:part=field.author() ) ");
    transformed.append("#feature:bm25f:b=0.5:lengths=anchor( #extents:cat:part=field.anchor() ) ) ");
    transformed.append("#feature:idf( #extents:cat:part=postings() ) ");
    transformed.append("#combine:2=3.7:1=0.5:0=0.5:norm=false( #feature:bm25f:b=0.3:lengths=title( #extents:dog:part=field.title() ) ");
    transformed.append("#feature:bm25f:b=0.8:lengths=author( #extents:dog:part=field.author() ) ");
    transformed.append("#feature:bm25f:b=0.5:lengths=anchor( #extents:dog:part=field.anchor() ) )");
    transformed.append("#feature:idf( #extents:dog:part=postings() ) ");
    transformed.append("#combine:2=3.7:1=0.5:0=0.5:norm=false( #feature:bm25f:b=0.3:lengths=title( #extents:donkey:part=field.title() ) ");
    transformed.append("#feature:bm25f:b=0.8:lengths=author( #extents:donkey:part=field.author() ) ");
    transformed.append("#feature:bm25f:b=0.5:lengths=anchor( #extents:donkey:part=field.anchor() ) ) ");
    transformed.append("#feature:idf( #extents:donkey:part=postings() ) ");
    transformed.append(" )");

    Node expected = StructuredQuery.parse(transformed.toString());
    assertEquals(expected.toString(), q2.toString());
  }

  public void testPL2FTraversalCorrectness() throws Exception {
    DiskIndex index = new DiskIndex(indexPath.getAbsolutePath());

    Parameters wMap = new Parameters();
    Parameters fMap = new Parameters();

    fMap.set("weights", wMap);

    wMap.set("anchor", 0.7);
    wMap.set("title", 0.3);

    // Now set it to the retrieval
    Parameters p = new Parameters();
    p.set("pl2f", fMap);

    String[] fields = {"title", "author", "anchor"};
    p.set("fields", Arrays.asList(fields));

    LocalRetrieval retrieval = new LocalRetrieval(index, p);

    PL2FTraversal traversal = new PL2FTraversal(retrieval);
    Node q1 = StructuredQuery.parse("#pl2f(#text:cat() #text:dog() #text:donkey())");
    Node q2 = StructuredQuery.copy(traversal, q1);

    StringBuilder transformed = new StringBuilder();

    transformed.append("#combine:norm=false( ");
    transformed.append("#feature:dfr:qf=1:qfmax=1:nodeFrequency=13:documentCount=5( ");
    transformed.append("#combine:2=0.7:1=0.5:0=0.3( ");
    transformed.append("#feature:pl2f:c=0.5:lengths=title( #counts:cat:part=field.title() ) ");
    transformed.append("#feature:pl2f:c=0.5:lengths=author( #counts:cat:part=field.author() ) ");
    transformed.append("#feature:pl2f:c=0.5:lengths=anchor( #counts:cat:part=field.anchor() ) ) ) ");
    transformed.append("#feature:dfr:qf=1:qfmax=1:nodeFrequency=11:documentCount=5( ");
    transformed.append("#combine:2=0.7:1=0.5:0=0.3( ");
    transformed.append("#feature:pl2f:c=0.5:lengths=title( #counts:dog:part=field.title() ) ");
    transformed.append("#feature:pl2f:c=0.5:lengths=author( #counts:dog:part=field.author() ) ");
    transformed.append("#feature:pl2f:c=0.5:lengths=anchor( #counts:dog:part=field.anchor() ) ) ) ");
    transformed.append("#feature:dfr:qf=1:qfmax=1:nodeFrequency=12:documentCount=5(");
    transformed.append("#combine:2=0.7:1=0.5:0=0.3( ");
    transformed.append("#feature:pl2f:c=0.5:lengths=title( #counts:donkey:part=field.title() ) ");
    transformed.append("#feature:pl2f:c=0.5:lengths=author( #counts:donkey:part=field.author() ) ");
    transformed.append("#feature:pl2f:c=0.5:lengths=anchor( #counts:donkey:part=field.anchor() ) ) ) ");
    transformed.append(" )");

    Node expected = StructuredQuery.parse(transformed.toString());
    //System.err.printf("Expected: %s\n\nGot: %s\n", expected.toString(), q2.toString() );
    assertEquals(expected.toString(), q2.toString());
  }

  public void testBM25FDeltaScoringVsModel() throws Exception {
    DiskIndex index = new DiskIndex(indexPath.getAbsolutePath());

    Parameters wMap = new Parameters();
    Parameters bMap = new Parameters();
    Parameters fMap = new Parameters();

    fMap.set("weights", wMap);
    fMap.set("smoothing", bMap);
    fMap.set("K", 1.2);

    wMap.set("anchor", 3.7);

    // Set smoothing per field
    bMap.set("title", 0.3);
    bMap.set("author", 0.8);

    // Now set it to the retrieval
    Parameters p = new Parameters();
    p.set("bm25f", fMap);

    String[] fields = {"title", "author", "anchor"};
    p.set("fields", Arrays.asList(fields));
  
    LocalRetrieval retrieval = new LocalRetrieval(index, p);
    String query = "#bm25f(cat dog donkey)";
    ScoredDocument[] results = retrieval.runQuery(query, p);

    //p.set("delta", true);
    ScoredDocument[] results2 = retrieval.runQuery(query, p);

    assertEquals(results.length, results2.length);
    for (int i = 0; i < results.length; i++) {
      assertEquals(results[i].document, results2[i].document);
      assertEquals(results[i].score, results2[i].score, 0.00001);
    }
  }

  public void testPRMSDeltaScoringVsModel() throws Exception {
    DiskIndex index = new DiskIndex(indexPath.getAbsolutePath());

    // set fields
    String[] fields = {"title", "author", "anchor"};
    Parameters global = new Parameters();
    global.set("fields", Arrays.asList(fields));

    String query = "#prms2(cat dog donkey)";

    LocalRetrieval retrieval = new LocalRetrieval(index, global);
    ScoredDocument[] results = retrieval.runQuery(query, global);

    //global.set("delta", true);
    ScoredDocument[] results2 = retrieval.runQuery(query, global);

    assertEquals(results.length, results2.length);

    /*
    for (int i = 0; i < results2.length; i++) {
    System.err.printf("%d : %s vs %s\n", i, results[i].toString(), results2[i].toString());
    }
     */

    for (int i = 0; i < results.length; i++) {
      assertEquals(results[i].document, results2[i].document);
      assertEquals(results[i].score, results2[i].score, 0.00001);
    }

  }

 public void testPL2FDeltaScoringVsModel() throws Exception {
    DiskIndex index = new DiskIndex(indexPath.getAbsolutePath());

    Parameters wMap = new Parameters();
    Parameters fMap = new Parameters();

    fMap.set("weights", wMap);

    wMap.set("anchor", 0.7);
    wMap.set("title", 0.3);

    // Now set it to the retrieval
    Parameters p = new Parameters();
    p.set("pl2f", fMap);

    String[] fields = {"title", "author", "anchor"};
    p.set("fields", Arrays.asList(fields));

    String query = "#pl2f(cat dog donkey)";
    LocalRetrieval retrieval = new LocalRetrieval(index, p);
    ScoredDocument[] results = retrieval.runQuery(query, p);

    //p.set("delta", true);
    ScoredDocument[] results2 = retrieval.runQuery(query, p);

    assertEquals(results.length, results2.length);

    /*
    for (int i = 0; i < results2.length; i++) {
    System.err.printf("%d : %s vs %s\n", i, results[i].toString(), results2[i].toString());
    } 
     */

    for (int i = 0; i < results.length; i++) {
      assertEquals(results[i].document, results2[i].document);
      assertEquals(results[i].score, results2[i].score, 0.00001);
    }
 }
  
  public void testPRMSModelCorrectness() throws Exception {
    DiskIndex index = new DiskIndex(indexPath.getAbsolutePath());

    // set fields
    String[] fields = {"title", "author", "anchor"};
    Parameters global = new Parameters();
    global.set("fields", Arrays.asList(fields));

    LocalRetrieval retrieval = new LocalRetrieval(index, global);
    ScoredDocument[] results = retrieval.runQuery("#prms2(cat dog donkey)", global);

    assertEquals(5, results.length);

    /**
    for (int i = 0; i < results.length; i++) {
    System.err.printf("%d : %s\n", i, results[i].toString());
    }
    */
     

    assertEquals(1, results[0].document);
    assertEquals(results[0].score, -11.160840, 0.00001);
    assertEquals(2, results[1].document);
    assertEquals(results[1].score, -11.172624, 0.00001);
    assertEquals(5, results[2].document);
    assertEquals(results[2].score, -11.189912, 0.00001);
    assertEquals(4, results[3].document);
    assertEquals(results[3].score, -11.231324, 0.00001);
    assertEquals(3, results[4].document);
    assertEquals(results[4].score, -11.240375, 0.00001);

  }

  public void testBM25FModelCorrectness() throws Exception {
    DiskIndex index = new DiskIndex(indexPath.getAbsolutePath());

    Parameters wMap = new Parameters();
    Parameters bMap = new Parameters();
    Parameters fMap = new Parameters();

    fMap.set("weights", wMap);
    fMap.set("smoothing", bMap);
    fMap.set("K", 1.2);

    wMap.set("anchor", 3.7);

    // Set smoothing per field
    bMap.set("title", 0.3);
    bMap.set("author", 0.8);

    // Now set it to the retrieval
    Parameters p = new Parameters();
    p.set("bm25f", fMap);

    // Set fields too
    String[] fields = {"title", "author", "anchor"};
    p.set("fields", Arrays.asList(fields));

    LocalRetrieval retrieval = new LocalRetrieval(index, p);
    ScoredDocument[] results = retrieval.runQuery("#bm25f(cat dog donkey)", p);

    /** for debugging...the test...
    for (int i = 0; i < results.length; i++) {
    System.err.printf("%d : %s\n", i, results[i].toString());
    }
     */
    
    // Verify our results
    assertEquals(5, results.length);

    assertEquals(1, results[0].document);
    assertEquals(results[0].score, 0.758854, 0.00001);
    assertEquals(5, results[1].document);
    assertEquals(results[1].score, 0.755744, 0.00001);
    assertEquals(2, results[2].document);
    assertEquals(results[2].score, 0.428942, 0.00001);
    assertEquals(4, results[3].document);
    assertEquals(results[3].score, 0.341049, 0.00001);
    assertEquals(3, results[4].document);
    assertEquals(results[4].score, 0.096271, 0.00001);

  }

  public void testPL2FModelCorrectness() throws Exception {
    DiskIndex index = new DiskIndex(indexPath.getAbsolutePath());

    Parameters wMap = new Parameters();
    Parameters fMap = new Parameters();

    fMap.set("weights", wMap);

    wMap.set("anchor", 0.7);
    wMap.set("title", 0.3);

    // Now set it to the retrieval
    Parameters p = new Parameters();
    p.set("pl2f", fMap);

    String[] fields = {"title", "author", "anchor"};
    p.set("fields", Arrays.asList(fields));

    LocalRetrieval retrieval = new LocalRetrieval(index, p);
    ScoredDocument[] results = retrieval.runQuery("#pl2f(cat dog donkey)", p);

    assertEquals(5, results.length);

    /*
    for (int i = 0; i < results.length; i++) {
    System.err.printf("%d : %s\n", i, results[i].toString());
    }
    */
    
    assertEquals(5, results[0].document);
    assertEquals(results[0].score, 2.294759, 0.00001);
    assertEquals(1, results[1].document);
    assertEquals(results[1].score, 2.291694, 0.00001);
    assertEquals(2, results[2].document);
    assertEquals(results[2].score, -0.359963, 0.00001);
    assertEquals(3, results[3].document);
    assertEquals(results[3].score, -3.088521, 0.00001);
    assertEquals(4, results[4].document);
    assertEquals(results[4].score, -3.462125, 0.00001);

  }

  public static void addEntries(PositionIndexWriter writer, String term, int[][] entries) throws IOException {
    writer.processWord(Utility.fromString(term));
    for (int[] plist : entries) {
      writer.processDocument(plist[0]);
      for (int i = 1; i < plist.length; i++) {
        writer.processPosition(plist[i]);
      }
    }
  }
  int[][] catEntries = {
    {1, 5, 23, 78, 112},
    {4, 3, 12, 48, 100, 109},
    {5, 19, 45, 66, 89}
  };
  int[][] dogEntries = {
    {1, 6, 27, 79, 118},
    {2, 1, 5, 19},
    {5, 3, 18, 46, 90}
  };
  int[][] donkeyEntries = {
    {1, 7, 30},
    {2, 2, 10, 20, 24, 25},
    {3, 1, 7},
    {5, 4, 47, 91}
  };

  public File makeFieldIndexes() throws IOException {
    // make a spot for the index
    File tempPath = Utility.createTemporaryDirectory();

    // put in a generic manifest
    new Parameters().write(tempPath + File.separator + "manifest");

    // build the title index
    Parameters extp = new Parameters();
    extp.set("statistics/collectionLength", 500);
    extp.set("statistics/documentCount", 5);
    extp.set("statistics/vocabCount", 3);
    extp.set("filename", tempPath + File.separator + "field.title");
    TupleFlowParameters extParameters = new FakeParameters(extp);

    PositionIndexWriter writer = new PositionIndexWriter(extParameters);
    // Add entries for title index
    addEntries(writer, "cat", catEntries);
    addEntries(writer, "dog", dogEntries);
    addEntries(writer, "donkey", donkeyEntries);
    writer.close();

    // build the author index
    extp = new Parameters();
    extp.set("statistics/collectionLength", 500);
    extp.set("statistics/documentCount", 5);
    extp.set("statistics/vocabCount", 3);

    extp.set("filename", tempPath + File.separator + "field.author");
    extParameters = new FakeParameters(extp);

    writer = new PositionIndexWriter(extParameters);
    // Add entries for title index
    addEntries(writer, "cat", catEntries);
    addEntries(writer, "dog", dogEntries);
    addEntries(writer, "donkey", donkeyEntries);
    writer.close();

    // build the anchor index
    extp = new Parameters();
    extp.set("statistics/collectionLength", 500);
    extp.set("statistics/documentCount", 5);
    extp.set("statistics/vocabCount", 3);

    extp.set("filename", tempPath + File.separator + "field.anchor");
    extParameters = new FakeParameters(extp);

    writer = new PositionIndexWriter(extParameters);
    // Add entries for title index
    addEntries(writer, "cat", catEntries);
    addEntries(writer, "dog", dogEntries);
    addEntries(writer, "donkey", donkeyEntries);
    writer.close();

    // build an extent index for field lengths
    extp.set("filename", tempPath + File.separator + "postings");
    extParameters = new FakeParameters(extp);

    writer = new PositionIndexWriter(extParameters);
    // Add entries for main index
    addEntries(writer, "cat", catEntries);
    addEntries(writer, "dog", dogEntries);
    addEntries(writer, "donkey", donkeyEntries);
    writer.close();

    // add some document names
    Parameters dnp = new Parameters();
    dnp.set("filename", tempPath + File.separator + "names");

    DiskNameWriter dnWriter = new DiskNameWriter(new FakeParameters(dnp));
    for (int i = 1; i <= 5; i++) {
      dnWriter.process(new NumberedDocumentData("DOC" + i, "", "", i, 100));
    }
    dnWriter.close();

    // build an extent index for field lengths
    extp = new Parameters();
    extp.set("filename", tempPath + File.separator + "extents");
    extParameters = new FakeParameters(extp);

    WindowIndexWriter ewriter = new WindowIndexWriter(extParameters);
    addDummyLengths(ewriter, "anchor");
    addDummyLengths(ewriter, "author");
    addDummyLengths(ewriter, "title");
    ewriter.close();

    // These are dumb and wrong on purpose to make sure the test is operating correctly.
    Parameters lp = new Parameters();
    lp.set("filename", tempPath + File.separator + "lengths");
    DiskLengthsWriter lWriter = new DiskLengthsWriter(new FakeParameters(lp));
    for (int i = 1; i < 6; i++) {
      lWriter.process(new NumberedDocumentData("DOC" + i, "", "", i, i));
    }
    lWriter.close();

    return tempPath;
  }

  private void addDummyLengths(WindowIndexWriter ewriter, String key) throws IOException {
    ewriter.processExtentName(Utility.fromString(key));
    ewriter.processNumber(1);
    ewriter.processBegin(1);
    ewriter.processTuple(121);
    ewriter.processNumber(2);
    ewriter.processBegin(1);
    ewriter.processTuple(101);
    ewriter.processNumber(3);
    ewriter.processBegin(1);
    ewriter.processTuple(51);
    ewriter.processNumber(4);
    ewriter.processBegin(1);
    ewriter.processTuple(81);
    ewriter.processNumber(5);
    ewriter.processBegin(1);
    ewriter.processTuple(151);
  }
}
