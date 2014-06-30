// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.retrieval.traversal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.core.index.disk.*;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.types.FieldLengthData;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Tests for correctness of field-based retrieval models. Currently this
 * consists of BM25F, PRMS, PL2F
 *
 * For each model we test: - the transformation correctness of model. - the
 * correctness of the math involved in the model.
 *
 * @author irmarc
 */
public class FieldTraversalTest {

  File indexPath;

  @Before
  public void setUp() throws FileNotFoundException, IOException {
    indexPath = makeFieldIndexes();
  }

  @After
  public void tearDown() throws IOException {
    Utility.deleteDirectory(indexPath);
  }

  @Test
  public void testSetFieldTraversal() throws Exception {
    LocalRetrieval retrieval = new LocalRetrieval(indexPath.getAbsolutePath(), Parameters.parseArray("fields", Arrays.asList("title", "anchor", "author")));

    Node raw = StructuredQuery.parse("cat dog donkey");
    Node prepared = retrieval.transformQuery(raw, Parameters.instance());

    System.out.println(raw);

    Traversal fieldSetter = new SetFieldTraversal();
    Node fielded = fieldSetter.traverse(prepared, Parameters.parseArray("setField", "title", "stemmer", ""));

    // make sure we've switched the parts appropriately
    assertEquals("#combine:w=1.0( #dirichlet:avgLength=200.0:collectionLength=1000:documentCount=5:maximumCount=5:nodeFrequency=13:w=0.3333333333333333( #lengths:title:part=lengths() #counts:cat:part=field.title() ) #dirichlet:avgLength=200.0:collectionLength=1000:documentCount=5:maximumCount=4:nodeFrequency=11:w=0.3333333333333333( #lengths:title:part=lengths() #counts:dog:part=field.title() ) #dirichlet:avgLength=200.0:collectionLength=1000:documentCount=5:maximumCount=5:nodeFrequency=12:w=0.3333333333333333( #lengths:title:part=lengths() #counts:donkey:part=field.title() ) )", fielded.toString());

    List<ScoredDocument> docs = retrieval.executeQuery(fielded).scoredDocuments;
    assertFalse(docs.isEmpty());
  }

  // We pull statistics directly from the index to make sure they are
  // generated correctly.
  @Test
  public void testPRMSTraversalCorrectness() throws Exception {
    DiskIndex index = new DiskIndex(indexPath.getAbsolutePath());

    // set fields
    String[] fields = {"title", "author", "anchor"};
    Parameters global = Parameters.instance();
    global.set("fields", Arrays.asList(fields));

    LocalRetrieval retrieval = new LocalRetrieval(index, global);
    Parameters qp = Parameters.instance();
    PRMS2Traversal traversal = new PRMS2Traversal(retrieval);
    Node q1 = StructuredQuery.parse("#prms2(#text:cat() #text:dog() #text:donkey())");
    Node q2 = traversal.traverse(q1, qp);
    
    StringBuilder transformed = new StringBuilder();
    transformed.append("#combine:norm=false( ");
    transformed.append("#wsum:0=0.3333333333333333:1=0.3333333333333333:2=0.3333333333333333 ( ");
    transformed.append("#dirichlet:lengths=title( #lengths:title:part=lengths() #counts:cat:part=field.title() ) ");
    transformed.append("#dirichlet:lengths=author( #lengths:author:part=lengths() #counts:cat:part=field.author() ) ");
    transformed.append("#dirichlet:lengths=anchor( #lengths:anchor:part=lengths() #counts:cat:part=field.anchor() ) ) ");
    transformed.append("#wsum:0=0.3333333333333333:1=0.3333333333333333:2=0.3333333333333333 ( ");
    transformed.append("#dirichlet:lengths=title( #lengths:title:part=lengths() #counts:dog:part=field.title() ) ");
    transformed.append("#dirichlet:lengths=author( #lengths:author:part=lengths() #counts:dog:part=field.author() ) ");
    transformed.append("#dirichlet:lengths=anchor( #lengths:anchor:part=lengths() #counts:dog:part=field.anchor() ) ) ");
    transformed.append("#wsum:0=0.3333333333333333:1=0.3333333333333333:2=0.3333333333333333 ( ");
    transformed.append("#dirichlet:lengths=title( #lengths:title:part=lengths() #counts:donkey:part=field.title() ) ");
    transformed.append("#dirichlet:lengths=author( #lengths:author:part=lengths() #counts:donkey:part=field.author() ) ");
    transformed.append("#dirichlet:lengths=anchor( #lengths:anchor:part=lengths() #counts:donkey:part=field.anchor() ) ) ");
    transformed.append(" )");

    Node expected = StructuredQuery.parse(transformed.toString());
    assertEquals(expected.toString(), q2.toString());
  }

  @Test
  public void testPRMSDeltaVsModel() throws Exception {
    DiskIndex index = new DiskIndex(indexPath.getAbsolutePath());

    // set fields
    String[] fields = {"title", "author", "anchor"};
    Parameters global = Parameters.instance();
    global.set("fields", Arrays.asList(fields));

    String query = "#prms2(cat dog donkey)";

    LocalRetrieval retrieval = new LocalRetrieval(index, global);
    Node raw = StructuredQuery.parse(query);
    Node root = retrieval.transformQuery(raw, global);
    global.set("deltaReady", false);
    List<ScoredDocument> results = retrieval.executeQuery(root, global).scoredDocuments;
   
    global.set("deltaReady", true);
    
    List<ScoredDocument> results2 = retrieval.executeQuery(root, global).scoredDocuments;

    assertEquals(results.size(), results2.size());

    for (int i = 0; i < results.size(); i++) {
      assertEquals(results.get(i).document, results2.get(i).document);
      assertEquals(results.get(i).score, results2.get(i).score, 0.00001);
    }

  }

  @Test
  public void testPRMS2ModelCorrectness() throws Exception {
    DiskIndex index = new DiskIndex(indexPath.getAbsolutePath());

    // set fields
    String[] fields = {"title", "author", "anchor"};
    Parameters global = Parameters.instance();

    LocalRetrieval retrieval = new LocalRetrieval(index, global);
    Parameters qp = Parameters.instance();
    qp.set("fields", Arrays.asList(fields));
    String query = "#prms2(cat dog donkey)";
    Node raw = StructuredQuery.parse(query);
    Node root = retrieval.transformQuery(raw, qp);
    qp.set("deltaReady", false);
    List<ScoredDocument> results = retrieval.executeQuery(root, qp).scoredDocuments;
 
    assertEquals(5, results.size());

    assertEquals(1, results.get(0).document);
    assertEquals(results.get(0).score, -11.160840, 0.00001);
    assertEquals(2, results.get(1).document);
    assertEquals(results.get(1).score, -11.172624, 0.00001);
    assertEquals(5, results.get(2).document);
    assertEquals(results.get(2).score, -11.189912, 0.00001);
    assertEquals(4, results.get(3).document);
    assertEquals(results.get(3).score, -11.231324, 0.00001);
    assertEquals(3, results.get(4).document);
    assertEquals(results.get(4).score, -11.240375, 0.00001);
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
  private static final int[][] catEntries = {
    {1, 5, 23, 78, 112},
    {4, 3, 12, 48, 100, 109},
    {5, 19, 45, 66, 89}
  };
  private static final int[][] dogEntries = {
    {1, 6, 27, 79, 118},
    {2, 1, 5, 19},
    {5, 3, 18, 46, 90}
  };
  private static final int[][] donkeyEntries = {
    {1, 7, 30},
    {2, 2, 10, 20, 24, 25},
    {3, 1, 7},
    {5, 4, 47, 91}
  };

  public File makeFieldIndexes() throws IOException {
    // make a spot for the index
    File tempPath = FileUtility.createTemporaryDirectory();

    // put in a generic manifest
    Parameters.instance().write(tempPath + File.separator + "manifest");

    // build the title index
    Parameters extp = Parameters.instance();
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
    extp = Parameters.instance();
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
    extp = Parameters.instance();
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
    Parameters dnp = Parameters.instance();
    dnp.set("filename", tempPath + File.separator + "names");

    DiskNameWriter dnWriter = new DiskNameWriter(new FakeParameters(dnp));
    for (int i = 1; i <= 5; i++) {
      dnWriter.process(new NumberedDocumentData("DOC" + i, "", "", i, 100));
    }
    dnWriter.close();

    // build an extent index for field lengths
    extp = Parameters.instance();
    extp.set("filename", tempPath + File.separator + "extents");
    extParameters = new FakeParameters(extp);

    WindowIndexWriter ewriter = new WindowIndexWriter(extParameters);
    
    Parameters lp = Parameters.instance();
    lp.set("filename", tempPath + File.separator + "lengths");
    DiskLengthsWriter lwriter = new DiskLengthsWriter(new FakeParameters(lp));

    addDummyLengths(ewriter, lwriter, "anchor", 0);
    addDummyLengths(ewriter, lwriter, "author", 0);
    // NOTE that 'document' lengths is intentionally different from the other fields
    // -- this checks that we are reading and smoothing with field data.
    addDummyLengths(ewriter, lwriter, "document", 100);
    addDummyLengths(ewriter, lwriter, "title", 0);
    ewriter.close();
    
    lwriter.close();

    return tempPath;
  }

  /** 
   * indexes lengths data
   *  - note that the error parameter ensures the length data is DIFFERENT 
   *    (when compared with the extents)
   */
  private void addDummyLengths(WindowIndexWriter ewriter, DiskLengthsWriter lwriter, String key, int error) throws IOException {
    ewriter.processExtentName(Utility.fromString(key));
    ewriter.processNumber(1);
    ewriter.processBegin(1);
    ewriter.processTuple(121);
    lwriter.process( new FieldLengthData(Utility.fromString(key), 1, (121 - 1) + error) );
    
    ewriter.processNumber(2);
    ewriter.processBegin(1);
    ewriter.processTuple(101);
    lwriter.process( new FieldLengthData(Utility.fromString(key), 2, (101 - 1) + error) );

    ewriter.processNumber(3);
    ewriter.processBegin(1);
    ewriter.processTuple(51);
    lwriter.process( new FieldLengthData(Utility.fromString(key), 3, (51 - 1) + error) );

    ewriter.processNumber(4);
    ewriter.processBegin(1);
    ewriter.processTuple(81);
    lwriter.process( new FieldLengthData(Utility.fromString(key), 4, (81 - 1) + error) );

    ewriter.processNumber(5);
    ewriter.processBegin(1);
    ewriter.processTuple(151);
    lwriter.process( new FieldLengthData(Utility.fromString(key), 5, (151 - 1) + error ));
  }
}
