// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.core.index.disk.*;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.core.types.FieldLengthData;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author trevor
 */
public class LocalRetrievalTest {

  File tempPath;

  public static File makeIndex() throws IOException, IncompatibleProcessorException {
    // make a spot for the index
    File tempPath = FileUtility.createTemporaryDirectory();

    // put in a generic manifest
    Parameters.instance().write(tempPath + File.separator + "manifest");

    // build an empty extent index
    Parameters extp = Parameters.instance();
    extp.set("filename", tempPath + File.separator + "extents");
    TupleFlowParameters extParameters = new FakeParameters(extp);

    WindowIndexWriter ewriter = new WindowIndexWriter(extParameters);
    ewriter.processExtentName(ByteUtil.fromString("title"));
    ewriter.processNumber(1);
    ewriter.processBegin(1);
    ewriter.processTuple(3);
    ewriter.close();

    // build an empty field index

    Parameters formats = Parameters.instance();
    formats.set("date", "date");
    Parameters tokenizer = Parameters.instance();
    tokenizer.set("formats", formats);
    String[] fields = {"date"};
    tokenizer.set("fields", Arrays.asList(fields));
    Parameters params = Parameters.instance();
    params.set("filename", tempPath + File.separator + "fields");
    params.set("tokenizer", tokenizer);

    FieldIndexWriter fwriter = new FieldIndexWriter(new FakeParameters(params));
    fwriter.processFieldName(ByteUtil.fromString("date"));
    DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT);
    try {
      fwriter.processNumber(1);
      fwriter.processTuple(Utility.fromLong(df.parse("1/2/1975").getTime()));

      fwriter.processNumber(2);
      fwriter.processTuple(Utility.fromLong(df.parse("4/19/1982").getTime()));

      fwriter.processNumber(3);
      fwriter.processTuple(Utility.fromLong(df.parse("2/20/1649").getTime()));

      fwriter.processNumber(5);
      fwriter.processTuple(Utility.fromLong(df.parse("3/11/2012").getTime()));

      fwriter.processNumber(18);
      fwriter.processTuple(Utility.fromLong(df.parse("12/25/0975").getTime()));
    } catch (ParseException pe) {
      throw new IOException(pe);
    }
    fwriter.close();

    // write positions!
    Parameters pp = Parameters.instance();
    pp.set("filename", tempPath + File.separator + "postings");
    pp.set("statistics/collectionLength", 10000);
    pp.set("statistics/documentCount", 100);
    pp.set("statistics/vocabCount", 20);
    TupleFlowParameters posParameters = new FakeParameters(pp);

    PositionIndexWriter pwriter = new PositionIndexWriter(posParameters);

    pwriter.processWord(ByteUtil.fromString("a"));
    pwriter.processDocument(1);
    pwriter.processPosition(1);
    pwriter.processPosition(2);
    pwriter.processPosition(3);

    pwriter.processDocument(3);
    pwriter.processPosition(1);

    pwriter.processDocument(5);
    pwriter.processPosition(1);

    pwriter.processWord(ByteUtil.fromString("b"));
    pwriter.processDocument(1);
    pwriter.processPosition(2);
    pwriter.processPosition(4);

    pwriter.processDocument(2);
    pwriter.processPosition(1);

    pwriter.processDocument(3);
    pwriter.processPosition(4);

    pwriter.processDocument(18);
    pwriter.processPosition(9);
    pwriter.close();

    // add some document names
    Parameters dnp = Parameters.instance();
    dnp.set("filename", tempPath + File.separator + "names");
    DiskNameWriter dnWriter = new DiskNameWriter(new FakeParameters(dnp));
    Parameters dnrp = Parameters.instance();
    dnrp.set("filename", tempPath + File.separator + "names.reverse");
    DiskNameReverseWriter dnrWriter = new DiskNameReverseWriter(new FakeParameters(dnrp));
    Sorter<NumberedDocumentData> dnrSorter = new Sorter<NumberedDocumentData>(new NumberedDocumentData.IdentifierOrder());
    dnrSorter.setProcessor(dnrWriter);

    for (int i = 0; i < 20; i++) {
      dnWriter.process(new NumberedDocumentData("DOC" + i, "", "", i, 100));
      dnrSorter.process(new NumberedDocumentData("DOC" + i, "", "", i, 100));
    }
    dnWriter.close();
    dnrSorter.close();

    Parameters lp = Parameters.instance();
    lp.set("filename", tempPath + File.separator + "lengths");
    DiskLengthsWriter lWriter = new DiskLengthsWriter(new FakeParameters(lp));

    byte[] d = ByteUtil.fromString("document");
    for (int i = 0; i < 20; i++) {
      lWriter.process(new FieldLengthData(d, i, 100));
    }
    lWriter.close();

    return tempPath;
  }

  public static File[] make10DocIndex() throws Exception {
    File trecCorpusFile, corpusFile, indexFile;

    // create a simple doc file, trec format:
    trecCorpusFile = FileUtility.createTemporary();
    Utility.copyStringToFile(
        AppTest.trecDocument("1", "This is a sample document") +
        AppTest.trecDocument("2", "The cat jumped over the moon") +
        AppTest.trecDocument("3", "If the shoe fits, it's ugly") +
        AppTest.trecDocument("4", "Though a program be but three lines long, someday it will have to be maintained.") +
        AppTest.trecDocument("5", "To be trusted is a greater compliment than to be loved") +
        AppTest.trecDocument("6", "Just because everything is different doesn't mean anything has changed.") +
        AppTest.trecDocument("7", "everything everything jumped sample ugly") +
        AppTest.trecDocument("8", "though cat moon cat cat cat") +
        AppTest.trecDocument("9", "document document document document") +
        AppTest.trecDocument("10", "program fits"),
      trecCorpusFile);

    // now, attempt to make a corpus file from that.
    corpusFile = FileUtility.createTemporaryDirectory();
    App.main(new String[]{"make-corpus", "--corpusPath=" + corpusFile.getAbsolutePath(),
              "--inputPath=" + trecCorpusFile.getAbsolutePath(), "--distrib=2"});

    // make sure the corpus file exists
    assertTrue(corpusFile.exists());

    // now, try to build an index from that
    indexFile = FileUtility.createTemporaryDirectory();
    App.main(new String[]{"build", "--stemmedPostings=false", "--indexPath=" + indexFile.getAbsolutePath(),
              "--inputPath=" + corpusFile.getAbsolutePath()});

    AppTest.verifyIndexStructures(indexFile);
    
    File[] files = new File[3];
    files[0] = trecCorpusFile;
    files[1] = corpusFile;
    files[2] = indexFile;
    return files;
  }

  @Before
  public void setUp() throws IOException, IncompatibleProcessorException {
    this.tempPath = makeIndex();
  }

  @After
  public void tearDown() throws IOException {
    FSUtil.deleteDirectory(tempPath);
  }

  @Test
  public void testSimple() throws Exception {
    LocalRetrieval retrieval = new LocalRetrieval(tempPath.toString(), Parameters.instance());

    Node aTerm = new Node("counts", "a");
    ArrayList<Node> aChild = new ArrayList<Node>();
    aChild.add(aTerm);
    NodeParameters a = new NodeParameters();
    a.set("mu", 1500);
    Node aFeature = new Node("dirichlet", a, aChild, 0);

    Node bTerm = new Node("counts", "b");
    ArrayList<Node> bChild = new ArrayList<Node>();
    NodeParameters b = new NodeParameters();
    b.set("mu", 1500);
    bChild.add(bTerm);
    Node bFeature = new Node("dirichlet", b, bChild, 0);

    ArrayList<Node> children = new ArrayList<Node>();
    children.add(aFeature);
    children.add(bFeature);
    Node root = new Node("combine", children);

    Parameters p = Parameters.instance();
    p.set("requested", 5);
    root = retrieval.transformQuery(root, p);
    List<ScoredDocument> results = retrieval.executeQuery(root, p).scoredDocuments;
    assertEquals(results.size(), 5);
    
   
    HashMap<Long, Double> realScores = new HashMap<Long, Double>();

    realScores.put(1l, -5.548387728381024);
    realScores.put(3l, -5.819614290181323);
    realScores.put(5l, -5.937808679213438);
    realScores.put(18l, -5.937808679213438);
    realScores.put(2l, -5.937808679213438);

    HashMap<Long, String> realNames = new HashMap<Long,String>();
    realNames.put(1l, "DOC1");
    realNames.put(2l, "DOC2");
    realNames.put(3l, "DOC3");
    realNames.put(5l, "DOC5");
    realNames.put(18l, "DOC18");

    // make sure the results are sorted
    double lastScore = Double.MAX_VALUE;
    
    for (ScoredDocument sd : results) {
      double score = sd.score;
      double expected = realScores.get(sd.document);
      String expname = realNames.get(sd.document);
      assertTrue(Utility.compare(lastScore, sd.score) >= 0);
      assertEquals(expname, sd.documentName);
      assertEquals(expected, score, 0.0001);

      lastScore = score;
    }
  }

  @Test
  public void testWorkingSet() throws Exception {
    LocalRetrieval retrieval = new LocalRetrieval(tempPath.toString(), Parameters.instance());
    Node root = StructuredQuery.parse("#combine( #dirichlet:mu=1500( #counts:a() ) #dirichlet:mu=1500( #counts:b() ) )");
    Parameters p = Parameters.instance();
    p.set("requested", 5);

    root = retrieval.transformQuery(root, p);

    List<String> ids = new ArrayList<String>();
    ids.add("DOC1");
    ids.add("DOC2");
    ids.add("DOC5");
    p.set("working", ids);
    List<ScoredDocument> results = retrieval.executeQuery(root, p).scoredDocuments;
          
    assertEquals(3, results.size());
    assertEquals(1, results.get(0).document);
    assert ((results.get(1).document == 2 && results.get(2).document == 5)
            || (results.get(1).document == 5 && results.get(2).document == 2));

    HashMap<Long, Double> realScores = new HashMap<Long, Double>();

    realScores.put(1l, -5.548387728381024);
    realScores.put(5l, -5.937808679213438);
    realScores.put(2l, -5.937808679213438);

    HashMap<Long, String> realNames = new HashMap<Long,String>();
    realNames.put(1l, "DOC1");
    realNames.put(2l, "DOC2");
    realNames.put(5l, "DOC5");

    // make sure the results are sorted
    double lastScore = Double.MAX_VALUE;

    for (ScoredDocument sd : results) {
      double score = sd.score;
      double expected = realScores.get(sd.document);
      String expname = realNames.get(sd.document);
      assertTrue(lastScore >= sd.score);
      assertEquals(expname, sd.documentName);
      assertEquals(expected, score, 0.0001);
      lastScore = score;
    }
  }

  @Test
  public void testWindow() throws Exception {
    LocalRetrieval retrieval = new LocalRetrieval(tempPath.toString(), Parameters.instance());

    String query = "#combine( #dirichlet:mu=1500( #uw:5( #extents:a:part=postings() #extents:b:part=postings() ) ) )";
    Node root = StructuredQuery.parse(query);
    Parameters p = Parameters.instance();
    p.set("requested", 5);
    root = retrieval.transformQuery(root, p);
    List<ScoredDocument> results = retrieval.executeQuery(root, p).scoredDocuments;
    

    assertEquals(2, results.size());

    HashMap<Long, Double> realScores = new HashMap<Long, Double>();

    realScores.put(1l, -5.585999438999818);
    realScores.put(3l, -5.991464547107982);

    HashMap<Long, String> realNames = new HashMap<Long,String>();
    realNames.put(1l, "DOC1");
    realNames.put(3l, "DOC3");

    // make sure the results are sorted
    double lastScore = Double.MAX_VALUE;

    for (ScoredDocument sd : results) {
      double score = sd.score;
      double expected = realScores.get(sd.document);
      String expname = realNames.get(sd.document);
      assertTrue(lastScore >= sd.score);
      assertEquals(expname, sd.documentName);
      assertEquals(expected, score, 0.0001);

      lastScore = score;
    }
    retrieval.close();
  }
}
