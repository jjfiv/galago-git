// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.disk.DiskLengthsWriter;
import org.lemurproject.galago.core.index.disk.DiskNameWriter;
import org.lemurproject.galago.core.index.disk.PositionIndexWriter;
import org.lemurproject.galago.core.index.disk.WindowIndexWriter;
import org.lemurproject.galago.core.index.disk.FieldIndexWriter;
import org.lemurproject.galago.core.index.disk.DiskNameReverseWriter;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.core.types.FieldLengthData;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Sorter;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;

/**
 *
 * @author trevor
 */
public class LocalRetrievalTest extends TestCase {

  File tempPath;

  public LocalRetrievalTest(String testName) {
    super(testName);
  }

  public static File makeIndex() throws FileNotFoundException, IOException, IncompatibleProcessorException {
    // make a spot for the index
    File tempPath = Utility.createTemporaryDirectory();

    // put in a generic manifest
    new Parameters().write(tempPath + File.separator + "manifest");

    // build an empty extent index
    Parameters extp = new Parameters();
    extp.set("filename", tempPath + File.separator + "extents");
    TupleFlowParameters extParameters = new FakeParameters(extp);

    WindowIndexWriter ewriter = new WindowIndexWriter(extParameters);
    ewriter.processExtentName(Utility.fromString("title"));
    ewriter.processNumber(1);
    ewriter.processBegin(1);
    ewriter.processTuple(3);
    ewriter.close();

    // build an empty field index

    Parameters formats = new Parameters();
    formats.set("date", "date");
    Parameters tokenizer = new Parameters();
    tokenizer.set("formats", formats);
    String[] fields = {"date"};
    tokenizer.set("fields", Arrays.asList(fields));
    Parameters params = new Parameters();
    params.set("filename", tempPath + File.separator + "fields");
    params.set("tokenizer", tokenizer);

    FieldIndexWriter fwriter = new FieldIndexWriter(new FakeParameters(params));
    fwriter.processFieldName(Utility.fromString("date"));
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
    Parameters pp = new Parameters();
    pp.set("filename", tempPath + File.separator + "postings");
    pp.set("statistics/collectionLength", 10000);
    pp.set("statistics/documentCount", 100);
    pp.set("statistics/vocabCount", 20);
    TupleFlowParameters posParameters = new FakeParameters(pp);

    PositionIndexWriter pwriter = new PositionIndexWriter(posParameters);

    pwriter.processWord(Utility.fromString("a"));
    pwriter.processDocument(1);
    pwriter.processPosition(1);
    pwriter.processPosition(2);
    pwriter.processPosition(3);

    pwriter.processDocument(3);
    pwriter.processPosition(1);

    pwriter.processDocument(5);
    pwriter.processPosition(1);

    pwriter.processWord(Utility.fromString("b"));
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
    Parameters dnp = new Parameters();
    dnp.set("filename", tempPath + File.separator + "names");
    DiskNameWriter dnWriter = new DiskNameWriter(new FakeParameters(dnp));
    Parameters dnrp = new Parameters();
    dnrp.set("filename", tempPath + File.separator + "names.reverse");
    DiskNameReverseWriter dnrWriter = new DiskNameReverseWriter(new FakeParameters(dnrp));
    Sorter<NumberedDocumentData> dnrSorter = new Sorter(new NumberedDocumentData.IdentifierOrder());
    dnrSorter.setProcessor(dnrWriter);

    for (int i = 0; i < 20; i++) {
      dnWriter.process(new NumberedDocumentData("DOC" + i, "", "", i, 100));
      dnrSorter.process(new NumberedDocumentData("DOC" + i, "", "", i, 100));
    }
    dnWriter.close();
    dnrSorter.close();

    Parameters lp = new Parameters();
    lp.set("filename", tempPath + File.separator + "lengths");
    DiskLengthsWriter lWriter = new DiskLengthsWriter(new FakeParameters(lp));

    byte[] d = Utility.fromString("document");
    for (int i = 1; i < 19; i++) {
      lWriter.process(new FieldLengthData(d, i, 100));
    }
    lWriter.close();

    return tempPath;
  }

  public static File[] make10DocIndex() throws Exception {
    File trecCorpusFile, corpusFile, indexFile;

    // create a simple doc file, trec format:
    StringBuilder trecCorpus = new StringBuilder();
    trecCorpus.append(AppTest.trecDocument("1", "This is a sample document"));
    trecCorpus.append(AppTest.trecDocument("2", "The cat jumped over the moon"));
    trecCorpus.append(AppTest.trecDocument("3", "If the shoe fits, it's ugly"));
    trecCorpus.append(AppTest.trecDocument("4", "Though a program be but three lines long, someday it will have to be maintained."));
    trecCorpus.append(AppTest.trecDocument("5", "To be trusted is a greater compliment than to be loved"));
    trecCorpus.append(AppTest.trecDocument("6", "Just because everything is different doesn't mean anything has changed."));
    trecCorpus.append(AppTest.trecDocument("7", "everything everything jumped sample ugly"));
    trecCorpus.append(AppTest.trecDocument("8", "though cat moon cat cat cat"));
    trecCorpus.append(AppTest.trecDocument("9", "document document document document"));
    trecCorpus.append(AppTest.trecDocument("10", "program fits"));
    trecCorpusFile = Utility.createTemporary();
    Utility.copyStringToFile(trecCorpus.toString(), trecCorpusFile);

    // now, attempt to make a corpus file from that.
    corpusFile = Utility.createTemporaryDirectory();
    App.main(new String[]{"make-corpus", "--corpusPath=" + corpusFile.getAbsolutePath(),
              "--inputPath=" + trecCorpusFile.getAbsolutePath(), "--distrib=2"});


    // make sure the corpus file exists
    assertTrue(corpusFile.exists());

    // now, try to build an index from that
    indexFile = Utility.createTemporaryDirectory();
    App.main(new String[]{"build", "--stemmedPostings=false", "--indexPath=" + indexFile.getAbsolutePath(),
              "--inputPath=" + corpusFile.getAbsolutePath()});

    AppTest.verifyIndexStructures(indexFile);

    File[] files = new File[3];
    files[0] = trecCorpusFile;
    files[1] = corpusFile;
    files[2] = indexFile;

    return files;
  }

  @Override
  public void setUp() throws IOException, IncompatibleProcessorException {
    this.tempPath = makeIndex();
  }

  @Override
  public void tearDown() throws IOException {
    Utility.deleteDirectory(tempPath);
  }

  public void testSimple() throws FileNotFoundException, IOException, Exception {
    LocalRetrieval retrieval = new LocalRetrieval(tempPath.toString(), new Parameters());

    Node aTerm = new Node("counts", "a");
    ArrayList<Node> aChild = new ArrayList<Node>();
    aChild.add(aTerm);
    NodeParameters a = new NodeParameters();
    a.set("default", "dirichlet");
    a.set("mu", 1500);
    Node aFeature = new Node("feature", a, aChild, 0);

    Node bTerm = new Node("counts", "b");
    ArrayList<Node> bChild = new ArrayList<Node>();
    NodeParameters b = new NodeParameters();
    b.set("default", "dirichlet");
    b.set("mu", 1500);
    bChild.add(bTerm);
    Node bFeature = new Node("feature", b, bChild, 0);

    ArrayList<Node> children = new ArrayList<Node>();
    children.add(aFeature);
    children.add(bFeature);
    Node root = new Node("combine", children);

    Parameters p = new Parameters();
    p.set("requested", 5);
    root = retrieval.transformQuery(root, p);
    ScoredDocument[] result = retrieval.runQuery(root, p);

    assertEquals(result.length, 5);

    HashMap<Integer, Double> realScores = new HashMap<Integer, Double>();

    realScores.put(1, -6.211080532397473);
    realScores.put(3, -6.81814312029245);
    realScores.put(5, -7.241792050486051);
    realScores.put(18, -7.241792050486051);
    realScores.put(2, -7.241792050486051);

    HashMap<Integer, String> realNames = new HashMap();
    realNames.put(1, "DOC1");
    realNames.put(2, "DOC2");
    realNames.put(3, "DOC3");
    realNames.put(5, "DOC5");
    realNames.put(18, "DOC18");

    // make sure the results are sorted
    double lastScore = Double.MAX_VALUE;

    for (int i = 0; i < result.length; i++) {
      double score = result[i].score;
      double expected = realScores.get(result[i].document);
      String expname = realNames.get(result[i].document);
      assertTrue(lastScore >= result[i].score);
      assertEquals(expname, result[i].documentName);
      assertEquals(expected, score, 0.0001);

      lastScore = score;
    }
  }

  public void testWorkingSet() throws FileNotFoundException, IOException, Exception {
    LocalRetrieval retrieval = new LocalRetrieval(tempPath.toString(), new Parameters());
    Node root = StructuredQuery.parse("#combine( #feature:dirichlet:mu=1500( #counts:a() ) #feature:dirichlet:mu=1500( #counts:b() ) )");
    Parameters p = new Parameters();
    p.set("requested", 5);
    
    root = retrieval.transformQuery(root, p);

    List<String> ids = new ArrayList<String>();
    ids.add("DOC1");
    ids.add("DOC2");
    ids.add("DOC5");
    p.set("working", ids);
   
    ScoredDocument[] result = retrieval.runQuery(root, p);

    assertEquals(3, result.length);
    assertEquals(1, result[0].document);
    assert ((result[1].document == 2 && result[2].document == 5)
            || (result[1].document == 5 && result[2].document == 2));

    HashMap<Integer, Double> realScores = new HashMap<Integer, Double>();

    realScores.put(1, -6.211080532397473);
    realScores.put(5, -7.241792050486051);
    realScores.put(2, -7.241792050486051);

    HashMap<Integer, String> realNames = new HashMap();
    realNames.put(1, "DOC1");
    realNames.put(2, "DOC2");
    realNames.put(5, "DOC5");

    // make sure the results are sorted
    double lastScore = Double.MAX_VALUE;

    for (int i = 0; i < result.length; i++) {
      double score = result[i].score;
      double expected = realScores.get(result[i].document);
      String expname = realNames.get(result[i].document);
      assertTrue(lastScore >= result[i].score);
      assertEquals(expname, result[i].documentName);
      assertEquals(expected, score, 0.0001);

      lastScore = score;
    }
  }

  public void testWindow() throws FileNotFoundException, IOException, Exception {
    LocalRetrieval retrieval = new LocalRetrieval(tempPath.toString(), new Parameters());

    String query = "#combine( #feature:dirichlet:mu=1500( #uw:5( #extents:a:part=postings() #extents:b:part=postings() ) ) )";
    Node root = StructuredQuery.parse(query);
    Parameters p = new Parameters();
    p.set("requested", 5);
    root = retrieval.transformQuery(root, p);

    ScoredDocument[] result = retrieval.runQuery(root, p);

    assertEquals(result.length, 2);

    HashMap<Integer, Double> realScores = new HashMap<Integer, Double>();

    realScores.put(1, -6.0968250627658085);
    realScores.put(3, -6.907755278982137);

    HashMap<Integer, String> realNames = new HashMap();
    realNames.put(1, "DOC1");
    realNames.put(3, "DOC3");

    // make sure the results are sorted
    double lastScore = Double.MAX_VALUE;

    for (int i = 0; i < result.length; i++) {
      double score = result[i].score;
      double expected = realScores.get(result[i].document);
      String expname = realNames.get(result[i].document);
      assertTrue(lastScore >= result[i].score);
      assertEquals(expname, result[i].documentName);
      assertEquals(expected, score, 0.0001);

      lastScore = score;
    }
    retrieval.close();
  }
}
