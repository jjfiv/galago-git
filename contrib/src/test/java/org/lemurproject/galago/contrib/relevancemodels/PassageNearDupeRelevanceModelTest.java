package org.lemurproject.galago.contrib.relevancemodels;

import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.traversal.RelevanceModelTraversal;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.File;
import java.util.List;
import static junit.framework.Assert.assertEquals;

/**
 * Tests for {@link PassageNearDupeRelevanceModel}.
 *
 * Removed tests - new plan is to remove RM from traversals. 
 *  RMExpander is similar to a traversal, but must be operated manually.
 * 
 * @author dietz, sjh
 */
public class PassageNearDupeRelevanceModelTest extends TestCase {
  private String whitelistfileName = "";

  File relsFile = null;
  File queryFile = null;
  File scoresFile = null;
  File trecCorpusFile = null;
  File corpusFile = null;
  File indexFile = null;

  public PassageNearDupeRelevanceModelTest(String testName) {
    super(testName);
  }
  
  public void testNothing() throws Exception{
    if(false){
      System.out.println("Add some tests for RelevanceModelExpander.");
    }
  }

//
//  public void testNoDeduplication() throws Exception {
//    // Create a retrieval object for use by the traversal
//    Parameters p = new Parameters();
//    p.set("index", indexFile.getAbsolutePath());
//    p.set("corpus", corpusFile.getAbsolutePath());
//    p.set("stemmedPostings", false);
//    p.set("fbOrigWt", 0.5);
//    p.set("fb2Pass", true);
//    p.set("docdedupe", false);
//    p.set("termWhitelistFile", "");
//
//    Parameters rmParams = new Parameters();
//    rmParams.set("passageQuery", false);
//    rmParams.set("passageSize", 3);
//    rmParams.set("passageShift", 1);
//
//    p.set("fbParams2Pass",rmParams);
//    p.set("relevanceModel","org.lemurproject.galago.contrib.relevancemodels.PassageNearDupeRelevanceModel");
//    LocalRetrieval retrieval = (LocalRetrieval) RetrievalFactory.instance(p);
//    RelevanceModelTraversal traversal = new RelevanceModelTraversal(retrieval, new Parameters());
//
//    Node parsedTree = StructuredQuery.parse("#rm:fbTerms=5:fbDocs=10( #feature:dirichlet( #extents:crisis:part=postings() ) )");
//    Node transformed = StructuredQuery.copy(traversal, parsedTree);
//
//    StringBuilder correct = new StringBuilder();
//    correct.append("#combine:0=0.5:1=0.5( #combine:w=1.0( #feature:dirichlet( #extents:crisis:part=postings() ) ) " +
//            "#combine:0=0.0054355411781812964:1=0.004822391365383995:2=0.0027204366475475786:3=0.0020180077026308195:4=0.0013607882628793812( " +
//            "#feature:dirichlet( #lengths:document:part=lengths() #extents:real:part=postings() ) " +
//            "#feature:dirichlet( #lengths:document:part=lengths() #extents:estate:part=postings() ) " +
//            "#feature:dirichlet( #lengths:document:part=lengths() #extents:bubble:part=postings() ) " +
//            "#feature:dirichlet( #lengths:document:part=lengths() #extents:result:part=postings() ) " +
//            "#feature:dirichlet( #lengths:document:part=lengths() #extents:created:part=postings() ) ) )");
//
//    Node correctNode = StructuredQuery.parse(correct.toString());
//
//    // check that generated terms are the same:
//    List<Node> correctExpTerms = correctNode.getChild(1).getInternalNodes();
//    List<Node> observedExpTerms = transformed.getChild(1).getInternalNodes();
//
//    assertEquals(correctExpTerms.size(), observedExpTerms.size());
//    for(int i=0;i<correctExpTerms.size();i++){
//      assertEquals(correctExpTerms.get(i).toString(), observedExpTerms.get(i).toString());
//    }
//    
//    // check that weights are approximately the same 7 decimals..
//    Node correctExp = correctNode.getChild(1);
//    Node observedExp = transformed.getChild(1);
//    for(int i=0;i<correctExpTerms.size();i++){
//      assertEquals(correctExp.getNodeParameters().getDouble(Integer.toString(i)),  observedExp.getNodeParameters().getDouble(Integer.toString(i)),  0.000001);
//    }
//    
//    retrieval.close();
//    // System.out.println(transformed.toPrettyString());
//  }
//
//
//
//  public void testDedupePassageRM() throws Exception {
//    // Create a retrieval object for use by the traversal
//    Parameters p = new Parameters();
//    p.set("index", indexFile.getAbsolutePath());
//    p.set("corpus", corpusFile.getAbsolutePath());
//    p.set("stemmedPostings", false);
//    p.set("fbOrigWt", 0.5);
//    p.set("fb2Pass", true);
//    p.set("docdedupe", true);
//
//    Parameters rmParams = new Parameters();
//    rmParams.set("passageQuery", true);
//    rmParams.set("passageSize", 3);
//    rmParams.set("passageShift", 1);
//
//    p.set("fbParams2Pass",rmParams);
//    p.set("relevanceModel","org.lemurproject.galago.contrib.relevancemodels.PassageNearDupeRelevanceModel");
//    LocalRetrieval retrieval = (LocalRetrieval) RetrievalFactory.instance(p);
//    RelevanceModelTraversal traversal = new RelevanceModelTraversal(retrieval, new Parameters());
//
//    Node parsedTree = StructuredQuery.parse("#rm:fbTerms=5:fbDocs=10( #feature:dirichlet( #extents:crisis:part=postings() ) )");
//    Node transformed = StructuredQuery.copy(traversal, parsedTree);
//
//    StringBuilder correct = new StringBuilder();
//    correct.append("#combine:0=0.5:1=0.5( #combine:w=1.0( #feature:dirichlet( #extents:crisis:part=postings() ) ) " +
//            "#combine:0=0.006666666666666665:1=0.006666666666666665:2=0.0033333333333333327:3=0.0033333333333333327:4=0.0033333333333333327( " +
//            "#feature:dirichlet( #lengths:document:part=lengths() #extents:estate:part=postings() ) " +
//            "#feature:dirichlet( #lengths:document:part=lengths() #extents:financial:part=postings() ) " +
//            "#feature:dirichlet( #lengths:document:part=lengths() #extents:normally:part=postings() ) " +
//            "#feature:dirichlet( #lengths:document:part=lengths() #extents:real:part=postings() ) " +
//            "#feature:dirichlet( #lengths:document:part=lengths() #extents:se:part=postings() ) ) )");
//
//    Node correctNode = StructuredQuery.parse(correct.toString());
//
//    // check that generated terms are the same:
//    List<Node> correctExpTerms = correctNode.getChild(1).getInternalNodes();
//    List<Node> observedExpTerms = transformed.getChild(1).getInternalNodes();
//
//    assertEquals(correctExpTerms.size(), observedExpTerms.size());
//    for(int i=0;i<correctExpTerms.size();i++){
//      assertEquals(correctExpTerms.get(i).toString(), observedExpTerms.get(i).toString());
//    }
//    
//    // check that weights are approximately the same 7 decimals..
//    Node correctExp = correctNode.getChild(1);
//    Node observedExp = transformed.getChild(1);
//    for(int i=0;i<correctExpTerms.size();i++){
//      assertEquals(correctExp.getNodeParameters().getDouble(Integer.toString(i)),  observedExp.getNodeParameters().getDouble(Integer.toString(i)),  0.000001);
//    }
//
//    retrieval.close();
//    // System.out.println(transformed.toPrettyString());
//  }
//
//
//  public void testDedupePassageWhitelistRM() throws Exception {
//    // Create a retrieval object for use by the traversal
//    Parameters p = new Parameters();
//    p.set("index", indexFile.getAbsolutePath());
//    p.set("corpus", corpusFile.getAbsolutePath());
//    p.set("stemmedPostings", false);
//    p.set("fbOrigWt", 0.5);
//    p.set("fb2Pass", true);
//    p.set("docdedupe", true);
//
//    Parameters rmParams = new Parameters();
//    rmParams.set("passageQuery", true);
//    rmParams.set("passageSize", 3);
//    rmParams.set("passageShift", 1);
//    rmParams.set("termWhitelistFile", whitelistfileName);
//
//    p.set("fbParams2Pass",rmParams);
//    p.set("relevanceModel","org.lemurproject.galago.contrib.relevancemodels.PassageNearDupeRelevanceModel");
//    LocalRetrieval retrieval = (LocalRetrieval) RetrievalFactory.instance(p);
//    RelevanceModelTraversal traversal = new RelevanceModelTraversal(retrieval, new Parameters());
//
//    Node parsedTree = StructuredQuery.parse("#rm:fbTerms=5:fbDocs=10( #feature:dirichlet( #extents:crisis:part=postings() ) )");
//    Node transformed = StructuredQuery.copy(traversal, parsedTree);
//
//    StringBuilder correct = new StringBuilder();
//    correct.append("#combine:0=0.5:1=0.5( #combine:w=1.0( #feature:dirichlet( #extents:crisis:part=postings() ) ) " +
//            "#combine:0=0.006666666666666665:1=0.0033333333333333327( " +
//            "#feature:dirichlet( #lengths:document:part=lengths() #extents:financial:part=postings() ) " +
//            "#feature:dirichlet( #lengths:document:part=lengths() #extents:normally:part=postings() ) ) )");
//
//    Node correctNode = StructuredQuery.parse(correct.toString());
//
//    // check that generated terms are the same:
//    List<Node> correctExpTerms = correctNode.getChild(1).getInternalNodes();
//    List<Node> observedExpTerms = transformed.getChild(1).getInternalNodes();
//
//    assertEquals(correctExpTerms.size(), observedExpTerms.size());
//    for(int i=0;i<correctExpTerms.size();i++){
//      assertEquals(correctExpTerms.get(i).toString(), observedExpTerms.get(i).toString());
//    }
//    
//    // check that weights are approximately the same 7 decimals..
//    Node correctExp = correctNode.getChild(1);
//    Node observedExp = transformed.getChild(1);
//    for(int i=0;i<correctExpTerms.size();i++){
//      assertEquals(correctExp.getNodeParameters().getDouble(Integer.toString(i)),  observedExp.getNodeParameters().getDouble(Integer.toString(i)),  0.000001);
//    }
//
//    retrieval.close();
//    // System.out.println(transformed.toPrettyString());
//  }
//
//
//  public void testDedupeDocumentRM() throws Exception {
//    // Create a retrieval object for use by the traversal
//    Parameters p = new Parameters();
//    p.set("index", indexFile.getAbsolutePath());
//    p.set("corpus", corpusFile.getAbsolutePath());
//    p.set("stemmedPostings", false);
//    p.set("fbOrigWt", 0.5);
//    p.set("defPassageRM", false);
//
////    Parameters p2 = new Parameters();
////    p2.set("dedupeScoreThresh", 0.95);
////    p2.set("docdedupe", true);
////    p.set("fbParams",p2);
//
//    p.set("dedupeScoreThresh", 0.95);
//    p.set("docdedupe", true);
//
//
//
//    p.set("relevanceModel","org.lemurproject.galago.contrib.relevancemodels.PassageNearDupeRelevanceModel");
//    LocalRetrieval retrieval = (LocalRetrieval) RetrievalFactory.instance(p);
//    RelevanceModelTraversal traversal = new RelevanceModelTraversal(retrieval, new Parameters());
//
//    Node parsedTree = StructuredQuery.parse("#rm:fbTerms=5:fbDocs=10( #feature:dirichlet( #extents:crisis:part=postings() ) )");
//    Node transformed = StructuredQuery.copy(traversal, parsedTree);
//
//    // truth data
//    StringBuilder correct = new StringBuilder();
//    correct.append("#combine:0=0.5:1=0.5( #combine:w=1.0( #feature:dirichlet( #extents:crisis:part=postings() ) ) " +
//            "#combine:0=0.003902666646188044:1=0.003596091739789393:2=0.001800711928351627:3=0.0014048578898335185:4=0.0010542133564807308( " +
//            "#feature:dirichlet( #lengths:document:part=lengths() #extents:real:part=postings() ) " +
//            "#feature:dirichlet( #lengths:document:part=lengths() #extents:estate:part=postings() ) " +
//            "#feature:dirichlet( #lengths:document:part=lengths() #extents:bubble:part=postings() ) " +
//            "#feature:dirichlet( #lengths:document:part=lengths() #extents:result:part=postings() ) " +
//            "#feature:dirichlet( #lengths:document:part=lengths() #extents:created:part=postings() ) ) )");
//
//    Node correctNode = StructuredQuery.parse(correct.toString());
//
//    // check that generated terms are the same:
//    List<Node> correctExpTerms = correctNode.getChild(1).getInternalNodes();
//    List<Node> observedExpTerms = transformed.getChild(1).getInternalNodes();
//
//    assertEquals(correctExpTerms.size(), observedExpTerms.size());
//    for(int i=0;i<correctExpTerms.size();i++){
//      assertEquals(correctExpTerms.get(i).toString(), observedExpTerms.get(i).toString());
//    }
//    
//    // check that weights are approximately the same 7 decimals..
//    Node correctExp = correctNode.getChild(1);
//    Node observedExp = transformed.getChild(1);
//    for(int i=0;i<correctExpTerms.size();i++){
//      assertEquals(correctExp.getNodeParameters().getDouble(Integer.toString(i)),  observedExp.getNodeParameters().getDouble(Integer.toString(i)),  0.000001);
//    }
//
//    retrieval.close();
//  }
//
//  public void testDedupeDocumentRMWhitelist() throws Exception {
//    // Create a retrieval object for use by the traversal
//    Parameters p = new Parameters();
//    p.set("index", indexFile.getAbsolutePath());
//    p.set("corpus", corpusFile.getAbsolutePath());
//    p.set("stemmedPostings", false);
//    p.set("fbOrigWt", 0.5);
//    p.set("defPassageRM", false);
//    p.set("dedupeScoreThresh", 0.95);
//    p.set("docdedupe", true);
//    p.set("termWhitelistFile", whitelistfileName);
//
//    p.set("relevanceModel","org.lemurproject.galago.contrib.relevancemodels.PassageNearDupeRelevanceModel");
//    LocalRetrieval retrieval = (LocalRetrieval) RetrievalFactory.instance(p);
//    RelevanceModelTraversal traversal = new RelevanceModelTraversal(retrieval, new Parameters());
//
//    Node parsedTree = StructuredQuery.parse("#rm:fbTerms=5:fbDocs=10( #feature:dirichlet( #extents:crisis:part=postings() ) )");
//    Node transformed = StructuredQuery.copy(traversal, parsedTree);
//
//    // truth data
//    StringBuilder correct = new StringBuilder();
//    correct.append("#combine:0=0.5:1=0.5( #combine:w=1.0( #feature:dirichlet( #extents:crisis:part=postings() ) ) " +
//            "#combine:0=9.197247191959517E-4:1=3.065749063986506E-4( " +
//            "#feature:dirichlet( #lengths:document:part=lengths() #extents:financial:part=postings() ) " +
//            "#feature:dirichlet( #lengths:document:part=lengths() #extents:normally:part=postings() ) ) )");
//
//    Node correctNode = StructuredQuery.parse(correct.toString());
//
//    // check that generated terms are the same:
//    List<Node> correctExpTerms = correctNode.getChild(1).getInternalNodes();
//    List<Node> observedExpTerms = transformed.getChild(1).getInternalNodes();
//
//    assertEquals(correctExpTerms.size(), observedExpTerms.size());
//    for(int i=0;i<correctExpTerms.size();i++){
//      assertEquals(correctExpTerms.get(i).toString(), observedExpTerms.get(i).toString());
//    }
//    
//    // check that weights are approximately the same 7 decimals..
//    Node correctExp = correctNode.getChild(1);
//    Node observedExp = transformed.getChild(1);
//    for(int i=0;i<correctExpTerms.size();i++){
//      assertEquals(correctExp.getNodeParameters().getDouble(Integer.toString(i)),  observedExp.getNodeParameters().getDouble(Integer.toString(i)),  0.000001);
//    }
//    retrieval.close();
//  }



  // Build an index based on 10 short docs
  @Override
  public void setUp() throws Exception {
    File[] files = make10DocIndex();
    trecCorpusFile = files[0];
    corpusFile = files[1];
    indexFile = files[2];

    File whiteListfile = Utility.createTemporary();
    Utility.copyStringToFile("financial normally\ncrisis", whiteListfile);

    whitelistfileName = whiteListfile.getAbsolutePath();
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
    if (corpusFile != null) {
      Utility.deleteDirectory(corpusFile);
    }
    if (indexFile != null) {
      Utility.deleteDirectory(indexFile);
    }

    new File(whitelistfileName).delete();
  }
  public static File[] make10DocIndex() throws Exception {
    File trecCorpusFile, corpusFile, indexFile;

    // create a simple doc file, trec format:
    StringBuilder trecCorpus = new StringBuilder();
    trecCorpus.append(trecDocument("1", "stuff like that too We were talking about the real estate crisis and the blogger mixed all financial industries together so we start out talking real estate and we now include insurance equities mutual funds commodities stuff that may score political points over beers but won't say squat about real"));
    trecCorpus.append(trecDocument("2", "stuff like that too We were talking about the real estate crisis and the blogger mixed all financial industries together so we start out talking real estate and we now include insurance equities mutual funds commodities stuff that may score political points over beers but won't say squat about real"));
    trecCorpus.append(trecDocument("3", "FURTHER REAL ESTATE COLLAPSE is COMING WHY post such a prophecy Does N hope a real estate failure will cause Obama to be deemed a failure Or that a real estate collapse will benefit his political ideology Wry I did not read your post until today Let it be known"));
    trecCorpus.append(trecDocument("4", "FURTHER REAL ESTATE COLLAPSE is COMING WHY post such a prophecy Does N hope a real estate failure will cause Obama to be deemed a failure Or that a real estate collapse will benefit his political ideology Fear is an effective 'antidote' for success and perveyors of fear have their"));
    trecCorpus.append(trecDocument("5", "FURTHER REAL ESTATE COLLAPSE is COMING WHY post such a prophecy Does N hope a real estate failure will cause Obama to be deemed a failure Or that a real estate collapse will benefit his political ideology Fear is an effective 'antidote' for success and perveyors of fear have their"));
    trecCorpus.append(trecDocument("5a", "FURTHER REAL ESTATE COLLAPSE is COMING WHY post such a prophecy Does N hope a real estate failure will cause Obama to be deemed a failure Or that a real estate collapse will benefit his political ideology Fear is an effective 'antidote' for success and perveyors of fear have their"));
    trecCorpus.append(trecDocument("5b", "FURTHER REAL ESTATE COLLAPSE is COMING WHY post such a prophecy Does N hope a real estate failure will cause Obama to be deemed a failure Or that a real estate collapse will benefit his political ideology Fear is an effective 'antidote' for success and perveyors of fear have their"));
    trecCorpus.append(trecDocument("5c", "FURTHER REAL ESTATE COLLAPSE is COMING WHY post such a prophecy Does N hope a real estate failure will cause Obama to be deemed a failure Or that a real estate collapse will benefit his political ideology Fear is an effective 'antidote' for success and perveyors of fear have their"));
    trecCorpus.append(trecDocument("6", "what happened in the Financial Crisis is normally what happens in real estate bubbles The people who drove it were generally middle and upper middle class people buying into the Sun Belt For the last 100 years there have been periodic booms and busts in real estate in this country."));
    trecCorpus.append(trecDocument("7", "happened in Japan where real estate also collapsed in the late 1980s but rather than mark down the damaged goods and dispose of them Japanese banks held them on their books The result was a painfully slow recovery in the Japanese real estate market The 1997 Asian crisis Southeast SE"));
    trecCorpus.append(trecDocument("8", "the toxic asset crisis which caused the real estate bubble to burst and it was the over valuation of real estate that created that bubble Said bubble was a result of an increase in value that was the result in an increase in the demand of investors without a corresponding"));
    trecCorpus.append(trecDocument("8a", "the toxic asset crisis which caused the real estate bubble to burst and it was the over valuation of real estate that created that bubble Said bubble was a result of an increase in value that was the result"));
    trecCorpus.append(trecDocument("9", "in consumer spending they will result in no net increase in revenue You forget it was specifically the deregulation of the financial market that caused the toxic asset crisis which caused the real estate bubble to burst and it was the over valuation of real estate that created that bubble"));
    trecCorpus.append(trecDocument("10", "subprime crisis and it can be tackled by flooding the system with newly created money Scarcely do they see that instead of being a real estate crisis a stock market crisis or a banking crisis this is a gold crisis It can only be resolved by involving gold in particular"));
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

    File[] files = new File[3];
    files[0] = trecCorpusFile;
    files[1] = corpusFile;
    files[2] = indexFile;
    return files;
  }
  public static String trecDocument(String docno, String text) {
    return "<DOC>\n<DOCNO>" + docno + "</DOCNO>\n"
            + "<TEXT>\n" + text + "</TEXT>\n</DOC>\n";
  }

}
