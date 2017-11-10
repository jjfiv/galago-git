/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.StreamUtil;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import java.lang.System;

//import static org.junit.rules.TemporaryFolder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;



/**
 * @author smh
 */
public class AnnotationsGenerateFnTest {

  private File indexFile = null;
  private File trecCorpusFile = null;
  private String indexFilePath = null;
  private String trecCorpusFilePath = null;
    
  //private final String tmpDirStr = System.getProperty ("java.io.tmpdir");

  //- Simple Text Doc
  private final static String textDocument =     "A text document\n" +
					         "ID: txt-1\n" +
                                                 "George Shultz used to live in Boston Massachusetts.\n" +
                                                 "He used to hang out in Washington D.C. with Ronald Reagan\n" +
                                                 "I think.\n" +
                                                 "They were both members of the Republican Party.\n";

  //- XML Doc
  private final static String xmlDocument =      "<document>\n" +
                                                 "<title>XML Document on G.W. Carver</title>\n" +
                                                 "<person>George Washington Carver</person> lived in\n" +
					         "<location>Georgia</location>, <location>Missouri</location>\n" +
					         "and <location>Iowa, USA</location>\n" +
                                                 "He had interactions with <organization>NAACP</organization>.\n" +
                                                 "</document>\n";

  //- TREC Text Doc
  private final static String trecTextDocument = "<DOC>\n" +
                                                 "<DOCNO>tt-1</DOCNO>\n" +
                                                 "<TITLE>Federal Authorities Investigate Explosion Killing Four Youths</TITLE>\n" +
                                                 "<TEXT>\n" +
                                                 "The Bureau of Alcohol Tobacco and Firearms is investigating an \n" +
                                                 "explosion that killed four Maryland teen-agers who were apparently \n" +
                                                 "experimenting with explosives, says State Police spokesperson\n" +
                                                 "Trooper John Smith.\n" +
                                                 "</TEXT>\n" +
                                                 "</DOC>\n";

  //- TREC Web Doc
  private final static String trecWebDocument =  "<DOC>\n" +
                                                 "<DOCNO>tw-04</DOCNO>\n" +
                                                 "<DOCHDR>\n" +
                                                 "http://myhost/trecweb/test/\n" +
                                                 "</DOCHDR>\n" +
                                                 "Tom Jones of the Society of Manufacturing Engineers said Ms. Smith \n" +
                                                 "has been recognized as an Outstanding Young Manufacturing Engineer. \n" +
						 "She resides in Paducah Kentucky. \n" +
                                                 "</DOC>\n";

  //- TREC Multi Document
  private final static String multiDocument =    "<DOC>\n" +
                                                 "<DOCNO>tt-1</DOCNO>\n" +
                                                 "<TITLE>Federal Authorities Investigate Explosion Killing Four Youths</TITLE>\n" +
                                                 "<TEXT>\n" +
                                                 "The Bureau of Alcohol Tobacco and Firearms is investigating an\n" +
                                                 "explosion that killed four Maryland teen-agers who were apparently\n" +
                                                 "experimenting with explosives.\n" +
                                                 "</TEXT>\n" +
                                                 "</DOC>\n\n" +

                                                 "<DOC>\n" +
                                                 "<DOCNO>tt-0002</DOCNO>\n" +
                                                 "<TITLE>University Erects A Factory Of The Future</TITLE>\n" +
                                                 "<TEXT>\n" +
                                                 "For students working in a miniature factory at the University\n" +
                                                 "of Missouri-Rolla, the future of american business is now.\n" +
                                                 "</TEXT>\n" +
                                                 "</DOC>\n\n" +

                                                 "<DOC>\n" +
                                                 "<DOCNO>tt-0235</DOCNO>\n" +
                                                 "<TITLE>Chrysler to Produce V-10 Engine</TITLE>\n" +
                                                 "<TEXT>\n" +
                                                 "Chrysler Motors Corp. says it will make a 300-horsepower, V-10\n" +
                                                 "engine during the 1990s.F\n" +
                                                 "</TEXT>\n" +
                                                 "</DOC>\n";

  //- TREC Web Doc with no entities
  private final static String nuthinDocument =   "<DOC>\n" +
                                                 "<DOCNO>tw-no-entities</DOCNO>\n" +
                                                 "<DOCHDR>\n" +
                                                 "http://myhost/trecweb/nuthin/\n" +
                                                 "</DOCHDR>\n" +
                                                 "TREC web stories about stuff.\n" +
                                                 "Life is like a box of chocolates.\n" +
                                                 "You just never know what you're going to get!\n" +
                                                 "</DOC>\n";
  //- Twitter doc						       
  private final static String twitterDocument = "uid-0 \tnow\t 0 4 9 10 11 3 \tfaked\n";

    
  @Before
  public void setUp() throws Exception {

    String trecCorpus = AppTest.trecDocument ("tt-1", "<TITLE>Federal Authorities Investigate Explosion"
					    + "Killing Four Youths</TITLE>\n<TEXT>The Bureau of Alcohol Tobacco and Firearms"
					    + "is investigating an explosion that killed four Maryland teen-agers"
					    + "who were apparently experimenting with explosives.</TEXT>")

                      + AppTest.trecDocument ("tt-0002", "<TITLE>University Erects A"
                                            + " Factory Of The Future</TITLE>\n<TEXT>For students working in a miniature"
                                            + " factory at the University of Missouri-Rolla, the future of"
                                            + " American business is now.</TEXT>")

                      + AppTest.trecDocument ("tt-0235", "<TITLE>Chrysler to Produce"
                                            + " V-10 Engine</TITLE>\n<TEXT>Chrysler Motors Corp. says it will make"
                                            + " a 300-horsepower, V-10 engine during the 1990s.</TEXT>");

    trecCorpusFile = FileUtility.createTemporary ();
    StreamUtil.copyStringToFile (trecCorpus, trecCorpusFile);
    trecCorpusFilePath = trecCorpusFile.getAbsolutePath ();

    indexFile = FileUtility.createTemporaryDirectory ();
    indexFilePath = indexFile.getAbsolutePath ();

    // Build the index
    App.main (new String[]{"build", "--indexPath=" + indexFilePath,
                                    "--inputPath=" + trecCorpusFilePath,
                                    "--tokenizer/fields+title"});

    // Checks path and components
    AppTest.verifyIndexStructures (indexFile);

  }  //- end Setup

    
  @After
  public void tearDown() throws IOException {

    if (trecCorpusFile != null) {
      trecCorpusFile.delete ();
    }

    if (indexFile != null) {
      FSUtil.deleteDirectory (indexFile);
    }

  }  //- end TearDown

    
  @Test
  public void testSingleIID () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream ();
    PrintStream printStream = new PrintStream (byteArrayStream);

    // Test single IID.  Note trailing comma is needed in the iidList.
    App.run (new String[]{ "annotations-generate",
			   "--indexPath=" + indexFilePath,
			   "--iidList=1,",
	                   "--annotationTypes=person,location,organization"
	                 }, printStream);

    String output = byteArrayStream.toString ();
    String[] outputLines = output.split (System.lineSeparator ());

    /*
    //- What's the output  
    System.out.println ("Output Length: " + outputLines.length);
    for (int i=0; i<outputLines.length; i++) {
      System.out.println ("Output[" + i + "]: " + outputLines[i]);
    }
    */

    // Check that output and expected line counts agree.  Should be 2 lines.
    assertTrue ("Output should contain " + outputLines.length, outputLines.length == 2);

    // Expected lines
    String[] expectedLines = { "Doc [1]  tt-0002",
			       indexFilePath + "  tt-0002  organization university_of_missouri-rolla 16 18 121 149"
                             };

    int i = 0;
    for (String line : expectedLines) {
      String expectedL = line.trim ();
      String outputL = outputLines[i].trim ();
      assertTrue ("Output line [" + i + "] doesn't match expected", expectedL.equals (outputL));
      i++;
    }

  }  //- end testSingleIID


  @Test
  public void testSingleEID () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream ();
    PrintStream printStream = new PrintStream (byteArrayStream);

    //- Use annotationType "all" instead of types list
    App.run (new String[]{ "annotations-generate",
			   "--indexPath=" + indexFilePath,
			   "--eidList=tt-0235,",
	                   "--annotationTypes=all"
	                 }, printStream);

    String output = byteArrayStream.toString ();
    String[] outputLines = output.split (System.lineSeparator ());

    // Expecting 3 lines.
    assertTrue ("Output should contain " + outputLines.length, outputLines.length == 3);

    //- Expected lines
    String[] expectedLines = { "Doc [2]  tt-0235 ",
                               indexFilePath + "  tt-0235  organization chrysler 0 1 14 22",
                               indexFilePath + "  tt-0235  organization chrysler_motors_corp. 5 7 60 81"
                             };

    int i = 0;
    for (String line : expectedLines) {
      String outputL = outputLines[i].trim();
      String expectedL = line.trim();
      assertTrue ("Output line [" + i + "] doesn't match expected", expectedL.equals (outputL));
      i++;
    }

  }  //- end testSingleEID


  @Test
  public void testUndefIID () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream ();
    PrintStream printStream = new PrintStream (byteArrayStream);

    // Test for undefined IID.  Single IID needs trailing comma.
    App.run (new String[]{ "annotations-generate",
			   "--indexPath=" + indexFilePath,
			   "--iidList=999,",
	                   "--annotationTypes=person,location,organization"
	                 }, printStream);

    String outputLine = byteArrayStream.toString ().trim ();

    // Expected line
    String expectedLine = "IID 999 does not exist in the index.  Skipping.";
    assertTrue ("Output line [" + expectedLine + "] doesn't match expected", expectedLine.equals (outputLine));

  }  //- end testUndefIID


  @Test
  public void testUndefEID () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream ();
    PrintStream printStream = new PrintStream (byteArrayStream);

    // Test for undefined EID
    App.run (new String[]{ "annotations-generate",
			   "--indexPath=" + indexFilePath,
			   "--eidList=XYZ",
	                   "--annotationTypes=ALL"
	                 }, printStream);

    String outputLine = byteArrayStream.toString ().trim ();

    // Expected line
    String expectedLine = "EID XYZ does not exist in the index.  Skipping.";
    assertTrue ("Output line [" + outputLine + "] doesn't match expected", expectedLine.equals (outputLine));

  }  //- end testUndefEID


  @Test
  public void testIIDsandEIDs () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream ();
    PrintStream printStream = new PrintStream (byteArrayStream);

    // Test for IID and EID list combination
    App.run (new String[]{ "annotations-generate",
			   "--indexPath=" + indexFilePath,
                           "--iidList=1,2",
			   "--eidList=tt-0235,tt-1",
	                   "--annotationTypes=organization,location"}, printStream);

    String output = byteArrayStream.toString ();
    String[] outputLines = output.split(System.lineSeparator());

    /*
    //- What's the output  
    System.out.println ("Output Length: " + outputLines.length);
    for (int i=0; i<outputLines.length; i++) {
      System.out.println ("Output[" + i + "]: " + outputLines[i]);
    }
    */

    //- Output should be 5 lines for three docs.  One of the docs was listed as IID and EID.
    assertTrue ("Output should contain " + outputLines.length, outputLines.length == 8);

    // Expected lines
    String[] expectedLines = { "Doc [0]  tt-1 ",
                               indexFilePath + "  tt-1  organization bureau_of_alcohol_tobacco 7 10 93 118",
                               indexFilePath + "  tt-1  location maryland 19 20 178 186",
			       "Doc [1]  tt-0002 ",
                               indexFilePath + "  tt-0002  organization university_of_missouri-rolla 16 18 121 149",
                               "Doc [2]  tt-0235 ",
                               indexFilePath + "  tt-0235  organization chrysler 0 1 14 22",
                               indexFilePath + "  tt-0235  organization chrysler_motors_corp. 5 7 60 81"
                             };

    int i = 0;
    for (String line : expectedLines) {
      String expectedL = line.trim();
      String outputL = outputLines[i].trim();
      assertEquals (expectedL, outputL);
      i++;
    }

  }  //- end testIIDandEID


  @Test
  public void testTrecTextOrganization () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream ();
    PrintStream printStream = new PrintStream (byteArrayStream);

    File trecTextFile = FileUtility.createTemporary ();
    String trecTextFilePath = trecTextFile.getAbsolutePath ();
    StreamUtil.copyStringToFile (trecTextDocument, trecTextFile);
    
    //- Test for trectext format document annotations.
    App.run (new String[]{ "annotations-generate",
			   "--inputFiles=" + trecTextFilePath,
                           "--annotationTypes=organization"
	                 }, printStream);


    String output = byteArrayStream.toString ();
    String[] outputLines = output.split (System.lineSeparator ());

    /*
    //- What's the output  

    for (int i=0; i<outputLines.length; i++) {
      System.out.println ("Output[" + i + "]: " + outputLines[i]);
    }
    */

    //- Expected lines
    String[] expectedLines = { trecTextFilePath + "  tt-1  organization bureau_of_alcohol_tobacco_and_firearms 8 13 88 126",
			       trecTextFilePath + "  tt-1  organization state_police 31 32 252 264",
                             };

    //- Output should be 2 lines.
    assertTrue ("Output should contain " + expectedLines.length, outputLines.length == 2);

    int i = 0;
    for (String line : expectedLines) {
      String expectedL = line.trim();
      String outputL = outputLines[i].trim();
      assertEquals (expectedL, outputL);
      i++;
    }

    //- Clean up
    if (!trecTextFile.delete ()) {
      throw new IOException ("Couldn't delete trectext file: " + trecTextFilePath);
    }

  }  //- end testTrecTextOrganization


  @Test
  public void testTrecTextPerson () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream ();
    PrintStream printStream = new PrintStream (byteArrayStream);

    File trecTextFile = FileUtility.createTemporary ();
    String trecTextFilePath = trecTextFile.getAbsolutePath ();
    StreamUtil.copyStringToFile (trecTextDocument, trecTextFile); 
    
    //- Test for trectext format document annotations.
    App.run (new String[]{ "annotations-generate",
			   "--inputFiles=" + trecTextFilePath,
                           "--annotationTypes=person"
	                 }, printStream);

    String output = byteArrayStream.toString ();
    String outputLine = output.trim ();

    // Expected line
    String expectedLine = trecTextFilePath + "  tt-1  person john_smith 35 36 286 296";

    assertEquals (expectedLine, outputLine);

    //- Clean up
    if (!trecTextFile.delete ()) {
      throw new IOException ("Couldn't delete trectext file: " + trecTextFilePath);
    }

  }  //- end testTrecTextPerson

    
  @Test
  public void testTrecTextLocation () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream ();
    PrintStream printStream = new PrintStream (byteArrayStream);

    File trecTextFile = FileUtility.createTemporary ();
    String trecTextFilePath = trecTextFile.getAbsolutePath ();
    StreamUtil.copyStringToFile (trecTextDocument, trecTextFile);
    
    //- Test for trectext format document annotations.
    App.run (new String[]{ "annotations-generate",
			   "--inputFiles=" + trecTextFilePath,
                           "--annotationTypes=location"
	                 }, printStream);


    String output = byteArrayStream.toString ();
    String outputLine = output.trim ();

    // Expected line
    String expectedLine = trecTextFilePath + "  tt-1  location maryland 21 22 175 183";
    assertEquals (expectedLine, outputLine);

    //- Clean up
    if (!trecTextFile.delete ()) {
      throw new IOException ("Couldn't delete trectext file: " + trecTextFilePath);
    }

  }  //- end testTrecTextLocation


  @Test
  public void testTrecTextALL () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream ();
    PrintStream printStream = new PrintStream (byteArrayStream);

    File trecTextFile = FileUtility.createTemporary ();
    String trecTextFilePath = trecTextFile.getAbsolutePath ();
    StreamUtil.copyStringToFile (trecTextDocument, trecTextFile);
    
    //- Test for trectext format document annotations.
    App.run (new String[]{ "annotations-generate",
			   "--inputFiles=" + trecTextFilePath,
                           "--annotationTypes=All"
	                 }, printStream);

    String output = byteArrayStream.toString ();
    String[] outputLines = output.split (System.lineSeparator ());

    /*
    //- What's the output  
    System.out.println ("Output Length: " + outputLines.length);
    for (int i=0; i<outputLines.length; i++) {
      System.out.println ("Output[" + i + "]: " + outputLines[i]);
    }
    */

    // Expected lines
    String[] expectedLines = { trecTextFilePath + "  tt-1  organization bureau_of_alcohol_tobacco_and_firearms 8 13 88 126",
                               trecTextFilePath + "  tt-1  location maryland 21 22 175 183",
                               trecTextFilePath + "  tt-1  organization state_police 31 32 252 264",
                               trecTextFilePath + "  tt-1  person john_smith 35 36 286 296"
                             };

    //- Output should be 4 lines.
    assertTrue ("Output should contain " + expectedLines.length, outputLines.length == 4);

    int i = 0;
    for (String line : expectedLines) {
      String expectedL = line.trim();
      String outputL = outputLines[i].trim();
      assertEquals (expectedL, outputL);
      i++;
    }

    //- Clean up
    if (!trecTextFile.delete ()) {
      throw new IOException ("Couldn't delete trectext file: " + trecTextFilePath);
    }

  }  //- end testTrecTextALL

    
  public void testTrecWeb () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream ();
    PrintStream printStream = new PrintStream (byteArrayStream);

    File trecWebFile = FileUtility.createTemporary ();
    String trecWebFilePath = trecWebFile.getAbsolutePath ();
    StreamUtil.copyStringToFile (trecWebDocument, trecWebFile);
    
    //- Test for trectext format document annotations.
    App.run (new String[]{ "annotations-generate",
                           "--inputFiles=" + trecWebFilePath,
                           "--annotationTypes=ALL"
	                 }, printStream);


    String output = byteArrayStream.toString ();
    String[] outputLines = output.split (System.lineSeparator ());

    /*
    //- What's the output  
    System.out.println ("Output Length: " + outputLines.length);
    for (int i=0; i<outputLines.length; i++) {
      System.out.println ("Output[" + i + "]: " + outputLines[i]);
    }
    */

    // Expected lines
    String[] expectedLines = { trecWebFilePath + "  tw-04  person tom_jones 0 1 0 9",
                               trecWebFilePath + "  tw-04  organization society_of_manufacturing_engineers 4 7 17 51",
                               trecWebFilePath + "  tw-04  person smith 10 11 61 66",
                               trecWebFilePath + "  tw-04  location paducah_kentucky 24 25 150 166"
                             };

    //- Output should be 4 lines.
    assertTrue ("Output should contain " + expectedLines.length, outputLines.length == 4);

    int i = 0;
    for (String line : expectedLines) {
      String expectedL = line.trim();
      String outputL = outputLines[i].trim();
      assertEquals (expectedL, outputL);
      i++;
    }

    //- Clean up
    if (!trecWebFile.delete ()) {
      throw new IOException ("Couldn't delete trecweb file: " + trecWebFilePath);
    }

  }  //- end testTrecWebFile


  public void testTextFile () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream ();
    PrintStream printStream = new PrintStream (byteArrayStream);

    File textFile = FileUtility.createTemporary ();
    String textFilePath = textFile.getAbsolutePath ();
    StreamUtil.copyStringToFile (textDocument, textFile);
    
    //- Test for text format document annotations.
    App.run (new String[]{ "annotations-generate",
                           "--inputFiles=" + textFilePath,
                           "--annotationTypes=ALL"
	                 }, printStream);

    String output = byteArrayStream.toString ();
    String[] outputLines = output.split (System.lineSeparator ());

    /*
    //- What's the output  
    System.out.println ("Output Length: " + outputLines.length);
    for (int i=0; i<outputLines.length; i++) {
      System.out.println ("Output[" + i + "]: " + outputLines[i]);
    }
    */

    // Expected lines
    String[] expectedLines = { textFilePath + "  " + textFilePath + "  person george_shultz 6 7 26 39",
                               textFilePath + "  " + textFilePath + "  location boston_massachusetts 12 13 56 76",
                               textFilePath + "  " + textFilePath + "  location washington_d.c. 21 22 101 116",
                               textFilePath + "  " + textFilePath + "  person ronald_reagan 24 25 122 135",
                               textFilePath + "  " + textFilePath + "  organization republican_party 35 36 175 191"
                             };

    //- Output should be 5 lines.
    assertTrue ("Output should contain " + expectedLines.length, outputLines.length == 5);

    int i = 0;
    for (String line : expectedLines) {
      String expectedL = line.trim();
      String outputL = outputLines[i].trim();
      assertEquals (expectedL, outputL);
      i++;
    }

    //- Clean up
    if (!textFile.delete ()) {
      throw new IOException ("Couldn't delete text file: " + textFilePath);
    }

  }  //- end testTextFile


  public void testXMLFile () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream ();
    PrintStream printStream = new PrintStream (byteArrayStream);

    File xmlFile = FileUtility.createTemporary ();
    String xmlFilePath = xmlFile.getAbsolutePath ();
    StreamUtil.copyStringToFile (xmlDocument, xmlFile);
    
    //- Test for trectext format document annotations.
    App.run (new String[]{ "annotations-generate",
                           "--inputFiles=" + xmlFilePath,
                           "--annotationTypes=ALL"
	                 }, printStream);

    String output = byteArrayStream.toString ();
    String[] outputLines = output.split (System.lineSeparator ());

    /*
    //- What's the output  
    System.out.println ("Output Length: " + outputLines.length);
    for (int i=0; i<outputLines.length; i++) {
      System.out.println ("Output[" + i + "]: " + outputLines[i]);
    }
    */

    //- Expected lines
    String[] expectedLines = { xmlFilePath + "  " + xmlFilePath + "  person g.w._carver_george_washington_carver 3 7 34 86",
                               xmlFilePath + "  " + xmlFilePath + "  location georgia 10 11 115 122",
                               xmlFilePath + "  " + xmlFilePath + "  location missouri 12 13 145 153",
                               xmlFilePath + "  " + xmlFilePath + "  location iowa 14 15 179 183",
                               xmlFilePath + "  " + xmlFilePath + "  location usa 16 17 185 188",
                               xmlFilePath + "  " + xmlFilePath + "  organization naacp 21 22 239 244"
                             };

    //- Output should be 6 lines.
    assertTrue ("Output should contain " + expectedLines.length, outputLines.length == 6);

    int i = 0;
    for (String line : expectedLines) {
      String expectedL = line.trim();
      String outputL = outputLines[i].trim();
      assertEquals (expectedL, outputL);
      i++;
    }

    //- Clean up
    if (!xmlFile.delete ()) {
      throw new IOException ("Couldn't delete XML file: " + xmlFilePath);
    }

  }  //- end testXMLFile


  public void testTrecTextMultiDocs () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream ();
    PrintStream printStream = new PrintStream (byteArrayStream);

    File multiDocFile = FileUtility.createTemporary ();
    String multiDocFilePath = multiDocFile.getAbsolutePath ();
    StreamUtil.copyStringToFile (multiDocument, multiDocFile);
    
    //- Test for trectext format document annotations.
    App.run (new String[]{ "annotations-generate",
                           "--inputFiles=" + multiDocFilePath,
                           "--annotationTypes=ALL"
	                 }, printStream);

    String output = byteArrayStream.toString ();
    String[] outputLines = output.split (System.lineSeparator ());

    /*
    //- What's the output  
    System.out.println ("Output Length: " + outputLines.length);
    for (int i=0; i<outputLines.length; i++) {
      System.out.println ("Output[" + i + "]: " + outputLines[i]);
    }
    */

    //- Expected lines
    String[] expectedLines = { multiDocFilePath + "  " + multiDocFilePath + "  tt-1  organization bureau_of_alcohol_tobacco_and_firearms 8 13 88 126",
                               multiDocFilePath + "  " + multiDocFilePath + "  tt-1  location maryland 21 22 174 182",
                               multiDocFilePath + "  " + multiDocFilePath + "  tt-0002  organization university_of_missouri-rolla 16 18 115 143",
                               multiDocFilePath + "  " + multiDocFilePath + "  tt-0235  organization chrysler 0 1 7 15",
                               multiDocFilePath + "  " + multiDocFilePath + "  tt-0235  organization chrysler_motors_corp. 5 7 54 75"
                             };

    //- Output should be 5 lines.
    assertTrue ("Output should contain " + expectedLines.length, outputLines.length == 5);

    int i = 0;
    for (String line : expectedLines) {
      String expectedL = line.trim();
      String outputL = outputLines[i].trim();
      assertEquals (expectedL, outputL);
      i++;
    }

    //- Clean up
    if (!multiDocFile.delete ()) {
      throw new IOException ("Couldn't delete multi document file: " + multiDocFilePath);
    }

  }  //- end testTrecTextMultiDocs


  public void testNoEntitiesDoc () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream ();
    PrintStream printStream = new PrintStream (byteArrayStream);

    File nuthinDocFile = FileUtility.createTemporary ();
    String nuthinDocFilePath = nuthinDocFile.getAbsolutePath ();
    StreamUtil.copyStringToFile (nuthinDocument, nuthinDocFile);
    
    //- Test for trectext format document annotations.
    App.run (new String[]{ "annotations-generate",
                           "--inputFiles=" + nuthinDocFilePath,
                           "--annotationTypes=ALL"
	                 }, printStream);

    String outputLine = byteArrayStream.toString ();

    //- Expected line
    String expectedLine =  nuthinDocFilePath + "  " + "  [tw-no-entities]  No extractions this document.";

    assertEquals (expectedLine, outputLine);

    //- Clean up
    if (!nuthinDocFile.delete ()) {
      throw new IOException ("Couldn't delete no entities document file: " + nuthinDocFilePath);
    }

  }  //- end testNoEntitiesDoc

    
} //- end class AnnotationsGenerateFnTest
