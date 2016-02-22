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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * @author sjh, smh
 */
public class DumpDocTermsFnTest {

  File trecCorpusFile = null;
  File indexFile = null;
  File childPath = null;
  File childPathStemmed = null;

  @Before
  public void setUp() throws Exception {


    String trecCorpus = AppTest.trecDocument ("AP890101-0001", "First doc.\n<HEAD>60s films are here</HEAD>"
                                            + " The celluloid torch has been passed to a new generation:"
                                            + " filmmakers who grew up in the 1960s.")

                      + AppTest.trecDocument ("AP890101-0002", "Second doc.\n<HEAD>University Erects A"
                                            + " Factory Of The Future</HEAD>\nFor students working in a miniature"
                                            + " factory at the University of Missouri-Rolla, the future of"
                                            + " American business is now.")

                      + AppTest.trecDocument ("AP890104-0235", "The third doc.\n<HEAD>Chrysler to Produce"
                                            + " V-10 Engine</HEAD>\nChrysler Motors Corp. says it will make"
                                            + " a 300-horsepower, V-10 engine during the 1990s.");

    trecCorpusFile = FileUtility.createTemporary ();
    StreamUtil.copyStringToFile (trecCorpus, trecCorpusFile);

    indexFile = FileUtility.createTemporaryDirectory ();

    // Build the index
    App.main (new String[]{"build", "--indexPath=" + indexFile.getAbsolutePath(),
                                    "--inputPath=" + trecCorpusFile.getAbsolutePath(),
                                    "--tokenizer/fields+head"});

    // Checks path and components
    AppTest.verifyIndexStructures (indexFile);

    // Test using full postings
    childPath = new File (indexFile, "postings");
    assertTrue (childPath.exists ());

  }  //- end Setup

    
  @After
  public void tearDown() throws IOException {

    if (trecCorpusFile != null) {
      trecCorpusFile.delete();
    }

    if (indexFile != null) {
      FSUtil.deleteDirectory (indexFile);
    }

  }  //- end TearDown

    
  @Test
  public void testSingleIID () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(byteArrayStream);

    // Test single IID.  Note trailing comma is needed in the iidList.
    App.run (new String[]{ "dump-doc-terms",
			   "--index=" + childPath.getAbsolutePath(),
			   "--iidList=1," }, printStream);

    String output = byteArrayStream.toString ();
    String[] outputLines = output.split(System.lineSeparator());

    /*
    //- What's the output  
    System.out.println ("Output Length: " + outputLines.length);
    for (int i=0; i<outputLines.length; i++) {
      System.out.println ("Output[" + i + "]: " + outputLines[i]);
    }
    */

    // Check that output and expected line counts agree.  Should be 22 lines.
    assertTrue ("Output should contain " + outputLines.length, outputLines.length == 22);

    // Expected lines
    String[] expectedLines = { "Doc: 1 [AP890101-0002]\tTerm Count: 21\tTotal Words: 29\tMax TF: 3",
			       "\ta,1,4,13",
			       "\tamerican,1,25",
			       "\tat,1,16",
			       "\tbusiness,1,26",
			       "\tdoc,1,1",
			       "\terects,1,3",
			       "\tfactory,1,5,15",
			       "\tfor,1,9",
			       "\tfuture,1,8,23",
			       "\tin,1,12",
			       "\tis,1,27",
			       "\tminiature,1,14",
			       "\tmissouri,1,20",
			       "\tnow,1,28",
			       "\tof,1,6,19,24",
			       "\trolla,1,21",
			       "\tsecond,1,0",
			       "\tstudents,1,10",
			       "\tthe,1,7,17,22",
			       "\tuniversity,1,2,18",
			       "\tworking,1,11"
                             };

    int i = 0;
    for (String line : expectedLines) {
      String expectedL = line.trim();
      String outputL = outputLines[i].trim();
	   assertTrue ("Output line [" + i + "] doesn't match expected",
		            expectedL.equals (outputL));
	   i++;
    }

  }  //- end testSingleIID


  @Test
  public void testSingleEID () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(byteArrayStream);

    // test non-stemmed
    App.run (new String[]{ "dump-doc-terms",
			   "--index=" + childPath.getAbsolutePath(),
			   "--eidList=AP890101-0002" }, printStream);

    //printStream.println ("PrintStream Output: " + byteArrayStream.toString());
    String output = byteArrayStream.toString ();
    String[] outputLines = output.split(System.lineSeparator());

    // Expecting 22 lines.
    assertTrue ("Output should contain " + outputLines.length, outputLines.length == 22);

    // Expected lines
    String[] expectedLines = { "Doc: 1 [AP890101-0002]\tTerm Count: 21\tTotal Words: 29\tMax TF: 3",
			       "\ta,1,4,13",
			       "\tamerican,1,25",
			       "\tat,1,16",
			       "\tbusiness,1,26",
			       "\tdoc,1,1",
			       "\terects,1,3",
			       "\tfactory,1,5,15",
			       "\tfor,1,9",
			       "\tfuture,1,8,23",
			       "\tin,1,12",
			       "\tis,1,27",
			       "\tminiature,1,14",
			       "\tmissouri,1,20",
			       "\tnow,1,28",
			       "\tof,1,6,19,24",
			       "\trolla,1,21",
			       "\tsecond,1,0",
			       "\tstudents,1,10",
			       "\tthe,1,7,17,22",
			       "\tuniversity,1,2,18",
			       "\tworking,1,11"
                             };

    int i = 0;
    for (String line : expectedLines) {
      String outputL = outputLines[i].trim();
      String expectedL = line.trim();
	   assertTrue ("Output line [" + i + "] doesn't match expected",
		            expectedL.equals (outputL));
	   i++;
    }

  }  //- end testSingleEID


  @Test
  public void testTwoIIDs () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(byteArrayStream);

    // Test a list of IIDs.  Note trailing comma in listing isn't needed this time.
    App.run (new String[]{ "dump-doc-terms",
			   "--index=" + childPath.getAbsolutePath(),
			   "--iidList=0,2" }, printStream);

    String output = byteArrayStream.toString ();
    String[] outputLines = output.split(System.lineSeparator());

    // Check that output and expected line counts agree.  Should be 45 lines.
    assertTrue ("Expected 45 output lines but got " + outputLines.length, outputLines.length == 45);

    // Expected lines
    String[] expectedLines = { "Doc: 0 [AP890101-0001]\tTerm Count: 22\tTotal Words: 23\tMax TF: 2",
			                      "\t1960s,0,22",
                               "\t60s,0,2",
                               "\ta,0,13",
                               "\tare,0,4",
                               "\tbeen,0,10",
                               "\tcelluloid,0,7",
                               "\tdoc,0,1",
                               "\tfilmmakers,0,16",
                               "\tfilms,0,3",
                               "\tfirst,0,0",
                               "\tgeneration,0,15",
                               "\tgrew,0,18",
                               "\thas,0,9",
                               "\there,0,5",
                               "\tin,0,20",
                               "\tnew,0,14",
                               "\tpassed,0,11",
                               "\tthe,0,6,21",
                               "\tto,0,12",
                               "\ttorch,0,8",
                               "\tup,0,19",
                               "\twho,0,17",
                               "",
                               "Doc: 2 [AP890104-0235]\tTerm Count: 20\tTotal Words: 25\tMax TF: 2",
                               "\t10,2,7,20",
                               "\t1990s,2,24",
                               "\t300,2,17",
                               "\ta,2,16",
                               "\tchrysler,2,3,9",
                               "\tcorp,2,11",
                               "\tdoc,2,2",
                               "\tduring,2,22",
                               "\tengine,2,8,21",
                               "\thorsepower,2,18",
                               "\tit,2,13",
                               "\tmake,2,15",
                               "\tmotors,2,10",
                               "\tproduce,2,5",
                               "\tsays,2,12",
                               "\tthe,2,0,23",
                               "\tthird,2,1",
                               "\tto,2,4",
                               "\tv,2,6,19",
                               "\twill,2,14"
                             };

    int i = 0;
    for (String line : expectedLines) {
      String expectedL = line.trim();
      String outputL = outputLines[i].trim();
      assertEquals (expectedL, outputL);
      //assertEquals (expectedLines[i], outputLines[i].trim());
	   i++;
    }

  }  //- end testTwoIIDs


  @Test
  public void testTwoEIDs () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(byteArrayStream);

    // Test list of two EIDs
    App.run (new String[]{ "dump-doc-terms",
			   "--index=" + childPath.getAbsolutePath(),
			   "--eidList=AP890101-0001,AP890104-0235" }, printStream);

    String output = byteArrayStream.toString ();
    String[] outputLines = output.split(System.lineSeparator());

    // Output should be have 45 lines.
    assertTrue ("Output should contain " + outputLines.length, outputLines.length == 45);

    // Expected lines
    String[] expectedLines = { "Doc: 0 [AP890101-0001]\tTerm Count: 22\tTotal Words: 23\tMax TF: 2",
			                      "\t1960s,0,22",
                               "\t60s,0,2",
                               "\ta,0,13",
                               "\tare,0,4",
                               "\tbeen,0,10",
                               "\tcelluloid,0,7",
                               "\tdoc,0,1",
                               "\tfilmmakers,0,16",
                               "\tfilms,0,3",
                               "\tfirst,0,0",
                               "\tgeneration,0,15",
                               "\tgrew,0,18",
                               "\thas,0,9",
                               "\there,0,5",
                               "\tin,0,20",
                               "\tnew,0,14",
                               "\tpassed,0,11",
                               "\tthe,0,6,21",
                               "\tto,0,12",
                               "\ttorch,0,8",
                               "\tup,0,19",
                               "\twho,0,17",
                               "",
                               "Doc: 2 [AP890104-0235]\tTerm Count: 20\tTotal Words: 25\tMax TF: 2",
                               "\t10,2,7,20",
                               "\t1990s,2,24",
                               "\t300,2,17",
                               "\ta,2,16",
                               "\tchrysler,2,3,9",
                               "\tcorp,2,11",
                               "\tdoc,2,2",
                               "\tduring,2,22",
                               "\tengine,2,8,21",
                               "\thorsepower,2,18",
                               "\tit,2,13",
                               "\tmake,2,15",
                               "\tmotors,2,10",
                               "\tproduce,2,5",
                               "\tsays,2,12",
                               "\tthe,2,0,23",
                               "\tthird,2,1",
                               "\tto,2,4",
                               "\tv,2,6,19",
                               "\twill,2,14"
                             };

    int i = 0;
    for (String line : expectedLines) {
      String expectedL = line.trim();
      String outputL = outputLines[i].trim();
      assertEquals (expectedL, outputL);
	   i++;
    }

  }  //- end testTwoEIDs


  @Test
  public void testUndefIID () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(byteArrayStream);

    // Test for undefined IID.  Single IID needs trailing comma.
    App.run (new String[]{ "dump-doc-terms",
			   "--index=" + childPath.getAbsolutePath(),
			   "--iidList=999," }, printStream);

    String output = byteArrayStream.toString ();
    String[] outputLines = output.split("\n");

    // Output should be a single line.
    assertTrue ("Output should contain " + outputLines.length, outputLines.length == 1);

    // Expected lines
    String[] expectedLines = { "Doc: 999\t[ --- ]\tDoc ID does not exist" };

    int i = 0;
    for (String line : expectedLines) {
      String expectedL = line.trim();
      String outputL = outputLines[i].trim();
	   assertTrue ("Output line [" + i + "] doesn't match expected",
		            expectedL.equals (outputL));
	   i++;
    }

  }  //- end testUndefIID


  @Test
  public void testUndefEID () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(byteArrayStream);

    // Test for undefined EID
    App.run (new String[]{ "dump-doc-terms",
			   "--index=" + childPath.getAbsolutePath(),
			   "--eidList=XYZ" }, printStream);

    String output = byteArrayStream.toString ();
    String[] outputLines = output.split("\n");

    // Only single output line expected
    assertTrue ("Output should contain " + outputLines.length, outputLines.length == 1);

    // Expected lines
    String[] expectedLines = { "Doc: ---\t[XYZ]\tDoc ID does not exist" };

    int i = 0;
    for (String line : expectedLines) {
      String expectedL = line.trim();
      String outputL = outputLines[i].trim();
      assertTrue ("Output line [" + i + "] doesn't match expected",
		            expectedL.equals (outputL));
	   i++;
    }

  }  //- end testUndefEID


  @Test
  public void testIIDandEIDwithOverlap () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(byteArrayStream);

    // Test for IID and EID list combination
    App.run (new String[]{ "dump-doc-terms",
			   "--index=" + childPath.getAbsolutePath(),
                           "--iidList=0,1",
			   "--eidList=AP890104-0235,AP890101-0002" }, printStream);

    String output = byteArrayStream.toString ();
    String[] outputLines = output.split(System.lineSeparator());

    /*
    //- What's the output  
    System.out.println ("Output Length: " + outputLines.length);
    for (int i=0; i<outputLines.length; i++) {
      System.out.println ("Output[" + i + "]: " + outputLines[i]);
    }
    */

    // Output should be 68 lines containing three docs.  One of the docs is listed twice as IID and EID.
    assertTrue ("Output should contain " + outputLines.length, outputLines.length == 68);

    // Expected lines
    String[] expectedLines = { "Doc: 0 [AP890101-0001]\tTerm Count: 22\tTotal Words: 23\tMax TF: 2",
                               "\t1960s,0,22",
                               "\t60s,0,2",
                               "\ta,0,13",
                               "\tare,0,4",
                               "\tbeen,0,10",
                               "\tcelluloid,0,7",
                               "\tdoc,0,1",
                               "\tfilmmakers,0,16",
                               "\tfilms,0,3",
                               "\tfirst,0,0",
                               "\tgeneration,0,15",
                               "\tgrew,0,18",
                               "\thas,0,9",
                               "\there,0,5",
                               "\tin,0,20",
                               "\tnew,0,14",
                               "\tpassed,0,11",
                               "\tthe,0,6,21",
                               "\tto,0,12",
                               "\ttorch,0,8",
                               "\tup,0,19",
                               "\twho,0,17",
                               "",
                               "Doc: 1 [AP890101-0002]\tTerm Count: 21\tTotal Words: 29\tMax TF: 3",
                               "\ta,1,4,13",
                               "\tamerican,1,25",
                               "\tat,1,16",
                               "\tbusiness,1,26",
                               "\tdoc,1,1",
                               "\terects,1,3",
                               "\tfactory,1,5,15",
                               "\tfor,1,9",
                               "\tfuture,1,8,23",
                               "\tin,1,12",
                               "\tis,1,27",
                               "\tminiature,1,14",
                               "\tmissouri,1,20",
                               "\tnow,1,28",
                               "\tof,1,6,19,24",
                               "\trolla,1,21",
                               "\tsecond,1,0",
                               "\tstudents,1,10",
                               "\tthe,1,7,17,22",
                               "\tuniversity,1,2,18",
                               "\tworking,1,11",
                               "",
                               "Doc: 2 [AP890104-0235]\tTerm Count: 20\tTotal Words: 25\tMax TF: 2",
                               "\t10,2,7,20",
                               "\t1990s,2,24",
                               "\t300,2,17",
                               "\ta,2,16",
                               "\tchrysler,2,3,9",
                               "\tcorp,2,11",
                               "\tdoc,2,2",
                               "\tduring,2,22",
                               "\tengine,2,8,21",
                               "\thorsepower,2,18",
                               "\tit,2,13",
                               "\tmake,2,15",
                               "\tmotors,2,10",
                               "\tproduce,2,5",
                               "\tsays,2,12",
                               "\tthe,2,0,23",
                               "\tthird,2,1",
                               "\tto,2,4",
                               "\tv,2,6,19",
                               "\twill,2,14"
                             };

    int i = 0;
    for (String line : expectedLines) {
      String expectedL = line.trim();
      String outputL = outputLines[i].trim();
      assertEquals (expectedL, outputL);
  	   i++;
    }

  }  //- end testIIDandEIDwithOverlap

} //- end class DumpDocTermsDnTest
