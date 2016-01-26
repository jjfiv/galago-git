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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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


    String trecCorpus = AppTest.trecDocument ("AP890101-00001", "First doc.\n<HEAD>60s films are here</HEAD>\n"
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
    childPathStemmed = new File (indexFile, "field.krovetz.head");
    assertTrue (childPathStemmed.exists());

    childPath = new File (indexFile, "field.head");
    assertTrue (childPath.exists ());
  }

    
  @After
  public void tearDown() throws IOException {

    if (trecCorpusFile != null) {
      trecCorpusFile.delete();
    }

    if (indexFile != null) {
      FSUtil.deleteDirectory (indexFile);
    }
  }

    
  @Test
  public void testSingleIID () throws Exception {

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(byteArrayStream);

    // test non-stemmed
    App.run (new String[]{ "dump-doc-terms",
			   "--index=" + childPath.getAbsolutePath(),
			   "--iidList=1," }, printStream);

    //printStream.println ("PrintStream Output: " + byteArrayStream.toString());
    String output = byteArrayStream.toString ();
    String[] outputLines = output.split("\n");

    // Check that output and expected line counts agree.  Should be 8 lines.
    assertTrue ("Output should contain " + outputLines.length, outputLines.length == 8);

    // Expected lines
    String[] expectedLines = { "Doc: 1 [AP890101-0002]\tTerm Count: 7\tTotal Words: 7\tMax TF: 1",
                               "\ta,1,4",
                               "\terects,1,3",
                               "\tfactory,1,5",
                               "\tfuture,1,8",
                               "\tof,1,6",
                               "\tthe,1,7",
                               "\tuniversity,1,2"
                             };

    int i = 0;
    for (String line : expectedLines) {
	assertTrue ("Output line [" + i + "] doesn't match expected",
		    expectedLines[i].equals (outputLines[i]));
	i++;
    }

  }  //- end testSingleIID

} //- end class
