/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import org.junit.Test;
import org.lemurproject.galago.core.types.DocumentUrl;
import org.lemurproject.galago.core.types.ExtractedLink;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.OrderedCombiner;
import org.lemurproject.galago.tupleflow.TypeReader;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.StreamUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author sjh
 */
public class HarvestLinksFnTest {

  @Test
  public void testHarvestLinks() throws Exception {
    File tempDir = FileUtility.createTemporaryDirectory();
    try {
      File input = new File(tempDir, "input.trecweb");
      writeInput(input);

      File indri = new File(tempDir, "indri");
      File galago = new File(tempDir, "galago");
      File jobTmp = new File(tempDir, "jobTmp");

      // run harvest links
      Parameters p = Parameters.create();
      p.set("inputPath", input.getAbsolutePath());
      p.set("indri", true);
      p.set("filePrefix", tempDir.getAbsolutePath());
      p.set("prefixReplacement", indri.getAbsolutePath());
      p.set("galago", true);
      p.set("outputFolder", galago.getAbsolutePath());
      p.set("galagoDist", 3); // should get 2 output files
      p.set("distrib", 2);
      p.set("galagoJobDir", jobTmp.getAbsolutePath());
      p.set("server", false);

      HarvestLinksFn hl = new HarvestLinksFn();
      hl.run(p, System.out);

      // now check that we have correct output data
      // first indri
      assert (indri.exists() && indri.list().length == 1) : "There should be one file in the indri directory.";
      File outputFile = new File(indri, "input.trecweb");
      assert (outputFile.exists()) : "input.trecweb should exist in the indri directory.";

      // perhaps there ought to be a test for gzip in the Utility method
      GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(outputFile));
      String data = StreamUtil.copyStreamToString(gzis);

      String expectedPrefix = 
          "DOCNO=test-0\n"
          + "http://small-test.0\n"
          + "LINKS=12\n"
          + "LINKDOCNO=test-0\n"
          + "LINKFROM=http://small-test.0\n"
          + "TEXT=\"t.0\"\n"
          + "LINKDOCNO=test-1\n"
          + "LINKFROM=http://small-test.1\n"
          + "TEXT=\"t.0\"\n"
          + "LINKDOCNO=test-2\n"
          + "LINKFROM=http://small-test.2\n"
          + "TEXT=\"t.0\"\n"
          + "LINKDOCNO=test-4\n"
          + "LINKFROM=http://small-test.4\n"
          + "TEXT=\"t.0\"\n"
          + "LINKDOCNO=test-6\n"
          + "LINKFROM=http://small-test.6\n"
          + "TEXT=\"t.0\"\n"
          + "LINKDOCNO=test-6\n"
          + "LINKFROM=http://small-test.6\n"
          + "TEXT=\"t.0\"\n"
          + "LINKDOCNO=test-6\n"
          + "LINKFROM=http://small-test.6\n"
          + "TEXT=\"t.0\"\n"
          + "LINKDOCNO=test-6\n"
          + "LINKFROM=http://small-test.6\n"
          + "TEXT=\"t.0\"\n"
          + "LINKDOCNO=test-6\n"
          + "LINKFROM=http://small-test.6\n"
          + "TEXT=\"t.0\"\n"
          + "LINKDOCNO=test-7\n"
          + "LINKFROM=http://small-test.7\n"
          + "TEXT=\"t.0\"\n"
          + "LINKDOCNO=test-8\n"
          + "LINKFROM=http://small-test.8\n"
          + "TEXT=\"t.0\"\n"
          + "LINKDOCNO=test-11\n"
          + "LINKFROM=http://small-test.11\n"
          + "TEXT=\"t.0\"\n"
          + "DOCNO=test-1\n"
          + "http://small-test.1\n"
          + "LINKS=6\n"
          + "LINKDOCNO=test-0\n"
          + "LINKFROM=http://small-test.0\n";

      assert (data.startsWith(expectedPrefix)) : "input.trecweb should match the provided prefix.\n" + data;


      // now check galago data
      File names = new File(galago, "names");
      File srcOrder = new File(galago, "srcNameOrder");
      File destOrder = new File(galago, "destNameOrder");

      assert (names.isDirectory() && srcOrder.isDirectory() && destOrder.isDirectory()) : "galago folder should contain three folders.";
      File[] nameFiles = FileUtility.safeListFiles(names);
      assertEquals(nameFiles.length, 3);

      TypeReader<DocumentUrl> reader1 = OrderedCombiner.combineFromFileObjs(Arrays.asList(nameFiles), new DocumentUrl.IdentifierOrder());
      DocumentUrl durl = reader1.read();
      long count = 0;
      while (durl != null) {
        count += 1;
        durl = reader1.read();
      }
      assertEquals(count, 12);

      TypeReader<ExtractedLink> reader2 = OrderedCombiner.combineFromFileObjs(Arrays.asList(FileUtility.safeListFiles(srcOrder)), new ExtractedLink.SrcNameOrder());
      ExtractedLink ln = reader2.read();
      count = 0;
      while (ln != null) {
        count += 1;
        ln = reader2.read();
      }
      assertEquals(count, 53);

    } finally {
      FSUtil.deleteDirectory(tempDir);
    }
  }

  /**
   * Also used by PageRankFnTest
   * @see org.lemurproject.galago.core.tools.apps.PageRankFnTest
   * @throws IOException
   */
  public static void writeInput(File input) throws IOException {
    String data = "<DOC>\n"
            + "<DOCNO>test-0</DOCNO>\n"
            + "<DOCHDR>\n"
            + "http://small-test.0\n"
            + "</DOCHDR>\n"
            + "<html>\n"
            + "This document links to 0 1 3 5 7 and 9\n"
            + "<a href=http://small-test.0>t.0</a>\n"
            + "<a href=http://small-test.1>t.1</a>\n"
            + "<a href=http://small-test.3>t.3</a>\n"
            + "<a href=http://small-test.5>t.5</a>\n"
            + "<a href=http://small-test.7><img path=t.7></a>\n"
            + "<a href=http://small-test.9 />\n"
            + "</html>\n"
            + "</DOC>\n"
            + "\n"
            + "<DOC>\n"
            + "<DOCNO>test-1</DOCNO>\n"
            + "<DOCHDR>\n"
            + "http://small-test.1\n"
            + "</DOCHDR>\n"
            + "<html>\n"
            + "This document links to 0 2 4 6 8 and ext-100 ext-101\n"
            + "<a href=http://small-test.0>t.0</a>\n"
            + "<a href=http://small-test.2>t.2</a>\n"
            + "<a href=http://small-test.4>t.4</a>\n"
            + "<a href=http://small-test.6>t.6</a>\n"
            + "<a href=http://small-test.8>t.8</a>\n"
            + "<a href=http://small-test.ext.100 />\n"
            + "<a href=http://small-test.ext.101>external</a>\n"
            + "</html>\n"
            + "</DOC>\n"
            + "\n"
            + "<DOC>\n"
            + "<DOCNO>test-2</DOCNO>\n"
            + "<DOCHDR>\n"
            + "http://small-test.2\n"
            + "</DOCHDR>\n"
            + "<html>\n"
            + "This document links to 0 1 2 3 4 and 5\n"
            + "<a href=http://small-test.0>t.0</a>\n"
            + "<a href=http://small-test.1>t.1</a>\n"
            + "<a href=http://small-test.2>t.2</a>\n"
            + "<a href=http://small-test.3>t.3</a>\n"
            + "<a href=http://small-test.4>t.4</a>\n"
            + "<a href=http://small-test.5 />\n"
            + "</html>\n"
            + "</DOC>\n"
            + "\n"
            + "<DOC>\n"
            + "<DOCNO>test-3</DOCNO>\n"
            + "<DOCHDR>\n"
            + "http://small-test.3\n"
            + "</DOCHDR>\n"
            + "<html>\n"
            + "This document links to 5 6 7 8 and 9\n"
            + "<a href=http://small-test.5>t.5</a>\n"
            + "<a href=http://small-test.6>t.6</a>\n"
            + "<a href=http://small-test.7>t.7</a>\n"
            + "<a href=http://small-test.8>t.8</a>\n"
            + "<a href=http://small-test.9 />\n"
            + "</html>\n"
            + "</DOC>\n"
            + "\n"
            + "<DOC>\n"
            + "<DOCNO>test-4</DOCNO>\n"
            + "<DOCHDR>\n"
            + "http://small-test.4\n"
            + "</DOCHDR>\n"
            + "<html>\n"
            + "This document links to 0 1 1 2 3 5 and 8\n"
            + "<a href=http://small-test.0>t.0</a>\n"
            + "<a href=http://small-test.1>t.1</a>\n"
            + "<a href=http://small-test.1>t.1</a>\n"
            + "<a href=http://small-test.2>t.2</a>\n"
            + "<a href=http://small-test.3>t.3</a>\n"
            + "<a href=http://small-test.5 />\n"
            + "<a href=http://small-test.8 />\n"
            + "</html>\n"
            + "</DOC>\n"
            + "\n"
            + "<DOC>\n"
            + "<DOCNO>test-5</DOCNO>\n"
            + "<DOCHDR>\n"
            + "http://small-test.5\n"
            + "</DOCHDR>\n"
            + "<html>\n"
            + "This document links to 5 5 5 5 and 5\n"
            + "<a href=http://small-test.5>t.5</a>\n"
            + "<a href=http://small-test.5>t.5</a>\n"
            + "<a href=http://small-test.5>t.5</a>\n"
            + "<a href=http://small-test.5>t.5</a>\n"
            + "<a href=http://small-test.5>t.5</a>\n"
            + "</html>\n"
            + "</DOC>\n"
            + "\n"
            + "<DOC>\n"
            + "<DOCNO>test-6</DOCNO>\n"
            + "<DOCHDR>\n"
            + "http://small-test.6\n"
            + "</DOCHDR>\n"
            + "<html>\n"
            + "This document links to 0 0 0 0 and 0\n"
            + "<a href=http://small-test.0>t.0</a>\n"
            + "<a href=http://small-test.0>t.0</a>\n"
            + "<a href=http://small-test.0>t.0</a>\n"
            + "<a href=http://small-test.0>t.0</a>\n"
            + "<a href=http://small-test.0>t.0</a>\n"
            + "</html>\n"
            + "</DOC>\n"
            + "\n"
            + "<DOC>\n"
            + "<DOCNO>test-7</DOCNO>\n"
            + "<DOCHDR>\n"
            + "http://small-test.7\n"
            + "</DOCHDR>\n"
            + "<html>\n"
            + "This document links to 0 1 3 5 7 and 9\n"
            + "<a href=http://small-test.0>t.0</a>\n"
            + "<a href=http://small-test.1>t.1</a>\n"
            + "<a href=http://small-test.3>t.3</a>\n"
            + "<a href=http://small-test.5>t.5</a>\n"
            + "<a href=http://small-test.7>t.7</a>\n"
            + "<a href=http://small-test.9 />\n"
            + "</html>\n"
            + "</DOC>\n"
            + "\n"
            + "<DOC>\n"
            + "<DOCNO>test-8</DOCNO>\n"
            + "<DOCHDR>\n"
            + "http://small-test.8\n"
            + "</DOCHDR>\n"
            + "<html>\n"
            + "This document links to 0 2 4 6 and 8\n"
            + "<a href=http://small-test.0>t.0</a>\n"
            + "<a href=http://small-test.2>t.2</a>\n"
            + "<a href=http://small-test.4>t.4</a>\n"
            + "<a href=http://small-test.6>t.6</a>\n"
            + "<a href=http://small-test.8>t.8</a>\n"
            + "</html>\n"
            + "</DOC>\n"
            + "\n"
            + "<DOC>\n"
            + "<DOCNO>test-9</DOCNO>\n"
            + "<DOCHDR>\n"
            + "http://small-test.9\n"
            + "</DOCHDR>\n"
            + "<html>\n"
            + "This document does not link to anyone.\n"
            + "</html>\n"
            + "</DOC>\n"
            + "\n"
            + "<DOC>\n"
            + "<DOCNO>test-10</DOCNO>\n"
            + "<DOCHDR>\n"
            + "http://small-test.10\n"
            + "</DOCHDR>\n"
            + "<html>\n"
            + "This document does not link, and is not linked.\n"
            + "</html>\n"
            + "</DOC>\n"
            + "\n"
            + "<DOC>\n"
            + "<DOCNO>test-11</DOCNO>\n"
            + "<DOCHDR>\n"
            + "http://small-test.11\n"
            + "</DOCHDR>\n"
            + "<html>\n"
            + "This document is not linked, but links to 0 1 2.\n"
            + "<a href=http://small-test.0>t.0</a>\n"
            + "<a href=http://small-test.1>t.1</a>\n"
            + "<a href=http://small-test.2>t.2</a>\n"
            + "</html>\n"
            + "</DOC>\n";

    Utility.copyStringToFile(data, input);
  }
}
