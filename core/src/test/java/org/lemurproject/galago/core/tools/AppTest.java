// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.corpus.SplitBTreeReader;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class AppTest extends TestCase {

  public AppTest(String testName) {
    super(testName);
  }

  public static String trecDocument(String docno, String text) {
    return "<DOC>\n<DOCNO>" + docno + "</DOCNO>\n"
            + "<TEXT>\n" + text + "</TEXT>\n</DOC>\n";
  }

  public static void verifyIndexStructures(File indexPath) throws Exception {
    // Check main path
    assertTrue(indexPath.isDirectory());
    // Time to check standard parts
    Retrieval ret = RetrievalFactory.instance(indexPath.getAbsolutePath(), new Parameters());
    Parameters availableParts = ret.getAvailableParts();
  }

  public void testMakeCorpora() throws Exception {
    File trecCorpusFile = null;
    File corpusFile1 = null;
    File corpusFile2 = null;
    File indexFile1 = null;
    File indexFile2 = null;

    try {
      // create a simple doc file, trec format:
      String trecCorpus = trecDocument("55", "This is a sample document")
              + trecDocument("59", "sample document two");
      trecCorpusFile = Utility.createTemporary();
      Utility.copyStringToFile(trecCorpus, trecCorpusFile);

      // now, attempt to make a corpus folder from that.
      corpusFile1 = Utility.createTemporary();
      App.main(new String[]{"make-corpus", "--corpusPath=" + corpusFile1.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile.getAbsolutePath(), "--corpusFormat=file"});
      // make sure the corpus file exists
      assertTrue(corpusFile1.exists());

      // now, attempt to make a corpus folder from that.
      corpusFile2 = Utility.createTemporaryDirectory();
      App.main(new String[]{"make-corpus", "--corpusPath=" + corpusFile2.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile.getAbsolutePath(), "--distrib=2",
                "--corpusParameters/corpusTags=false", "--corpusParameters/corpusTerms=false"});

      // make sure the corpus folder exists
      assertTrue(corpusFile2.exists());
      assertTrue(new File(corpusFile2, "split.keys").exists());
      assertTrue(new File(corpusFile2, "0").exists());

      assertTrue(SplitBTreeReader.isBTree(new File(corpusFile2, "split.keys")));
      assertFalse(SplitBTreeReader.isBTree(new File(corpusFile2, "0")));

      // now, try to build an index from that
      indexFile1 = Utility.createTemporaryDirectory();
      App.main(new String[]{"build", "--indexPath=" + indexFile1.getAbsolutePath(),
                "--inputPath=" + corpusFile1.getAbsolutePath()});

      // now, try to build an index from that
      indexFile2 = Utility.createTemporaryDirectory();
      App.main(new String[]{"build", "--indexPath=" + indexFile2.getAbsolutePath(),
                "--inputPath=" + corpusFile2.getAbsolutePath()});

      // make sure the indexes exists
      assertTrue(indexFile1.exists());
      assertTrue(indexFile2.exists());

    } finally {
      if (trecCorpusFile != null) {
        trecCorpusFile.delete();
      }
      if (corpusFile1 != null) {
        corpusFile1.delete();
      }
      if (corpusFile2 != null) {
        Utility.deleteDirectory(corpusFile2);
      }
      if (indexFile1 != null) {
        Utility.deleteDirectory(indexFile1);
      }
      if (indexFile2 != null) {
        Utility.deleteDirectory(indexFile2);
      }
    }
  }

  public void testSimplePipeline() throws Exception {
    File relsFile = null;
    File queryFile1 = null;
    File queryFile2 = null;
    File scoresFile = null;
    File trecCorpusFile = null;
    File corpusFile = null;
    File indexFile = null;

    try {
      // create a simple doc file, trec format:
      String trecCorpus = trecDocument("55", "This is a sample document")
              + trecDocument("59", "sample document two");
      trecCorpusFile = Utility.createTemporary();
      Utility.copyStringToFile(trecCorpus, trecCorpusFile);

      // now, attempt to make a corpus file from that.
      corpusFile = Utility.createTemporaryDirectory();
      App.main(new String[]{"make-corpus", "--corpusPath=" + corpusFile.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile.getAbsolutePath(), "--distrib=2"});

      // make sure the corpus file exists
      assertTrue(corpusFile.exists());

      // now, try to build an index from that
      indexFile = Utility.createTemporaryDirectory();
      App.main(new String[]{"build", "--indexPath=" + indexFile.getAbsolutePath(),
                "--inputPath=" + corpusFile.getAbsolutePath(),
                "--corpus=true"});

      // Checks path and components
      verifyIndexStructures(indexFile);

      // try to batch search that index with a no-match string
      String queries =
              "{\n"
              + "\"shareNodes\" : true, \"queries\" : [\n"
              + "{ \"number\" :\"5\", \"text\" : \"nothing\"},\n"
              + "{ \"number\" :\"9\", \"text\" : \"sample\"},\n"
              + "{ \"number\" :\"10\", \"text\" : \"nothing sample\"},\n"
              + "{ \"number\" :\"14\", \"text\" : \"#combine(#ordered:1(this is) sample)\"},\n"
              + "{ \"number\" :\"23\", \"text\" : \"#combine( sample sample document document )\"},\n"
              + "{ \"number\" :\"24\", \"text\" : \"#combine( #combine(sample) two #combine(document document) )\"},\n"
              + "{ \"number\" :\"25\", \"text\" : \"#combine( sample two document )\"}\n"
              + "]}\n";
      queryFile1 = Utility.createTemporary();
      Utility.copyStringToFile(queries, queryFile1);

      // Smoke test with batch search
      ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
      PrintStream printStream = new PrintStream(byteArrayStream);

      App.run(new String[]{"batch-search",
                "--index=" + indexFile.getAbsolutePath(),
                queryFile1.getAbsolutePath()}, printStream);

      // Now, verify that some stuff exists
      String output = byteArrayStream.toString();

      String expectedScores =
              "9 Q0 59 1 -1.38562925 galago\n"
              + "9 Q0 55 2 -1.38695903 galago\n"
              + "10 Q0 59 1 -2.08010799 galago\n"
              + "10 Q0 55 2 -2.08143777 galago\n"
              + "14 Q0 55 1 -1.73220460 galago\n"
              + "14 Q0 59 2 -1.73353440 galago\n"
              + "23 Q0 59 1 -1.38562925 galago\n"
              + "23 Q0 55 2 -1.38695903 galago\n"
              + "24 Q0 59 1 -1.61579296 galago\n"
              + "24 Q0 55 2 -1.61889580 galago\n"
              + "25 Q0 59 1 -1.61579296 galago\n"
              + "25 Q0 55 2 -1.61889580 galago\n";

      assertEquals(expectedScores, output);

      // Verify dump-keys works
      byteArrayStream = new ByteArrayOutputStream();
      printStream = new PrintStream(byteArrayStream);

      App.run(new String[]{"dump-keys", corpusFile.getAbsolutePath() + File.separator + "split.keys"}, printStream);
      output = byteArrayStream.toString();
      assertEquals("0\n1\n", output);

      // Verify doc works
      byteArrayStream = new ByteArrayOutputStream();
      printStream = new PrintStream(byteArrayStream);

      App.run(new String[]{"doc", "--index=" + indexFile.getAbsolutePath(), "--id='55'"}, printStream);
      output = byteArrayStream.toString();
//      assertEquals("#IDENTIFIER: 55\n<TEXT>\nThis is a sample document</TEXT>\n\n", output);

      // Verify dump-index works
      byteArrayStream = new ByteArrayOutputStream();
      printStream = new PrintStream(byteArrayStream);

      String postingsName = Utility.join(new String[]{indexFile.getAbsolutePath(),
                "postings.porter"}, File.separator);
      App.run(new String[]{"dump-index", postingsName}, printStream);
      output = byteArrayStream.toString();
      String correct = "a,0,2\n"
              + "document,0,4\n"
              + "document,1,1\n"
              + "is,0,1\n"
              + "sampl,0,3\n"
              + "sampl,1,0\n"
              + "this,0,0\n"
              + "two,1,2\n";

      assertEquals(correct, output);

      // Verify eval works
      byteArrayStream = new ByteArrayOutputStream();
      printStream = new PrintStream(byteArrayStream);

      scoresFile = Utility.createTemporary();
      Utility.copyStringToFile(expectedScores, scoresFile);
      relsFile = Utility.createTemporary();
      Utility.copyStringToFile("9 Q0 55 1\n", relsFile);

      // for now this is just a smoke test.
      App.run(new String[]{"eval",
                "--baseline=" + scoresFile.getAbsolutePath(),
                "--judgments=" + relsFile.getAbsolutePath()},
              printStream);


      queries = "{ \"x\" : ["
              + "\"document\",\n"
              + "\"#counts:a:part=postings()\",\n"
              + "\"#counts:a:part=postings.porter()\"\n"
              + "]}\n";
      queryFile2 = Utility.createTemporary();
      Utility.copyStringToFile(queries, queryFile2);


      byteArrayStream = new ByteArrayOutputStream();
      printStream = new PrintStream(byteArrayStream);
      // now check xcount and doccount
      App.run(new String[]{"xcount",
                "--index=" + indexFile.getAbsolutePath(),
                queryFile2.getAbsolutePath()}, printStream);
      output = byteArrayStream.toString();
      String expected = "2\tdocument\n"
              + "1\t#counts:a:part=postings()\n"
              + "1\t#counts:a:part=postings.porter()\n";

       assertEquals(expected, output);

    } finally {
      if (relsFile != null) {
        relsFile.delete();
      }
      if (queryFile1 != null) {
        queryFile1.delete();
      }
      if (queryFile2 != null) {
        queryFile2.delete();
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
    }
  }

  public void testSimplePipeline2() throws Exception {
    File queryFile = null;
    File scoresFile = null;
    File trecCorpusFile = null;
    File indexFile = null;

    try {
      // create a simple doc file, trec format:
      String trecCorpus = trecDocument("55", "This is a sample document")
              + trecDocument("59", "sample document two");
      trecCorpusFile = Utility.createTemporary();
      Utility.copyStringToFile(trecCorpus, trecCorpusFile);

      // now, try to build an index from that
      indexFile = Utility.createTemporaryDirectory();
      App.main(new String[]{"build", "--indexPath=" + indexFile.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile.getAbsolutePath(),
                "--corpus=true"});

      // Checks path and components
      verifyIndexStructures(indexFile);

      // try to batch search that index with a no-match string
      String queries =
              "{\n"
              + "\"queries\" : [\n"
              + "{ \"number\" :\"5\", \"text\" : \"nothing\"},\n"
              + "{ \"number\" :\"9\", \"text\" : \"sample\"},\n"
              + "{ \"number\" :\"10\", \"text\" : \"nothing sample\"},\n"
              + "{ \"number\" :\"14\", \"text\" : \"#combine(#ordered:1(this is) sample)\"},\n"
              + "{ \"number\" :\"23\", \"text\" : \"#combine( sample sample document document )\"},\n"
              + "{ \"number\" :\"24\", \"text\" : \"#combine( #combine(sample) two #combine(document document) )\"},\n"
              + "{ \"number\" :\"25\", \"text\" : \"#combine( sample two document )\"}\n"
              + "]}\n";
      queryFile = Utility.createTemporary();
      Utility.copyStringToFile(queries, queryFile);

      // Smoke test with batch search
      ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
      PrintStream printStream = new PrintStream(byteArrayStream);

      new App().run(new String[]{"batch-search",
                "--index=" + indexFile.getAbsolutePath(),
                queryFile.getAbsolutePath()}, printStream);

      // Now, verify that some stuff exists
      String output = byteArrayStream.toString();

      String expectedScores =
              "9 Q0 59 1 -1.38562925 galago\n"
              + "9 Q0 55 2 -1.38695903 galago\n"
              + "10 Q0 59 1 -2.08010799 galago\n"
              + "10 Q0 55 2 -2.08143777 galago\n"
              + "14 Q0 55 1 -1.73220460 galago\n"
              + "14 Q0 59 2 -1.73353440 galago\n"
              + "23 Q0 59 1 -1.38562925 galago\n"
              + "23 Q0 55 2 -1.38695903 galago\n"
              + "24 Q0 59 1 -1.61579296 galago\n"
              + "24 Q0 55 2 -1.61889580 galago\n"
              + "25 Q0 59 1 -1.61579296 galago\n"
              + "25 Q0 55 2 -1.61889580 galago\n";

      assertEquals(expectedScores, output);

      // Smoke test with batch search - non normalizing
      byteArrayStream = new ByteArrayOutputStream();
      printStream = new PrintStream(byteArrayStream);

      new App().run(new String[]{"batch-search",
                "--norm=false",
                "--index=" + indexFile.getAbsolutePath(),
                queryFile.getAbsolutePath()}, printStream);

      // Now, verify that some stuff exists
      output = byteArrayStream.toString();

      expectedScores =
              "9 Q0 59 1 -1.38562925 galago\n"
              + "9 Q0 55 2 -1.38695903 galago\n"
              + "10 Q0 59 1 -4.16021597 galago\n"
              + "10 Q0 55 2 -4.16287555 galago\n"
              + "14 Q0 55 1 -3.46440920 galago\n"
              + "14 Q0 59 2 -3.46706879 galago\n"
              + "23 Q0 59 1 -5.54251699 galago\n"
              + "23 Q0 55 2 -5.54783614 galago\n"
              + "24 Q0 59 1 -4.84737888 galago\n"
              + "24 Q0 55 2 -4.85668740 galago\n"
              + "25 Q0 59 1 -4.84737888 galago\n"
              + "25 Q0 55 2 -4.85668740 galago\n";


      assertEquals(expectedScores, output);

    } finally {
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
  }
}