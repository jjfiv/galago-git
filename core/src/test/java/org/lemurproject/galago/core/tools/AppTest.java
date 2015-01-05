// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools;

import org.junit.Assert;
import org.junit.Test;
import org.lemurproject.galago.core.btree.format.BTreeFactory;
import org.lemurproject.galago.core.btree.format.SplitBTreeReader;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.StreamUtil;
import org.lemurproject.galago.utility.btree.disk.GalagoBTreeReader;
import org.lemurproject.galago.utility.btree.disk.VocabularyReader;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.List;

import static org.junit.Assert.*;

/**
 *
 * @author trevor
 */
public class AppTest {

    final String newLine = System.getProperty("line.separator");

    public static String trecDocument(String docno, String text) {
        return "<DOC>\n<DOCNO>" + docno + "</DOCNO>\n"
                + "<TEXT>\n" + text + "</TEXT>\n</DOC>\n";
    }

    public static void verifyIndexStructures(File indexPath) throws Exception {
        // Check main path
        assertTrue(indexPath.isDirectory());
        // Time to check standard parts
        Retrieval ret = RetrievalFactory.instance(indexPath.getAbsolutePath(), Parameters.create());
        Parameters availableParts = ret.getAvailableParts();
        assertNotNull(availableParts);

        // doc lengths
        File childPath = new File(indexPath, "lengths");
        assertTrue(childPath.exists());

        // doc names -- there are two files
        childPath = new File(indexPath, "names");
        assertTrue(childPath.exists());
        childPath = new File(indexPath, "names.reverse");
        assertTrue(childPath.exists());

        // postings
        childPath = new File(indexPath, "postings");
        assertTrue(childPath.exists());

    }

    @Test
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
            trecCorpusFile = FileUtility.createTemporary();
            StreamUtil.copyStringToFile(trecCorpus, trecCorpusFile);

            // now, attempt to make a corpus folder from that.
            corpusFile1 = FileUtility.createTemporary();

            App.main(new String[]{"make-corpus", "--corpusPath=" + corpusFile1.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile.getAbsolutePath(), "--corpusFormat=file", "--server=false"});
            // make sure the corpus file exists
            assertTrue(corpusFile1.exists());
 
            // now, attempt to make a corpus folder from that.
            corpusFile2 = FileUtility.createTemporaryDirectory();
            App.main(new String[]{"make-corpus", "--corpusPath=" + corpusFile2.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile.getAbsolutePath(), "--distrib=2",
                "--corpusParameters/corpusTags=false", "--corpusParameters/corpusTerms=false", "--server=false"});

            // make sure the corpus folder exists
            assertTrue(corpusFile2.exists());
            assertTrue(new File(corpusFile2, "split.keys").exists());
            assertTrue(new File(corpusFile2, "0").exists());

            assertTrue(SplitBTreeReader.isBTree(new File(corpusFile2, "split.keys")));
            assertFalse(SplitBTreeReader.isBTree(new File(corpusFile2, "0")));

            // now, try to build an index from that
            indexFile1 = FileUtility.createTemporaryDirectory();
            App.main(new String[]{"build", "--indexPath=" + indexFile1.getAbsolutePath(),
                "--inputPath=" + corpusFile1.getAbsolutePath(), "--server=false"});

            // now, try to build an index from that
            indexFile2 = FileUtility.createTemporaryDirectory();
            App.main(new String[]{"build", "--indexPath=" + indexFile2.getAbsolutePath(),
                "--inputPath=" + corpusFile2.getAbsolutePath(), "--server=false"});

            // make sure the indexes exists
            assertTrue(indexFile1.exists());
            assertTrue(indexFile2.exists());

        } finally {

            if (trecCorpusFile != null) {
                Assert.assertTrue(trecCorpusFile.delete());
            }
            // TODO MCZ 3/27/2014 (#225 Galago tests fail on windows Move Edit)
            // this currently fails on Windows, 
            // I beleive there is a reader or writer that has not been
            // closed so the delete() returns false. 
            boolean deleted;
            deleted = corpusFile1.delete();
            if (!deleted) {
                deleted = true; 
                FSUtil.deleteDirectory(corpusFile1);
            }
            assertTrue(deleted);
            deleted = corpusFile2.delete();
            if (!deleted) {
                deleted = true; 
                FSUtil.deleteDirectory(corpusFile2);
            }
            assertTrue(deleted);

            if (indexFile1 != null) {
                FSUtil.deleteDirectory(indexFile1);
            }
            if (indexFile2 != null) {
                FSUtil.deleteDirectory(indexFile2);
            }

        }
    }

    @Test
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
            trecCorpusFile = FileUtility.createTemporary();
            StreamUtil.copyStringToFile(trecCorpus, trecCorpusFile);

            // now, attempt to make a corpus file from that.
            corpusFile = FileUtility.createTemporaryDirectory();
            App.main(new String[]{"make-corpus", "--corpusPath=" + corpusFile.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile.getAbsolutePath(), "--distrib=2", "--server=false"});

            // make sure the corpus file exists
            assertTrue(corpusFile.exists());
            assertTrue(corpusFile.isDirectory());
            assertTrue(new File(corpusFile, "split.keys").isFile());
            assertTrue(BTreeFactory.isBTree(new File(corpusFile, "split.keys")));

            // open the corpus
            GalagoBTreeReader reader = BTreeFactory.getBTreeReader(corpusFile);

            // we will divide the corpus by vocab blocks
            VocabularyReader vocabulary = reader.getVocabulary();
            List<VocabularyReader.IndexBlockInfo> slots = vocabulary.getSlots();
            assertNotEquals(0, slots.size());

            // now, try to build an index from that
            indexFile = FileUtility.createTemporaryDirectory();
            App.main(new String[]{"build", "--indexPath=" + indexFile.getAbsolutePath(),
                "--inputPath=" + corpusFile.getAbsolutePath(),
                "--corpus=true", "--server=false"});

            // Checks path and components
            verifyIndexStructures(indexFile);

            // try to batch search that index with a no-match string
            String queries
                    = "{\n"
                    + "\"shareNodes\" : true, \"queries\" : [\n"
                    + "{ \"number\" :\"5\", \"text\" : \"nothing\"},\n"
                    + "{ \"number\" :\"9\", \"text\" : \"sample\"},\n"
                    + "{ \"number\" :\"10\", \"text\" : \"nothing sample\"},\n"
                    + "{ \"number\" :\"14\", \"text\" : \"#combine(#ordered:1(this is) sample)\"},\n"
                    + "{ \"number\" :\"23\", \"text\" : \"#combine( sample sample document document )\"},\n"
                    + "{ \"number\" :\"24\", \"text\" : \"#combine( #combine(sample) two #combine(document document) )\"},\n"
                    + "{ \"number\" :\"25\", \"text\" : \"#combine( sample two document )\"}\n"
                    + "]}\n";
            queryFile1 = FileUtility.createTemporary();
            StreamUtil.copyStringToFile(queries, queryFile1);

            // Smoke test with batch search
            ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
            PrintStream printStream = new PrintStream(byteArrayStream);

            App.run(new String[]{"batch-search",
                "--index=" + indexFile.getAbsolutePath(),
                queryFile1.getAbsolutePath()}, printStream);

            // Now, verify that some stuff exists
            String output = byteArrayStream.toString();

            String expectedScores
                    = "9 Q0 59 1 -1.38562925 galago" + newLine
                    + "9 Q0 55 2 -1.38695903 galago" + newLine
                    + "10 Q0 59 1 -2.08010799 galago" + newLine
                    + "10 Q0 55 2 -2.08143777 galago" + newLine
                    + "14 Q0 55 1 -1.73220460 galago" + newLine
                    + "14 Q0 59 2 -1.73353440 galago" + newLine
                    + "23 Q0 59 1 -1.38562925 galago" + newLine
                    + "23 Q0 55 2 -1.38695903 galago" + newLine
                    + "24 Q0 59 1 -1.61579296 galago" + newLine
                    + "24 Q0 55 2 -1.61889580 galago" + newLine
                    + "25 Q0 59 1 -1.61579296 galago" + newLine
                    + "25 Q0 55 2 -1.61889580 galago" + newLine;

            assertEquals(expectedScores, output);

            // Verify dump-keys works
            byteArrayStream = new ByteArrayOutputStream();
            printStream = new PrintStream(byteArrayStream);

            App.run(new String[]{"dump-keys", corpusFile.getAbsolutePath() + File.separator + "split.keys"}, printStream);
            output = byteArrayStream.toString();
            assertEquals("0" + newLine + "1" + newLine, output);

            // Verify doc works
            byteArrayStream = new ByteArrayOutputStream();
            printStream = new PrintStream(byteArrayStream);

            App.run(new String[]{"doc", "--index=" + indexFile.getAbsolutePath(), "--id='55'"}, printStream);

            // Verify dump-index works
            byteArrayStream = new ByteArrayOutputStream();
            printStream = new PrintStream(byteArrayStream);

            String postingsName = Utility.join(new String[]{indexFile.getAbsolutePath(),
                "postings.krovetz"}, File.separator);
            App.run(new String[]{"dump-index", postingsName}, printStream);
            output = byteArrayStream.toString();
            String correct = "a,0,2" + newLine
                    + "document,0,4" + newLine
                    + "document,1,1" + newLine
                    + "is,0,1" + newLine
                    + "sample,0,3" + newLine
                    + "sample,1,0" + newLine
                    + "this,0,0" + newLine
                    + "two,1,2" + newLine;

            assertEquals(correct, output);

            // Verify eval works
            byteArrayStream = new ByteArrayOutputStream();
            printStream = new PrintStream(byteArrayStream);

            scoresFile = FileUtility.createTemporary();
            StreamUtil.copyStringToFile(expectedScores, scoresFile);
            relsFile = FileUtility.createTemporary();
            StreamUtil.copyStringToFile("9 Q0 55 1" + newLine, relsFile);

            // for now this is just a smoke test.
            App.run(new String[]{"eval",
                "--baseline=" + scoresFile.getAbsolutePath(),
                "--judgments=" + relsFile.getAbsolutePath()},
                    printStream);

            queries = "{ \"x\" : ["
                    + "\"document\"," + newLine
                    + "\"#counts:a:part=postings()\"," + newLine
                    + "\"#counts:a:part=postings.krovetz()\"" + newLine
                    + "]}" + newLine;
            queryFile2 = FileUtility.createTemporary();
            StreamUtil.copyStringToFile(queries, queryFile2);

            byteArrayStream = new ByteArrayOutputStream();
            printStream = new PrintStream(byteArrayStream);
            // now check xcount and doccount

            App.run(new String[]{"xcount",
                "--index=" + indexFile.getAbsolutePath(),
                queryFile2.getAbsolutePath()}, printStream);

            output = byteArrayStream.toString();
            String expected = "2\tdocument" + newLine
                    + "1\t#counts:a:part=postings()" + newLine
                    + "1\t#counts:a:part=postings.krovetz()" + newLine;

            assertEquals(expected, output);

        } finally {
            if (relsFile != null) {
                Assert.assertTrue(relsFile.delete());
            }
            if (queryFile1 != null) {
                Assert.assertTrue(queryFile1.delete());
            }
            if (queryFile2 != null) {
                Assert.assertTrue(queryFile2.delete());
            }
            if (scoresFile != null) {
                Assert.assertTrue(scoresFile.delete());
            }
            if (trecCorpusFile != null) {
                Assert.assertTrue(trecCorpusFile.delete());
            }
            if (corpusFile != null) {
                FSUtil.deleteDirectory(corpusFile);
            }
            if (indexFile != null) {
                FSUtil.deleteDirectory(indexFile);
            }
        }
    }

    @Test
    public void testSimplePipeline2() throws Exception {
        File queryFile = null;
        File trecCorpusFile = null;
        File indexFile = null;

        try {
            // create a simple doc file, trec format:
            String trecCorpus = trecDocument("55", "This is a sample document")
                    + trecDocument("59", "sample document two");
            trecCorpusFile = FileUtility.createTemporary();
            StreamUtil.copyStringToFile(trecCorpus, trecCorpusFile);

            // now, try to build an index from that
            indexFile = FileUtility.createTemporaryDirectory();
            App.main(new String[]{"build", "--indexPath=" + indexFile.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile.getAbsolutePath(),
                "--corpus=true", "--server=false"});

            // Checks path and components
            verifyIndexStructures(indexFile);

            // try to batch search that index with a no-match string
            String queries
                    = "{\n"
                    + "\"queries\" : [\n"
                    + "{ \"number\" :\"5\", \"text\" : \"nothing\"},\n"
                    + "{ \"number\" :\"9\", \"text\" : \"sample\"},\n"
                    + "{ \"number\" :\"10\", \"text\" : \"nothing sample\"},\n"
                    + "{ \"number\" :\"14\", \"text\" : \"#combine(#ordered:1(this is) sample)\"},\n"
                    + "{ \"number\" :\"23\", \"text\" : \"#combine( sample sample document document )\"},\n"
                    + "{ \"number\" :\"24\", \"text\" : \"#combine( #combine(sample) two #combine(document document) )\"},\n"
                    + "{ \"number\" :\"25\", \"text\" : \"#combine( sample two document )\"}\n"
                    + "]}\n";
            queryFile = FileUtility.createTemporary();
            StreamUtil.copyStringToFile(queries, queryFile);

            // Smoke test with batch search
            ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
            PrintStream printStream = new PrintStream(byteArrayStream);

            App.run(new String[]{"batch-search",
                "--index=" + indexFile.getAbsolutePath(),
                queryFile.getAbsolutePath()}, printStream);

            // Now, verify that some stuff exists
            String output = byteArrayStream.toString();

            String expectedScores
                    = "9 Q0 59 1 -1.38562925 galago" + newLine
                    + "9 Q0 55 2 -1.38695903 galago" + newLine
                    + "10 Q0 59 1 -2.08010799 galago" + newLine
                    + "10 Q0 55 2 -2.08143777 galago" + newLine
                    + "14 Q0 55 1 -1.73220460 galago" + newLine
                    + "14 Q0 59 2 -1.73353440 galago" + newLine
                    + "23 Q0 59 1 -1.38562925 galago" + newLine
                    + "23 Q0 55 2 -1.38695903 galago" + newLine
                    + "24 Q0 59 1 -1.61579296 galago" + newLine
                    + "24 Q0 55 2 -1.61889580 galago" + newLine
                    + "25 Q0 59 1 -1.61579296 galago" + newLine
                    + "25 Q0 55 2 -1.61889580 galago" + newLine;

            assertEquals(expectedScores, output);

            // Smoke test with batch search - non normalizing
            byteArrayStream = new ByteArrayOutputStream();
            printStream = new PrintStream(byteArrayStream);

            App.run(new String[]{"batch-search",
                "--norm=false",
                "--index=" + indexFile.getAbsolutePath(),
                queryFile.getAbsolutePath()}, printStream);

            printStream.close();
            
            // Now, verify that some stuff exists
            output = byteArrayStream.toString();
            byteArrayStream.close();
            
            expectedScores
                    = "9 Q0 59 1 -1.38562925 galago" + newLine
                    + "9 Q0 55 2 -1.38695903 galago" + newLine
                    + "10 Q0 59 1 -4.16021597 galago" + newLine
                    + "10 Q0 55 2 -4.16287555 galago" + newLine
                    + "14 Q0 55 1 -3.46440920 galago" + newLine
                    + "14 Q0 59 2 -3.46706879 galago" + newLine
                    + "23 Q0 59 1 -5.54251699 galago" + newLine
                    + "23 Q0 55 2 -5.54783614 galago" + newLine
                    + "24 Q0 59 1 -4.84737888 galago" + newLine
                    + "24 Q0 55 2 -4.85668740 galago" + newLine
                    + "25 Q0 59 1 -4.84737888 galago" + newLine
                    + "25 Q0 55 2 -4.85668740 galago" + newLine;

            assertEquals(expectedScores, output);

        } finally {
            if (queryFile != null) {
                Assert.assertTrue(queryFile.delete());
            }
            if (trecCorpusFile != null) {
                Assert.assertTrue(trecCorpusFile.delete());
            }
            if (indexFile != null) {       
                FSUtil.deleteDirectory(indexFile);
            }

        }
    }
}
