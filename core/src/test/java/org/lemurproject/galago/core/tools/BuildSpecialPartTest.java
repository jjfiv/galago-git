// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools;

import org.junit.Test;
import org.lemurproject.galago.core.index.disk.DocumentIndicatorReader;
import org.lemurproject.galago.core.index.disk.DocumentPriorReader;
import org.lemurproject.galago.core.retrieval.iterator.IndicatorIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.StreamUtil;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author trevor
 */
public class BuildSpecialPartTest {

    final String newLine = System.getProperty("line.separator");

    public static String trecDocument(String docno, String text) {
        return "<DOC>\n<DOCNO>d" + docno + "</DOCNO>\n"
                + "<TEXT>\n" + text + "</TEXT>\n</DOC>\n";
    }

    @Test
    public void testIndicators() throws Exception {
        File trecCorpusFile = null;
        File indicatorFile = null;
        File indexFile = null;
        File queryFile = null;

        try {
            // create a simple doc file, trec format:
            String trecCorpus = trecDocument("10", "sample document four")
                    + trecDocument("11", "sample document five")
                    + trecDocument("55", "This is a sample document")
                    + trecDocument("59", "sample document two")
                    + trecDocument("73", "sample document three");
            trecCorpusFile = FileUtility.createTemporary();
            StreamUtil.copyStringToFile(trecCorpus, trecCorpusFile);

            // now build an index from that
            indexFile = FileUtility.createTemporaryDirectory();
            App.main(new String[]{"build", "--indexPath=" + indexFile.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile.getAbsolutePath(), "--server=false"});

            String indicators
                    = "d1" + newLine
                    + "d5" + newLine
                    + "d55\ttrue" + newLine
                    + "d59\tfalse" + newLine
                    + "d10" + newLine;

            indicatorFile = FileUtility.createTemporary();
            StreamUtil.copyStringToFile(indicators, indicatorFile);

            App.main(new String[]{"build-special", "--indexPath=" + indexFile.getAbsolutePath(),
                "--inputPath=" + indicatorFile.getAbsolutePath(), "--type=indicator",
                "--partName=testingIndicators", "--server=false"});

            DocumentIndicatorReader reader = new DocumentIndicatorReader(indexFile.getAbsolutePath() + File.separator + "testingIndicators");

            String output
                    = "0	true" + newLine
                    + "2	true" + newLine
                    + "3	false" + newLine;

            DocumentIndicatorReader.KeyIterator iterator = reader.getIterator();
            StringBuilder correct = new StringBuilder();
            do {
                correct.append(iterator.getCurrentDocument()).append("\t").append(iterator.getCurrentIndicator()).append(newLine);
            } while (iterator.nextKey());

            assert output.equals(correct.toString());

            // Test it as a value iterator
            IndicatorIterator vIt = reader.getIterator(StructuredQuery.parse("#indicator:part=testingIndicators()"));
            assertFalse(vIt.isDone());
            assertTrue(vIt.hasMatch(new ScoringContext(0)));
            vIt.movePast(0);
            assertTrue(vIt.hasMatch(new ScoringContext(2)));
            vIt.movePast(2);
            assertFalse(vIt.isDone());
            //assertFalse(vIt.hasMatch(3));  // jfoley - has a match, but the value is false
            assertFalse(vIt.indicator(new ScoringContext(3))); // this *makes sense* for an indicator function
            assertEquals(3, vIt.currentCandidate());
            vIt.movePast(3);
            assertTrue(vIt.isDone());

            // now test a query:
            String queries
                    = "{ \"queries\" : [" + newLine
                    + "{ \"number\" : \"1\", \"text\" : \"sample\" }," + newLine
                    + "{ \"number\" : \"2\", \"text\" : \"#require( #indicator:part=testingIndicators() #combine( sample ) )\" }" + newLine
                    + "]}" + newLine;

            queryFile = FileUtility.createTemporary();
            StreamUtil.copyStringToFile(queries, queryFile);

            // test with batch search
            ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
            PrintStream printStream = new PrintStream(byteArrayStream);

            new App().run(new String[]{"batch-search",
                "--index=" + indexFile.getAbsolutePath(),
                queryFile.getAbsolutePath()}, printStream);

            // Now, verify that some stuff exists
            String out = byteArrayStream.toString();

            String expected = "1 Q0 d10 1 -1.22350933 galago" + newLine
                    + "1 Q0 d11 2 -1.22350933 galago" + newLine
                    + "1 Q0 d59 3 -1.22350933 galago" + newLine
                    + "1 Q0 d73 4 -1.22350933 galago" + newLine
                    + "1 Q0 d55 5 -1.22483912 galago" + newLine
                    + "2 Q0 d10 1 -1.22350933 galago" + newLine
                    + "2 Q0 d55 2 -1.22483912 galago" + newLine;

            assertEquals(expected, out);

        } finally {
            if (trecCorpusFile != null) {
                trecCorpusFile.delete();
            }
            if (indicatorFile != null) {
                indicatorFile.delete();
            }
            if (queryFile != null) {
                queryFile.delete();
            }
            if (indexFile != null) {
                FSUtil.deleteDirectory(indexFile);
            }
        }
    }

    @Test
    public void testPriors() throws Exception {
        File trecCorpusFile = null;
        File priorFile = null;
        File indexFile = null;
        File queryFile = null;

        try {
            // create a simple doc file, trec format:
            String trecCorpus = trecDocument("10", "sample document four")
                    + trecDocument("11", "sample document five")
                    + trecDocument("55", "This is a sample document")
                    + trecDocument("59", "sample document two")
                    + trecDocument("73", "sample document three");
            trecCorpusFile = FileUtility.createTemporary();
            StreamUtil.copyStringToFile(trecCorpus, trecCorpusFile);

            String priors
                    = "d10\t-23.0259" + newLine
                    + "d11\t-1e-10" + newLine
                    + "d59\t-7.0" + newLine
                    + "d73\t-6.0" + newLine;

            priorFile = FileUtility.createTemporary();
            StreamUtil.copyStringToFile(priors, priorFile);

            // now, try to build an index from that
            indexFile = FileUtility.createTemporary();
            indexFile.delete();
            App.main(new String[]{"build", "--indexPath=" + indexFile.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile.getAbsolutePath(), "--server=false"});

            App.main(new String[]{"build-special", "--indexPath=" + indexFile.getAbsolutePath(),
                "--inputPath=" + priorFile.getAbsolutePath(), "--type=prior",
                "--partName=testingPriors", "--server=false"});

            DocumentPriorReader reader = new DocumentPriorReader(indexFile.getAbsolutePath() + File.separator + "testingPriors");

            HashMap<Integer, Double> priorData = new HashMap();
            priorData.put(0, -23.0259);
            priorData.put(1, -1e-10);
            priorData.put(3, -7.0);
            priorData.put(4, -6.0);

            DocumentPriorReader.KeyIterator iterator = reader.getIterator();
            do {
                int doc = iterator.getCurrentDocument();
                double score = iterator.getCurrentScore();
                assert (priorData.get(doc) == score);
            } while (iterator.nextKey());

            // test it as a basic iterator 
            ScoreIterator vIt = (ScoreIterator) reader.getIterator(StructuredQuery.parse("#prior:part=testingPriors()"));
            ScoringContext context = new ScoringContext();

            context.document = 0;
            assertFalse(vIt.isDone());
            assertEquals(-23.0259, vIt.score(context), 0.001);
            vIt.movePast(0);
            context.document = 1;
            assertEquals(-1e-10, vIt.score(context), 0.0001);
            vIt.movePast(1);
            context.document = 3;
            assertEquals(-7.0, vIt.score(context), 0.001);
            vIt.movePast(3);
            context.document = 4;
            assertEquals(-6.0, vIt.score(context), 0.001);
            vIt.movePast(4);
            assertTrue(vIt.isDone());

            // now test a query:
            String queries
                    = "{ \"queries\" : [" + newLine
                    + "{ \"number\" : \"1\", \"text\" : \"sample\" }," + newLine
                    + "{ \"number\" : \"2\", \"text\" : \"#combine( #prior:part=testingPriors() sample )\" }" + newLine
                    + "]}" + newLine;

            queryFile = FileUtility.createTemporary();
            StreamUtil.copyStringToFile(queries, queryFile);

            // test with batch search
            ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
            PrintStream printStream = new PrintStream(byteArrayStream);

            App.run(new String[]{"batch-search",
                "--index=" + indexFile.getAbsolutePath(),
                queryFile.getAbsolutePath()}, printStream);

            // Now, verify that some stuff exists
            String out = byteArrayStream.toString();

            String expected = "1 Q0 d10 1 -1.22350933 galago" + newLine
                    + "1 Q0 d11 2 -1.22350933 galago" + newLine
                    + "1 Q0 d59 3 -1.22350933 galago" + newLine
                    + "1 Q0 d73 4 -1.22350933 galago" + newLine
                    + "1 Q0 d55 5 -1.22483912 galago" + newLine
                    + "2 Q0 d11 1 -0.61175467 galago" + newLine
                    + "2 Q0 d73 2 -3.61175467 galago" + newLine
                    + "2 Q0 d59 3 -4.11175467 galago" + newLine
                    + "2 Q0 d10 4 -12.12470467 galago" + newLine
                    + "2 Q0 d55 5 -12.12534503 galago" + newLine;

      //      System.err.println(expected);
            //      System.err.println(out);
            assertEquals(expected, out);

        } finally {
            if (trecCorpusFile != null) {
                trecCorpusFile.delete();
            }
            if (priorFile != null) {
                priorFile.delete();
            }
            if (queryFile != null) {
                queryFile.delete();
            }
            if (indexFile != null) {
                FSUtil.deleteDirectory(indexFile);
            }
        }
    }
}
