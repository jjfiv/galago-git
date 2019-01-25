
package org.lemurproject.galago.core;

import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.utility.StreamUtil;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author michaelz
 *
 * Note: this really should live in org.lemurproject.galago.core.eval but because we're
 * using App, it would result in a circular dependency.
 */
public class EvalTest {

    String qrels_file_name;
    List<String> results_files = new ArrayList<>();

    @Before
    public void setUp() throws IOException {
        File qrel = File.createTempFile("qrel", "");
        qrel.deleteOnExit();
        qrels_file_name = qrel.getAbsolutePath();

        String qrels =
                "1 0 doc0 1\n" +
                        "1 0 doc1 0\n" +
                        "2 0 doc2 1\n" +
                        "2 0 doc3 1\n";

        StreamUtil.copyStringToFile(qrels, qrel);

        // create results files
        File results_1 = File.createTempFile("results", "");
        results_1.deleteOnExit();
        results_files.add(results_1.getAbsolutePath());

        String results_1_text =
                "1 Q0 doc1 1 -9.61367426 galago\n" +
                        "2 Q0 doc3 1 -9.51367426 galago\n";

        StreamUtil.copyStringToFile(results_1_text, results_1);

        File results_2 = File.createTempFile("results", "");
        results_2.deleteOnExit();
        results_files.add(results_2.getAbsolutePath());

        String results_2_text =
                "1 Q0 doc1 1 -6.14587456 galago\n" +
                        "1 Q0 doc0 2 -8.54678414 galago\n" +
                        "2 Q0 doc2 1 -8.54876556 galago\n";

        StreamUtil.copyStringToFile(results_2_text, results_2);

    }

    @Test
    public void testEvalSets() throws Exception {

        ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(byteArrayStream);

        App.run(new String[]{"eval",
                        "--metrics+map",
                        "--runs+" + results_files.get(0),
                        "--runs+" + results_files.get(1),
                        "--judgments=" + qrels_file_name},
                printStream);

        String output = byteArrayStream.toString();
        String[] outputLines = output.split(System.lineSeparator());

        assertEquals("0.250", outputLines[1].trim().replaceAll(" +", " ").split(" ")[1]);
        assertEquals("0.500", outputLines[2].trim().replaceAll(" +", " ").split(" ")[1]);

    }

    @Test
    public void testEvalSetsPrecision1() throws Exception {

        ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(byteArrayStream);

        // test precision argument
        App.run(new String[]{"eval",
                        "--metrics+map",
                        "--precision=5",
                        "--runs+" + results_files.get(0),
                        "--runs+" + results_files.get(1),
                        "--judgments=" + qrels_file_name},
                printStream);

        String output = byteArrayStream.toString();
        String[] outputLines = output.split(System.lineSeparator());

        assertEquals("0.25000", outputLines[1].trim().replaceAll(" +", " ").split(" ")[1]);
        assertEquals("0.50000", outputLines[2].trim().replaceAll(" +", " ").split(" ")[1]);

    }

    @Test
    public void testEvalSetsPrecision2() throws Exception {

        ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(byteArrayStream);

        // test precision argument
        App.run(new String[]{"eval",
                        "--metrics+map",
                        "--precision=2",
                        "--runs+" + results_files.get(0),
                        "--runs+" + results_files.get(1),
                        "--judgments=" + qrels_file_name},
                printStream);

        String output = byteArrayStream.toString();
        String[] outputLines = output.split(System.lineSeparator());

        assertEquals("0.25", outputLines[1].trim().replaceAll(" +", " ").split(" ")[1]);
        assertEquals("0.50", outputLines[2].trim().replaceAll(" +", " ").split(" ")[1]);

    }
}
