/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintStream;
import org.lemurproject.galago.core.index.disk.DiskNameReverseReader;
import org.lemurproject.galago.core.index.disk.DiskNameReverseReader.KeyIterator;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.core.tools.BuildIndex;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Job;

/**
 *
 * @author sjh
 */
public class BuildPartialIndex extends AppFunction {

  @Override
  public String getName() {
    return "build-partial-index";
  }

  @Override
  public String getHelpString() {
    return "galago build-partial-index <parameters>\n"
            + "\n"
            + "\tBuilds a partial index from an existing index\n"
            + "\tusing a corpus structure. Assumes a small list of documents\n"
            + "\n"
            + "\t--documentNameList=</path/to/file>\n"
            + "\t--index=/path/to/input/index/\n"
            + "\t--partialIndex=/path/to/output/index/\n";
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    if (!p.isString("documentNameList")
            || !p.isString("index")) {
      output.println(getHelpString());
      return;
    }

    createPartialIndex(p, output);
  }

  private static void createPartialIndex(Parameters p, PrintStream output) throws Exception {
    File index = new File(p.getString("index"));
    File corpus = new File(p.getString("index"), "corpus");
    File outputIndex = new File(p.getString("partialIndex"));

    if (!corpus.exists()) {
      System.err.println("corpus is required!");
    }

    File documentNames = new File(p.getString("documentNameList"));
    File documentIds = new File(p.getString("documentNameList") + ".ids");
    collectIds(documentNames, index, documentIds);


    p.set("inputPath", corpus.getAbsolutePath());
    p.set("indexPath", outputIndex.getAbsolutePath());

    if (!p.isMap("parser")) {
      p.set("parser", new Parameters());
    }
    p.getMap("parser").set("docIds", documentIds.getAbsolutePath());
    // force input format
    p.set("filetype", "selectivecorpus");
    // force distrib factor == 1 -- otherwise duplicate documents
    p.set("distrib", 1);

    Job job = new BuildIndex().getIndexJob(p);

    AppFunction.runTupleFlowJob(job, p, output);
  }

  private static void collectIds(File documentNames, File index, File documentIds) throws Exception {
    DiskNameReverseReader namesReader = new DiskNameReverseReader(new File(index, "names.reverse").getAbsolutePath());
    KeyIterator namesIterator = namesReader.getIterator();

    BufferedReader input = new BufferedReader(new FileReader(documentNames));
    BufferedWriter output = new BufferedWriter(new FileWriter(documentIds));
    String line;
    while ((line = input.readLine()) != null) {
      line = line.trim();
      byte[] name = Utility.fromString(line);
      namesIterator.findKey(name);
      if (namesIterator.getCurrentName().equals(line)) {
        int id = namesIterator.getCurrentIdentifier();
        output.write(id + "\n");
      } else {
        System.err.println("Unable to determine document : " + line + " ignoring.");
      }
    }
    input.close();
    output.close();
  }
}
