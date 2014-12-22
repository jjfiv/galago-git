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
import java.util.HashSet;
import java.util.Set;
import org.lemurproject.galago.core.index.disk.DiskNameReverseReader;
import org.lemurproject.galago.core.index.disk.DiskNameReverseReader.KeyIterator;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;
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
            + "\t--partialIndex=/path/to/output/index/\n"
            + getTupleFlowParameterString();
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
      System.err.println("Corpus is required!");
    }

    File documentNames = new File(p.getString("documentNameList"));
    File documentIds = new File(p.getString("documentNameList") + ".ids");
    collectIds(documentNames, index, documentIds, (int) p.get("distrib", 10));


    // force input format
    p.set("filetype", "selectivecorpus");
    p.set("inputPath", documentIds.getAbsolutePath());
    p.set("indexPath", outputIndex.getAbsolutePath());

    if (!p.isMap("parser")) {
      p.set("parser", Parameters.create());
    }
    p.getMap("parser").set("corpus", corpus.getAbsolutePath());

    Job job = new BuildIndex().getIndexJob(p);

    AppFunction.runTupleFlowJob(job, p, output);
  }

  private static void collectIds(File documentNames, File index, File documentIds, int distrib) throws Exception {

    // read document names 
    DiskNameReverseReader namesReader = new DiskNameReverseReader(new File(index, "names.reverse").getAbsolutePath());
    KeyIterator namesIterator = namesReader.getIterator();

    BufferedReader input = new BufferedReader(new FileReader(documentNames));
    String line;
    Set<Long> ids = new HashSet();
    while ((line = input.readLine()) != null) {
      line = line.trim();
      byte[] name = ByteUtil.fromString(line);
      namesIterator.findKey(name);
      if (namesIterator.getCurrentName().equals(line)) {
        long id = namesIterator.getCurrentIdentifier();
        // round-robin distribution
        ids.add(id);
      } else {
        System.err.println("Unable to determine document : " + ByteUtil.toString(name) + " ignoring.");
      }
    }
    input.close();
    
    // make a folder for these files
    if(documentIds.isDirectory()){
      FSUtil.deleteDirectory(documentIds);
    }
    if(documentIds.isFile()){
      documentIds.delete();
    }

    documentIds.mkdirs();
    BufferedWriter[] writers = new BufferedWriter[distrib];
    for (int i = 0; i < distrib; i++) {
      writers[i] = new BufferedWriter(new FileWriter(new File(documentIds, "" + i)));
    }
    
    int j=0;
    for(Long id : ids){
      writers[j % distrib].write(id + "\n");
      j+=1;
    }
    
    // close up
    for (int i = 0; i < distrib; i++) {
      writers[i].close();
    }
  }
}
