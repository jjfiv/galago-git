/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.lemurproject.galago.core.links.pagerank.TypeFileReader;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.core.types.DocumentUrl;
import org.lemurproject.galago.core.types.ExtractedLink;
import org.lemurproject.galago.core.types.PageRankScore;
import org.lemurproject.galago.tupleflow.FileSource;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.Order;
import org.lemurproject.galago.tupleflow.OrderedCombiner;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.ReaderSource;
import org.lemurproject.galago.tupleflow.Splitter;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ConnectionAssignmentType;
import org.lemurproject.galago.tupleflow.execution.InputStep;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.OutputStep;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.Step;
import org.lemurproject.galago.tupleflow.types.FileName;

/**
 *
 * @author sjh
 */
public class PageRankFn extends AppFunction {

  @Override
  public String getName() {
    return "pagerank";
  }

  @Override
  public String getHelpString() {
    return "galago pagerank [parameters]\n\n"
            + "Parameters\n"
            + "\tlinkdata=/path/to/harvest/link/data/\n"
            + "\toutputFolder=/path/to/write/data/\n"
            + "\n"
            + getTupleFlowParameterString();
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    if (!p.containsKey("linkdata")) {
      output.println(getHelpString());
      return;
    }

    initialize(p);

    int maxItrs = (int) p.get("maxItr", 10);
    for (int i = 1; i <= maxItrs; i++) {
      Job itr = getIterationJob(p, i);

      boolean success = runTupleFlowJob(itr, p, output);
      if (!success) {
        output.println("PAGE RANK FAILED TO EXECUTE.");
        return;
      }
      if (checkConvergence(i, p)) {
        output.println("PAGE RANK CONVERGED.");
        return;
      }
    }
    output.println("PAGE RANK MAX-ITR REACHED.");
  }

  private void initialize(Parameters p) throws IOException, IncompatibleProcessorException {

    File outputFolder = new File(p.getString("outputFolder"));
    File itrZero = new File(outputFolder, "pageranks.0");
    itrZero.mkdirs();

    File inputData = new File(p.getString("linkdata"));
    File namesFolder = new File(inputData, "names");

    List<String> inputFiles = Arrays.asList(namesFolder.list());
    List<String> outputFiles = new ArrayList();

    for (int i = 0; i < p.get("distrib", 10); i++) {
      outputFiles.add(new File(outputFolder, "pageranks." + Integer.toString(i)).getAbsolutePath());
    }

    double defaultScore = 1.0;
    if (p.containsKey("defaultScore")) {
      defaultScore = p.getDouble("defaultScore");

    } else {
      // determine correct default score
      // two passes -- first count the number of documents
      double docCount = 0;
      ReaderSource<DocumentUrl> reader = OrderedCombiner.combineFromFiles(inputFiles, new DocumentUrl.IdentifierOrder());
      DocumentUrl url = reader.read();
      while (url != null) {
        docCount += 1;
        url = reader.read();
      }
      defaultScore = 1.0 / docCount;
    }


    // now initialize pagerank scores to 1/|C|
    ReaderSource<DocumentUrl> reader = OrderedCombiner.combineFromFiles(inputFiles, new DocumentUrl.IdentifierOrder());

    Processor<PageRankScore> writer;
    writer = Splitter.splitToFiles(outputFiles.toArray(new String[0]), new PageRankScore.DocNameOrder(), new PageRankScore.DocNameOrder());

    DocumentUrl url = reader.read();
    while (url != null) {
      url = reader.read();
      PageRankScore score = new PageRankScore(url.identifier, defaultScore);
      writer.process(score);
    }

    writer.close();
  }

  private boolean checkConvergence(int i, Parameters p) {
    File conv = new File("pagerank.convergence." + i);
    if (conv.exists()) {
      return true;
    } else {
      return false;
    }
  }

  private Job getIterationJob(Parameters p, int i) {
    Job job = new Job();

    File linkData = new File(p.getString("linkdata"));
    File srcLinksFolder = new File(linkData, "srcNameOrder");

    File outputFolder = new File(p.getString("outputFolder"));
    File itrInput = new File(outputFolder, "pageranks." + (i - 1));
    File itrOutput = new File(outputFolder, "pageranks." + i);
    itrOutput.mkdir();

    // stage 1: list folders:  pageranks, and linkdata
    job.add(getSplitStage("splitScores", "scoreFiles", itrInput));
    job.add(getSplitStage("splitLinks", "linkFiles", srcLinksFolder));

    // stage 2: read files : emit pageranks and links to nodes
    job.add(getReaderStage("scoreReader","scoreFiles", "inputScores", PageRankScore.class.getName(), new PageRankScore.DocNameOrder()));
    job.add(getReaderStage("linkReader","linkFiles", "inputLinks", ExtractedLink.class.getName(), new ExtractedLink.SrcNameOrder()));

    // stage 3: process links (emit 1-lambda * score / linkcount) as partial scores
    //          alternatively emit 1-lambda as a random jump
    job.add(getRandomWalkStage(p));

    // stage 4: process random jumps (sum total pagerank, emit as random jump)
    job.add(getRandomJumpStage(p));

    // stage 5: reduce scores by document count, emit to files.
    job.add(getProcessPartialStage(p));

    // stage 6: compute differences (for convergence check)
    job.add(getConvergenceStage(p));

    job.connect("splitScores", "scoreReader", ConnectionAssignmentType.Each);
    job.connect("splitLinks", "linkReader", ConnectionAssignmentType.Each);

    job.connect("scoreReader", "rndWalk", ConnectionAssignmentType.Each);
    job.connect("linkReader", "rndWalk", ConnectionAssignmentType.Each);

    job.connect("scoreReader", "rndJump", ConnectionAssignmentType.Each);

    job.connect("rndWalk", "reducer", ConnectionAssignmentType.Each);
    job.connect("rndJump", "reducer", ConnectionAssignmentType.Each);

    // would prefer 'each', but this isn't very expensive.
    job.connect("reader", "convergence", ConnectionAssignmentType.Combined);
    job.connect("reducer", "convergence", ConnectionAssignmentType.Combined);

    return job;
  }

  private Stage getSplitStage(String name, String outputName, File folder) {
    // list directories, and generate several files.
    // two output streams (pagerank files, and 

    Stage stage = new Stage(name);
    stage.addOutput(outputName, new FileName.FilenameOrder());

    Parameters inParams = new Parameters();
    inParams.set("input", folder.getAbsolutePath());
    stage.add(new Step(FileSource.class, inParams));
    stage.add(Utility.getSorter(new FileName.FilenameOrder()));
    stage.add(new OutputStep(outputName));

    return stage;
  }

  private Stage getReaderStage(String name, String input, String output, String outputClass, Order order) {
    Stage stage = new Stage(name);

    stage.addInput(input, new FileName.FilenameOrder());
    stage.addOutput(output, order);

    stage.add(new InputStep(input));
    
    Parameters p = new Parameters();
    p.set("outputOrder", order.getClass().toString());
    p.set("outputClass", outputClass);
    stage.add(new Step(TypeFileReader.class, p));
    stage.add(new OutputStep(output));

    return stage;
  }

  private Stage getRandomWalkStage(Parameters p) {
    Stage stage = new Stage("rndWalk");
    
    stage.addInput("inputScores", null)
    stage.addInput("inputLinks", null)
    
    reutrn stage;
  }
}
