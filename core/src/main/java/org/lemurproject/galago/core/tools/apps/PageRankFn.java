/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.lemurproject.galago.core.links.pagerank.ComputeRandomJump;
import org.lemurproject.galago.core.links.pagerank.ComputeRandomWalk;
import org.lemurproject.galago.core.links.pagerank.ConvergenceTester;
import org.lemurproject.galago.core.links.pagerank.PageRankScoreCombiner;
import org.lemurproject.galago.core.links.pagerank.TypeFileReader;
import org.lemurproject.galago.core.links.pagerank.TypeFileWriter;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.core.types.DocumentUrl;
import org.lemurproject.galago.core.types.ExtractedLink;
import org.lemurproject.galago.core.types.PageRankJumpScore;
import org.lemurproject.galago.core.types.PageRankScore;
import org.lemurproject.galago.tupleflow.FileSource;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.Order;
import org.lemurproject.galago.tupleflow.OrderedCombiner;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.ReaderSource;
import org.lemurproject.galago.tupleflow.Sorter;
import org.lemurproject.galago.tupleflow.Splitter;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ConnectionAssignmentType;
import org.lemurproject.galago.tupleflow.execution.InputStep;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.MultiStep;
import org.lemurproject.galago.tupleflow.execution.OutputStep;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.Step;
import org.lemurproject.galago.tupleflow.types.FileName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author sjh
 */
public class PageRankFn extends AppFunction {

  private static final Logger logger = LoggerFactory.getLogger("Pagerank");

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
            + "\tlambda=[0.5] \n"
            + "\tdelta=[0.000001] \n"
            + "\tmaxItr=10 \n"
            + "\tdefaultScore=1/||D|| \n"
            + "\tdeleteIntData=false \n"
            + "\n"
            + getTupleFlowParameterString();
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    if (!p.containsKey("linkdata")) {
      output.println(getHelpString());
      return;
    }

    if (!p.containsKey("lambda")) {
      p.set("lambda", 0.5);
    }
    // 0 <= lambda <= 1
    assert (p.getDouble("lambda") <= 1.0 && p.getDouble("lambda") >= 0.0);

    File outputFolder = new File(p.getString("outputFolder"));

    logger.info("Initializing...");
    initialize(p);

    int maxItrs = (int) p.get("maxItr", 10);
    int convergedAt = 0;
    for (int i = 1; i <= maxItrs; i++) {

      logger.info("Starting iteration " + i);

      Job itr = getIterationJob(p, i);

      Parameters runParams = new Parameters();
      if (p.isLong("distrib")) {
        runParams.set("distrib", p.getLong("distrib"));
      }
      if (p.isLong("port")) {
        runParams.set("port", p.getLong("port"));
      }
      if (p.isBoolean("server")) {
        runParams.set("server", p.getBoolean("server"));
      }
      if (p.isString("mode")) {
        runParams.set("mode", p.getString("mode"));
      }

      runParams.set("galagoJobDir", new File(outputFolder, "pagerank-job-tmp." + i).getAbsolutePath());
      runParams.set("deleteJobDir", p.get("deleteJobDir", true));

      boolean success = runTupleFlowJob(itr, runParams, output);
      convergedAt = i;

      if (!success) {
        logger.warn("PAGERANK FAILED TO EXECUTE.");
        return;
      }
      if (checkConvergence(i, p)) {
        logger.info("Converged at " + i);
        break;
      }
    }
    if (convergedAt > maxItrs) {
      logger.info("MaxIterations reached at " + maxItrs);
    }

    logger.info("Finalizing...");
    finalize(p, convergedAt);
    logger.info("Finished");
  }

  private void initialize(Parameters p) throws IOException, IncompatibleProcessorException {

    File outputFolder = new File(p.getString("outputFolder"));
    File itrZero = new File(outputFolder, "pageranks.0");
    itrZero.mkdirs();

    File inputData = new File(p.getString("linkdata"));
    File namesFolder = new File(inputData, "names");

    List<String> inputFiles = new ArrayList();
    for (File f : namesFolder.listFiles()) {
      System.err.println(f.getAbsolutePath());
      inputFiles.add(f.getAbsolutePath());
    }

    double defaultScore = 1.0;
    if (p.containsKey("defaultScore")) {
      defaultScore = p.getDouble("defaultScore");

    } else {
      // determine correct default score
      // two passes -- first count the number of documents
      double docCount = 0;
      ReaderSource<DocumentUrl> reader = OrderedCombiner.combineFromFileObjs(Arrays.asList(namesFolder.listFiles()), new DocumentUrl.IdentifierOrder());
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
    writer = Splitter.splitToFiles(itrZero.getAbsolutePath() + "/pagerank.", new PageRankScore.DocNameOrder(), (int) p.get("distrib", 10));

    DocumentUrl url = reader.read();
    while (url != null) {
      PageRankScore score = new PageRankScore(url.identifier, defaultScore);
      writer.process(score);
      url = reader.read();
    }

    writer.close();
  }

  private void finalize(Parameters p, int convergenceItr) throws IOException, IncompatibleProcessorException {
    // open reader over final pageranks
    File outputFolder = new File(p.getString("outputFolder"));
    File finalPageranks = new File(outputFolder, "pageranks." + convergenceItr);

    final File docNameOutput = new File(p.getString("outputFolder"), "pagerank.docNameOrder");
    final File scoreOutput = new File(p.getString("outputFolder"), "pagerank.scoreOrder");



    // copy into single file: <doc-name> <score>\n
    ReaderSource<PageRankScore> reader = OrderedCombiner.combineFromFileObjs(Arrays.asList(finalPageranks.listFiles()), new PageRankScore.DocNameOrder());
    BufferedWriter docOrderWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(docNameOutput)));
    PageRankScore docScore;
    while ((docScore = reader.read()) != null) {
      docOrderWriter.write(String.format("%s %.10f\n", docScore.docName, docScore.score));
    }
    docOrderWriter.close();



    // copy into single file: "<doc-name> <score>\n"
    // but this time score by scores (descending)
    reader = OrderedCombiner.combineFromFileObjs(Arrays.asList(finalPageranks.listFiles()), new PageRankScore.DocNameOrder());
    Sorter sorter = new Sorter(new PageRankScore.DescScoreOrder());
    Processor<PageRankScore> writer = new Processor<PageRankScore>() {

      private BufferedWriter scoreOrderWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(scoreOutput)));

      @Override
      public void process(PageRankScore docScore) throws IOException {
        scoreOrderWriter.write(String.format("%s %.10f\n", docScore.docName, docScore.score));
      }

      @Override
      public void close() throws IOException {
        scoreOrderWriter.close();
      }
    };

    reader.setProcessor(sorter);
    sorter.setProcessor(writer);
    reader.run();

    logger.info("...done writing output.");
    
    // finally if requested -- delete all intermediate data.
    if (p.get("deleteIntData", false)) {
      logger.info("Deleting intermediate data.");
      for (File f : outputFolder.listFiles()) {
        if (!f.getName().equals("pagerank.docNameOrder")
                && !f.getName().equals("pagerank.scoreOrder")) {
          if (f.isFile()) {
            f.delete();
          } else if (f.isDirectory()) {
            Utility.deleteDirectory(f);
          }
        }
      }
    }
  }

  private boolean checkConvergence(int i, Parameters p) {
    File conv = new File(p.getString("outputFolder"), "pagerank.converged." + i);
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
    File convgFile = new File(outputFolder, "pagerank.converged." + i);
    itrOutput.mkdir();

    // stage 1: list folders:  pageranks, and linkdata
    job.add(getSplitStage("splitScores", "scoreFiles", itrInput));
    job.add(getSplitStage("splitLinks", "linkFiles", srcLinksFolder));

    // stage 2: read files : emit pageranks and links to nodes
    job.add(getReaderStage("scoreReader", "scoreFiles", "inputScores", PageRankScore.class.getCanonicalName(), new PageRankScore.DocNameOrder()));
    job.add(getReaderStage("linkReader", "linkFiles", "inputLinks", ExtractedLink.class.getCanonicalName(), new ExtractedLink.SrcNameOrder()));

    // stage 3: process links (emit 1-lambda * score / linkcount) as partial scores
    //          alternatively emit 1-lambda as a random jump
    job.add(getRandomWalkStage(p));

    // stage 4: process random jumps (sum total pagerank, emit as random jump)
    job.add(getRandomJumpStage(p));

    // stage 5: reduce scores by document count, emit to files.
    job.add(getProcessPartialStage(p, new File(itrOutput, "pagerank.")));

    // stage 6: compute differences (for convergence check)
    job.add(getConvergenceStage(p, convgFile));

    job.connect("splitScores", "scoreReader", ConnectionAssignmentType.Each);
    job.connect("splitLinks", "linkReader", ConnectionAssignmentType.Each);

    job.connect("scoreReader", "rndWalk", ConnectionAssignmentType.Each);
    job.connect("linkReader", "rndWalk", ConnectionAssignmentType.Each);

    job.connect("scoreReader", "rndJump", ConnectionAssignmentType.Each);

    job.connect("scoreReader", "reducer", ConnectionAssignmentType.Each);
    job.connect("rndWalk", "reducer", ConnectionAssignmentType.Each);
    job.connect("rndJump", "reducer", ConnectionAssignmentType.Each);

    // would prefer 'each', but this isn't very expensive.
    job.connect("scoreReader", "convergence", ConnectionAssignmentType.Combined);
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
    p.set("order", Utility.join(order.getOrderSpec()));
    p.set("outputClass", outputClass);
    stage.add(new Step(TypeFileReader.class, p));
    // this reader ensures correct order.
    stage.add(new OutputStep(output));

    return stage;
  }

  private Stage getRandomWalkStage(Parameters p) {
    Stage stage = new Stage("rndWalk");

    stage.addInput("inputScores", new PageRankScore.DocNameOrder());
    stage.addInput("inputLinks", new ExtractedLink.SrcNameOrder());
    stage.addOutput("outputPartialScores", new PageRankScore.DocNameOrder());
    stage.addOutput("outputExtraJumps", new PageRankJumpScore.ScoreOrder());


    stage.add(new InputStep("inputScores"));
    Parameters rndWalk = new Parameters();
    rndWalk.set("linkStream", "inputLinks");
    rndWalk.set("jumpStream", "outputExtraJumps");
    rndWalk.set("lambda", p.getDouble("lambda"));
    stage.add(new Step(ComputeRandomWalk.class, rndWalk));
    stage.add(Utility.getSorter(new PageRankScore.DocNameOrder()));
    stage.add(new OutputStep("outputPartialScores"));

    return stage;
  }

  private Stage getRandomJumpStage(Parameters p) {
    Stage stage = new Stage("rndJump");

    stage.addInput("inputScores", new PageRankScore.DocNameOrder());
    stage.addOutput("outputCumulativeJump", new PageRankJumpScore.ScoreOrder());

    stage.add(new InputStep("inputScores"));
    // sum the scores, divide by count of pages, 
    // then emit single value that is the value of all random jumps to any given page.
    Parameters rndJumpParams = new Parameters();
    rndJumpParams.set("lambda", p.getDouble("lambda"));
    stage.add(new Step(ComputeRandomJump.class, rndJumpParams));

    // should only emit one item, but still...
    stage.add(Utility.getSorter(new PageRankJumpScore.ScoreOrder()));
    stage.add(new OutputStep("outputCumulativeJump"));

    return stage;
  }

  private Stage getProcessPartialStage(Parameters p, File outputFilePrefix) {
    Stage stage = new Stage("reducer");

    stage.addInput("inputScores", new PageRankScore.DocNameOrder());
    stage.addInput("outputPartialScores", new PageRankScore.DocNameOrder());
    stage.addInput("outputExtraJumps", new PageRankJumpScore.ScoreOrder());
    stage.addInput("outputCumulativeJump", new PageRankJumpScore.ScoreOrder());
    stage.addOutput("outputScores", new PageRankScore.DocNameOrder());

    stage.add(new InputStep("inputScores"));

    Parameters combinerParams = new Parameters();
    combinerParams.set("jumpStream1", "outputCumulativeJump");
    combinerParams.set("jumpStream2", "outputExtraJumps");
    combinerParams.set("scoreStream", "outputPartialScores");
    stage.add(new Step(PageRankScoreCombiner.class, combinerParams));
    stage.add(Utility.getSorter(new PageRankScore.DocNameOrder()));

    MultiStep processingFork = new MultiStep();
    processingFork.addGroup("writer");
    processingFork.addGroup("stream");

    Parameters writerParams = new Parameters();
    writerParams.set("outputFile", outputFilePrefix.getAbsolutePath()); // + i
    writerParams.set("class", PageRankScore.class.getCanonicalName());
    writerParams.set("order", Utility.join(new PageRankScore.DocNameOrder().getOrderSpec()));

    processingFork.addToGroup("writer", new Step(TypeFileWriter.class, writerParams));

    processingFork.addToGroup("stream", new OutputStep("outputScores"));

    stage.add(processingFork);

    return stage;

  }

  private Stage getConvergenceStage(Parameters p, File conv) {
    Stage stage = new Stage("convergence");

    stage.addInput("inputScores", new PageRankScore.DocNameOrder());
    stage.addInput("outputScores", new PageRankScore.DocNameOrder());

    Parameters convgParams = new Parameters();
    convgParams.set("convFile", conv.getAbsolutePath());
    convgParams.set("prevScoreStream", "inputScores");
    convgParams.set("currScoreStream", "outputScores");
    convgParams.set("delta", p.get("delta", 0.000001));
    stage.add(new Step(ConvergenceTester.class, convgParams));
    // if converged, a file is created in the output directory.

    return stage;
  }
}
