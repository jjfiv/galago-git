/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import org.lemurproject.galago.core.links.pagerank.*;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.core.types.DocumentUrl;
import org.lemurproject.galago.core.types.ExtractedLink;
import org.lemurproject.galago.core.types.PageRankJumpScore;
import org.lemurproject.galago.core.types.PageRankScore;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.execution.*;
import org.lemurproject.galago.tupleflow.execution.Step;
import org.lemurproject.galago.tupleflow.types.FileName;
import org.lemurproject.galago.tupleflow.types.TupleflowLong;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.logging.Logger;

/**
 *
 * @author sjh
 */
public class PageRankFn extends AppFunction {

  private static final Logger logger = Logger.getLogger("Pagerank");

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
    long docCount = initialize(p, output);

    if (docCount == 0) {
      output.println("failed to initialize. Aborting.");
      return;
    }

    // ensure a correct document count
    p.set("docCount", docCount);

    int maxItrs = (int) p.get("maxItr", 10);
    int convergedAt = 0;
    for (int i = 1; i <= maxItrs; i++) {

      logger.info("Starting iteration " + i);

      File itrIn = new File(outputFolder, "pageranks." + (i-1));
      File itrOut = new File(outputFolder, "pageranks." + i);
      
      // if we haven't produced this output yet:
      if (!itrOut.isDirectory()) {
        File itrOutTmp = new File(outputFolder, "pageranks." + i + ".tmp");

        Job itr = getIterationJob(p, itrIn, itrOutTmp, i);

        boolean success = runTupleFlowInstance(itr, new File(outputFolder, "pagerank-job-tmp." + i), p, output);

        if (!success) {
          logger.warning("PAGERANK FAILED TO EXECUTE.");
          return;
        }

        itrOutTmp.renameTo(itrOut);
      }

      convergedAt = i;
      if (checkConvergence(i, p)) {
        logger.info("Converged at " + i);
        break;
      }
    }
    if (convergedAt > maxItrs) {
      logger.info("MaxIterations reached at " + maxItrs);
    }

    logger.info("Finalizing...");
    finalize(p, convergedAt, output);
    logger.info("Finished");
  }

  /**
   * Creates initial pagerank scores for documents;
   *   1 / ||D||
   *  Stores data in intermediate folder: <tmp-folder>/pagerank.0
   * 
   *  returns docCount
   */
  private long initialize(Parameters p, PrintStream outputStream) throws IOException, IncompatibleProcessorException, Exception {

    Job job = new Job();

    File inputData = new File(p.getString("linkdata"));
    File namesFolder = new File(inputData, "names");

    File outputFolder = new File(p.getString("outputFolder"));
    File itrZeroFoldr = new File(outputFolder, "pageranks.0");
    File itrZeroFoldrTmp = new File(outputFolder, "pageranks.0.tmp");

    File itrZeroprefix = new File(itrZeroFoldrTmp, "pagerank.");
    File docCountFolder = new File(outputFolder, "docCount");

    // check if init has already run.
    boolean success = itrZeroFoldr.isDirectory();
    if (!success) {

      itrZeroFoldrTmp.mkdirs();

      docCountFolder.mkdirs();

      // stage 1: list folders:  pageranks, and linkdata
      job.add(getSplitStage("splitUrls", "urls", namesFolder));

      // stage 2 or 3: write inital scores to a folder
      Stage writer = new Stage("writer");
      writer.addInput("urls", new FileName.FilenameOrder());

      writer.add(new InputStep("urls"));
      Parameters readerParams = Parameters.instance();
      readerParams.set("order", Utility.join(new DocumentUrl.IdentifierOrder().getOrderSpec()));
      readerParams.set("outputClass", DocumentUrl.class.getCanonicalName());
      writer.add(new Step(TypeFileReader.class, readerParams));

      Parameters writerParams = Parameters.instance();
      writerParams.set("outputFile", itrZeroprefix.getAbsolutePath()); // + i
      writerParams.set("class", PageRankScore.class.getCanonicalName());
      writerParams.set("order", Utility.join(new PageRankScore.DocNameOrder().getOrderSpec()));
      writerParams.set("compression", "GZIP");

      if (p.containsKey("defaultScore")) {
        writerParams.set("defaultScore", p.getDouble("defaultScore"));
      } else {
        writerParams.set("docCountStream", "docCount");
      }

      writerParams.set("docCountFolder", docCountFolder.getAbsolutePath());

      writer.add(new Step(UrlToInitialPagerankScore.class, writerParams));
      writer.add(new Step(TypeFileWriter.class, writerParams));

      job.add(writer);
      job.connect("splitUrls", "writer", ConnectionAssignmentType.Each);


      // special stage to count the number of documents
      if (!p.containsKey("defaultScore")) {
        // this is used to initialize scores

        Stage counterStage = new Stage("counter");
        counterStage.addInput("urls", new FileName.FilenameOrder());
        counterStage.addOutput("docCount", new TupleflowLong.ValueOrder());

        counterStage.add(new InputStep("urls"));
        Parameters counterParams1 = Parameters.instance();
        counterParams1.set("order", Utility.join(new DocumentUrl.IdentifierOrder().getOrderSpec()));
        counterParams1.set("outputClass", DocumentUrl.class.getCanonicalName());
        counterStage.add(new Step(TypeFileReader.class, counterParams1));

        Parameters counterParams2 = Parameters.instance();
        counterParams2.set("inputClass", DocumentUrl.class.getName());
        counterStage.add(new Step(ObjectCounter.class, counterParams2));

        counterStage.add(new OutputStep("docCount"));

        job.add(counterStage);
        job.connect("splitUrls", "counter", ConnectionAssignmentType.Each);

        // ensure we have a connection to the writer
        writer.addInput("docCount", new TupleflowLong.ValueOrder());
        job.connect("counter", "writer", ConnectionAssignmentType.Combined);
      }

      // run job
      success = runTupleFlowInstance(job, new File(outputFolder, "pagerank.init.tmp"), p, outputStream);

      if (success) {
        itrZeroFoldrTmp.renameTo(itrZeroFoldr);
      }
    }

    // now init has been run
    if (success) {
      // read docCount
      long docCount = 0;
      for (File f : docCountFolder.listFiles()) {
        String str = Utility.readFileToString(f).trim();
        docCount += Long.parseLong(str);
      }

      return docCount;
    }

    return 0;
  }

  private void finalize(Parameters p, int convergenceItr, PrintStream output) throws IOException, IncompatibleProcessorException, Exception {

    // open reader over final pageranks
    File outputFolder = new File(p.getString("outputFolder"));
    File finalPageranks = new File(outputFolder, "pageranks." + convergenceItr);

    File docNameOutput = new File(p.getString("outputFolder"), "pagerank.docNameOrder");
    File scoreOutput = new File(p.getString("outputFolder"), "pagerank.scoreOrder");

    // final job copies the final pagerank scores to a single output file
    Job job = new Job();

    Stage stage = new Stage("final");

    // collect files
    Parameters inParams = Parameters.instance();
    inParams.set("input", finalPageranks.getAbsolutePath());
    stage.add(new Step(FileSource.class, inParams));

    // open reader
    Parameters readerParams = Parameters.instance();
    readerParams.set("order", Utility.join(new PageRankScore.DocNameOrder().getOrderSpec()));
    readerParams.set("outputClass", PageRankScore.class.getCanonicalName());
    stage.add(new Step(TypeFileReader.class, readerParams));


    // split into two streams
    MultiStep fork = new MultiStep();
    stage.add(fork);

    // fork 1 - write document-name ordered output
    fork.addGroup("docName");
    Parameters writerParams1 = Parameters.instance();
    writerParams1.set("output", docNameOutput.getAbsolutePath());
    fork.addToGroup("docName", new Step(FinalPageRankScoreWriter.class, writerParams1));

    // fork 2 - write score ordered output
    fork.addGroup("scores");
    fork.addToGroup("scores", Utility.getSorter(new PageRankScore.DescScoreOrder()));
    Parameters writerParams2 = Parameters.instance();
    writerParams2.set("output", scoreOutput.getAbsolutePath());
    fork.addToGroup("scores", new Step(FinalPageRankScoreWriter.class, writerParams2));

    job.add(stage);

    // run job
    boolean success = runTupleFlowInstance(job, new File(outputFolder, "pagerank.final.tmp"), p, output);

    if (success) {
      logger.info("...done writing output. Deleting intermediate data...");

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
    } else {
      logger.info("...final write stage failed, intermediate data NOT deleted.");
    }
  }

  private boolean checkConvergence(int i, Parameters p) {
    File conv = new File(p.getString("outputFolder"), "pagerank.not.converged." + i);
    if (conv.exists()) {
      return false;
    } else {
      return true;
    }
  }

  private Job getIterationJob(Parameters p, File itrInput, File itrOutput, int i) {
    Job job = new Job();


    File linkData = new File(p.getString("linkdata"));
    File srcLinksFolder = new File(linkData, "srcNameOrder");

    File outputFolder = new File(p.getString("outputFolder"));
    // File itrInput = new File(outputFolder, "pageranks." + (i - 1));
    // File itrOutput = new File(outputFolder, "pageranks." + i);
    File convgFile = new File(outputFolder, "pagerank.not.converged." + i);
    if(convgFile.exists()){
      convgFile.delete();
    }
    
    itrOutput.mkdir();

    // stage 1: list folders:  pageranks, and linkdata
    job.add(getSplitStage("splitScores", "scoreFiles", itrInput));
    job.add(getSplitStage("splitLinks", "linkFiles", srcLinksFolder));

    // stage 2: read files : emit pageranks and links to nodes
    job.add(getReaderStage("scoreReader", "scoreFiles", "inputScores", PageRankScore.class.getCanonicalName(), new PageRankScore.DocNameOrder(), CompressionType.GZIP));
    job.add(getReaderStage("linkReader", "linkFiles", "inputLinks", ExtractedLink.class.getCanonicalName(), new ExtractedLink.SrcNameOrder(), CompressionType.GZIP));

    // stage 3: process links (emit 1-lambda * score / linkcount) as partial scores
    //          alternatively emit 1-lambda as a random jump
    job.add(getRandomWalkStage(p));
    // job.add(getWalkToJumpStage(p));

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
    job.connect("rndWalk", "rndJump", ConnectionAssignmentType.Each); // do not want this replicated

    job.connect("scoreReader", "reducer", ConnectionAssignmentType.Each);
    job.connect("rndWalk", "reducer", ConnectionAssignmentType.Each);
    job.connect("rndJump", "reducer", ConnectionAssignmentType.Combined); // replicate this now.

    job.connect("scoreReader", "convergence", ConnectionAssignmentType.Each);
    job.connect("reducer", "convergence", ConnectionAssignmentType.Each);

    return job;
  }

  private Stage getSplitStage(String name, String outputName, File folder) {
    // list directories, and generate several files.
    // two output streams (pagerank files, and 

    Stage stage = new Stage(name);
    stage.addOutput(outputName, new FileName.FilenameOrder());

    Parameters inParams = Parameters.instance();
    inParams.set("input", folder.getAbsolutePath());
    stage.add(new Step(FileSource.class, inParams));
    stage.add(Utility.getSorter(new FileName.FilenameOrder()));
    stage.add(new OutputStep(outputName));

    return stage;
  }

  private Stage getReaderStage(String name, String input, String output, String outputClass, Order order, CompressionType c) {
    Stage stage = new Stage(name);

    stage.addInput(input, new FileName.FilenameOrder());
    stage.addOutput(output, order, c);

    stage.add(new InputStep(input));

    Parameters p = Parameters.instance();
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
    stage.addOutput("outputPartialScores", new PageRankScore.DocNameOrder(), CompressionType.GZIP);
    stage.addOutput("outputExtraJumps", new PageRankJumpScore.ScoreOrder(), CompressionType.GZIP);

    stage.add(new InputStep("inputScores"));
    Parameters rndWalk = Parameters.instance();
    rndWalk.set("linkStream", "inputLinks");
    rndWalk.set("jumpStream", "outputExtraJumps");
    rndWalk.set("lambda", p.getDouble("lambda"));
    rndWalk.set("docCount", p.getLong("docCount")); // extraJump needs to be divided evenly across all documents
    stage.add(new Step(ComputeRandomWalk.class, rndWalk));
    stage.add(Utility.getSorter(new PageRankScore.DocNameOrder(), CompressionType.GZIP));
    stage.add(new OutputStep("outputPartialScores"));

    return stage;
  }

  private Stage getRandomJumpStage(Parameters p) {
    Stage stage = new Stage("rndJump");

    stage.addInput("inputScores", new PageRankScore.DocNameOrder());
    stage.addInput("outputExtraJumps", new PageRankJumpScore.ScoreOrder());
    stage.addOutput("outputCumulativeJump", new PageRankJumpScore.ScoreOrder(), CompressionType.GZIP);

    stage.add(new InputStep("inputScores"));
    // sum the scores, divide by count of pages, 
    // then emit single value that is the value of all random jumps to any given page.
    Parameters rndJumpParams = Parameters.instance();
    rndJumpParams.set("lambda", p.getDouble("lambda"));
    rndJumpParams.set("docCount", p.getLong("docCount"));
    rndJumpParams.set("extraJumpStream", "outputExtraJumps");
    stage.add(new Step(ComputeRandomJump.class, rndJumpParams));

    // should only emit one item, but still...
    stage.add(Utility.getSorter(new PageRankJumpScore.ScoreOrder(), CompressionType.GZIP));
    stage.add(new OutputStep("outputCumulativeJump"));

    return stage;
  }

  private Stage getProcessPartialStage(Parameters p, File outputFilePrefix) {
    Stage stage = new Stage("reducer");

    stage.addInput("inputScores", new PageRankScore.DocNameOrder());
    stage.addInput("outputPartialScores", new PageRankScore.DocNameOrder());
    stage.addInput("outputCumulativeJump", new PageRankJumpScore.ScoreOrder());
    stage.addOutput("outputScores", new PageRankScore.DocNameOrder(), CompressionType.GZIP);

    stage.add(new InputStep("inputScores"));

    Parameters combinerParams = Parameters.instance();
    combinerParams.set("jumpStream1", "outputCumulativeJump");
    combinerParams.set("scoreStream", "outputPartialScores");
    stage.add(new Step(PageRankScoreCombiner.class, combinerParams));
    stage.add(Utility.getSorter(new PageRankScore.DocNameOrder(), CompressionType.GZIP));

    MultiStep processingFork = new MultiStep();
    processingFork.addGroup("writer");
    processingFork.addGroup("stream");

    Parameters writerParams = Parameters.instance();
    writerParams.set("outputFile", outputFilePrefix.getAbsolutePath()); // + i
    writerParams.set("class", PageRankScore.class.getCanonicalName());
    writerParams.set("order", Utility.join(new PageRankScore.DocNameOrder().getOrderSpec()));
    writerParams.set("compression", "GZIP");

    processingFork.addToGroup("writer", new Step(TypeFileWriter.class, writerParams));

    processingFork.addToGroup("stream", new OutputStep("outputScores"));

    stage.add(processingFork);

    return stage;

  }

  private Stage getConvergenceStage(Parameters p, File conv) {
    Stage stage = new Stage("convergence");

    stage.addInput("inputScores", new PageRankScore.DocNameOrder());
    stage.addInput("outputScores", new PageRankScore.DocNameOrder());

    Parameters convgParams = Parameters.instance();
    convgParams.set("convFile", conv.getAbsolutePath());
    convgParams.set("prevScoreStream", "inputScores");
    convgParams.set("currScoreStream", "outputScores");
    convgParams.set("delta", p.get("delta", 0.000001));
    stage.add(new Step(ConvergenceTester.class, convgParams));
    // if converged, a file is created in the output directory.

    return stage;
  }

  private boolean runTupleFlowInstance(Job job, File jobFolder, Parameters p, PrintStream output) throws Exception {

    if (!jobFolder.exists()) {
      jobFolder.mkdirs();
    }

    Parameters runParams = Parameters.instance();
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

    runParams.set("galagoJobDir", jobFolder.getAbsolutePath());
    runParams.set("deleteJobDir", p.get("deleteJobDir", true));

    int hash = (int) runParams.get("distrib", 0);
    if (hash > 0) {
      job.properties.put("hashCount", Integer.toString(hash));
    }

    ErrorStore store = new ErrorStore();
    JobExecutor.runLocally(job, store, runParams);
    if (store.hasStatements()) {
      output.println(store.toString());
      return false;
    }
    return true;

  }
}
