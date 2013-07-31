// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools.apps;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import org.lemurproject.galago.core.index.ExtractIndexDocumentNumbers;
import org.lemurproject.galago.core.index.disk.CountIndexWriter;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.disk.WindowIndexWriter;
import org.lemurproject.galago.core.parse.DocumentSource;
import org.lemurproject.galago.core.parse.stem.KrovetzStemmer;
import org.lemurproject.galago.core.parse.stem.NullStemmer;
import org.lemurproject.galago.core.parse.stem.Porter2Stemmer;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.NumberWordCount;
import org.lemurproject.galago.core.types.NumberedExtent;
import org.lemurproject.galago.core.types.TextFeature;
import org.lemurproject.galago.core.window.ExtractLocations;
import org.lemurproject.galago.core.window.NumberWordCountThresholder;
import org.lemurproject.galago.core.window.NumberedExtentThresholder;
import org.lemurproject.galago.core.window.ReduceNumberWordCount;
import org.lemurproject.galago.core.window.TextFeatureThresholder;
import org.lemurproject.galago.core.window.WindowFeaturer;
import org.lemurproject.galago.core.window.WindowFilter;
import org.lemurproject.galago.core.window.WindowProducer;
import org.lemurproject.galago.core.window.WindowToNumberWordCount;
import org.lemurproject.galago.core.window.WindowToNumberedExtent;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Parameters.Type;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ConnectionAssignmentType;
import org.lemurproject.galago.tupleflow.execution.ConnectionPointType;
import org.lemurproject.galago.tupleflow.execution.InputStep;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.OutputStep;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.StageConnectionPoint;
import org.lemurproject.galago.tupleflow.execution.Step;

/**
 *
 * Time efficient algorithm for ngram indexing
 *  - uses more temporary space
 *  - estimate the space required as (n*|C|)
 *
 * Space efficient algorithm
 *  - uses a hash function to create a filter
 *  - filter allows the discard of many infrequent ngrams
 *  - space requirement is very close to the final index
 *
 * @author sjh
 */
public class BuildWindowIndex extends AppFunction {

  boolean spaceEfficient;
  String indexPath;
  boolean positionalIndex;
  boolean stemming;
  int n;
  int width;
  boolean ordered;
  int threshold;
  boolean threshdf;
  Parameters buildParameters;
  String stemmerName;
  Class stemmerClass;

  public Stage getParseFilterStage() throws Exception {
    // reads through the corpus
    Stage stage = new Stage("parseFilter");

    stage.add(new StageConnectionPoint(
            ConnectionPointType.Input,
            "splits", new DocumentSplit.FileIdOrder()));
    stage.add(new StageConnectionPoint(
            ConnectionPointType.Output,
            "featureData", new TextFeature.FeatureOrder()));

    stage.add(new InputStep("splits"));
    stage.add(BuildStageTemplates.getParserStep(buildParameters));
    stage.add(BuildStageTemplates.getTokenizerStep(buildParameters));

    if (stemming) {
      stage.add(BuildStageTemplates.getStemmerStep(new Parameters(), stemmerClass));
    }

    // Document numbers don't really matter - they are dropped by the Featurer.
    Parameters p = new Parameters();
    p.set("indexPath", indexPath);
    stage.add(new Step(ExtractIndexDocumentNumbers.class, p));

    Parameters p2 = new Parameters();
    p2.set("n", n);
    p2.set("width", width);
    p2.set("ordered", ordered);
    if (buildParameters.isString("fields") || buildParameters.isList("fields", Type.STRING)) {
      p2.set("fields", (List<String>) buildParameters.getAsList("fields"));
    }
    stage.add(new Step(WindowProducer.class, p2));

    stage.add(new Step(WindowFeaturer.class));
    stage.add(Utility.getSorter(new TextFeature.FeatureOrder()));
    stage.add(new OutputStep("featureData"));

    return stage;
  }

  public Stage getReduceFilterStage() {
    Stage stage = new Stage("reduceFilter");
    stage.add(new StageConnectionPoint(
            ConnectionPointType.Input,
            "featureData", new TextFeature.FeatureOrder()));
    stage.add(new StageConnectionPoint(
            ConnectionPointType.Output,
            "filterData", new TextFeature.FileFilePositionOrder()));

    stage.add(new InputStep("featureData"));

    Parameters p = new Parameters();
    p.set("threshold", threshold);
    stage.add(new Step(TextFeatureThresholder.class, p));

    stage.add(Utility.getSorter(new TextFeature.FileFilePositionOrder()));

    // discards feature data - leaving only locations (data = byte[0]).
    stage.add(new Step(ExtractLocations.class));

    stage.add(new OutputStep("filterData"));

    return stage;
  }

  public Stage getParsePostingsStage() throws Exception {
    // reads through the corpus
    Stage stage = new Stage("parsePostings");

    stage.add(new StageConnectionPoint(
            ConnectionPointType.Input,
            "splits", new DocumentSplit.FileIdOrder()));

    if (positionalIndex) {
      stage.add(new StageConnectionPoint(
              ConnectionPointType.Output,
              "windows", new NumberedExtent.ExtentNameNumberBeginOrder()));
    } else {
      stage.add(new StageConnectionPoint(
              ConnectionPointType.Output,
              "windows", new NumberWordCount.WordDocumentOrder()));

    }
    if (spaceEfficient) {
      stage.add(new StageConnectionPoint(
              ConnectionPointType.Input,
              "filterData", new TextFeature.FileFilePositionOrder()));
    }

    stage.add(new InputStep("splits"));
    stage.add(BuildStageTemplates.getParserStep(buildParameters));
    stage.add(BuildStageTemplates.getTokenizerStep(buildParameters));
    if (stemming) {
      Class stemmer = stemmerClass;
      stage.add(BuildStageTemplates.getStemmerStep(new Parameters(), stemmer));
    }

    Parameters p = new Parameters();
    p.set("indexPath", indexPath);
    stage.add(new Step(ExtractIndexDocumentNumbers.class, p));

    Parameters p2 = new Parameters();
    p2.set("n", n);
    p2.set("width", width);
    p2.set("ordered", ordered);
    if (buildParameters.isString("fields") || buildParameters.isList("fields", Type.STRING)) {
      p2.set("fields", (List<String>) buildParameters.getAsList("fields"));
    }
    stage.add(new Step(WindowProducer.class, p2));

    if (spaceEfficient) {
      Parameters p3 = new Parameters();
      p3.set("filterStream", "filterData");
      stage.add(new Step(WindowFilter.class, p3));
    }

    if (this.positionalIndex) {
      stage.add(new Step(WindowToNumberedExtent.class));
      stage.add(Utility.getSorter(new NumberedExtent.ExtentNameNumberBeginOrder()));
    } else {
      stage.add(new Step(WindowToNumberWordCount.class));
      stage.add(Utility.getSorter(new NumberWordCount.WordDocumentOrder()));
      stage.add(new Step(ReduceNumberWordCount.class));
    }

    stage.add(new OutputStep("windows"));
    return stage;
  }

  public Stage getWritePostingsStage(String stageName, String inputName, String indexName) {
    Stage stage = new Stage(stageName);

    if (positionalIndex) {
      stage.add(new StageConnectionPoint(
              ConnectionPointType.Input,
              inputName, new NumberedExtent.ExtentNameNumberBeginOrder()));
    } else {
      stage.add(new StageConnectionPoint(
              ConnectionPointType.Input,
              inputName, new NumberWordCount.WordDocumentOrder()));

    }

    stage.add(new InputStep(inputName));

    Parameters p = new Parameters();
    p.set("threshold", threshold);
    p.set("threshdf", threshdf);
    if (threshold > 1) {
      if (positionalIndex) {
        stage.add(new Step(NumberedExtentThresholder.class, p));
      } else {
        stage.add(new Step(ReduceNumberWordCount.class));
        stage.add(new Step(NumberWordCountThresholder.class, p));
      }
    }

    Parameters p2 = new Parameters();
    p2.set("filename", indexPath + File.separator + indexName);
    p2.set("n", this.n);
    p2.set("width", this.width);
    p2.set("ordered", this.ordered);
    p2.set("usedocfreq", this.threshdf);
    p2.set("threshold", this.threshold);
    if (stemming) {
      p2.set("stemming", stemming); // slightly redundent only present if true //
      p2.set("stemmer", stemmerClass.getName());
    }

    if (this.positionalIndex) {
      stage.add(new Step(WindowIndexWriter.class, p2));
    } else {
      stage.add(new Step(CountIndexWriter.class, p2));
    }
    return stage;
  }

  public Job getIndexJob(Parameters p) throws Exception {

    Job job = new Job();
    this.buildParameters = p;

    this.indexPath = new File(p.getString("indexPath")).getAbsolutePath(); // fail if no path.
    List<String> inputPaths = p.getAsList("inputPath");

    // application of defaulty values
    this.stemming = p.get("stemming", false);
    if (stemming) {
      if (p.isString("stemmer") && p.isString("stemmerClass")) {
        stemmerName = p.getString("stemmer");
        stemmerClass = Class.forName(p.getString("stemmerClass"));
      } else if (p.isString("stemmer")) {
        stemmerName = p.getString("stemmer");
        stemmerClass = null;
        if (stemmerName.equals("null")) {
          stemmerClass = NullStemmer.class;
        } else if (stemmerName.equals("porter")) {
          stemmerClass = Porter2Stemmer.class;
        } else if (stemmerName.equals("krovetz")) {
          stemmerClass = KrovetzStemmer.class;
        } else {
          throw new RuntimeException("A stemmerClass must be specified for stemmer " + stemmerName);
        }
      } else if (p.isString("stemmerClass")) {
        stemmerClass = Class.forName(p.getString("stemmerClass"));
        stemmerName = p.getString("stemmerClass").replaceFirst(".*\\.", "");
      } else {
        // defaults:
        stemmerName = "porter";
        stemmerClass = Porter2Stemmer.class;
      }
    }

    this.positionalIndex = p.get("positionalIndex", false);
    this.n = (int) p.get("n", 2);
    this.width = (int) p.get("width", 1);
    this.ordered = p.get("ordered", true);
    this.threshold = (int) p.get("threshold", 2);
    this.threshdf = p.get("usedocfreq", false);

    spaceEfficient = p.get("spaceEfficient", false);
    if (threshold <= 1) {
      // no point being space efficient.
      spaceEfficient = false;
    }

    // tokenizer - fields
    if (buildParameters.isList("fields", Type.STRING) || buildParameters.isString("fields")) {
      buildParameters.set("tokenizer", new Parameters());
      buildParameters.getMap("tokenizer").set("fields", buildParameters.getAsList("fields"));
    }

    // we intend to add to the index;
    // so verify that the index submitted is a valid index
    try {
      DiskIndex i = new DiskIndex(indexPath);
      i.close();
    } catch (Exception e) {
      throw new IOException("Index " + indexPath + "is not a valid index\n" + e.toString());
    }

    String indexName;
    if (p.isString("outputIndexName")) {
      indexName = p.getString("outputIndexName");
    } else {
      if (ordered) {
        indexName = "od.n" + n + ".w" + width + ".h" + threshold;
      } else {
        indexName = "uw.n" + n + ".w" + width + ".h" + threshold;
      }

      if (threshdf) {
        indexName += ".df";
      }

      if (stemming) {
        indexName += "." + stemmerName;
      }
    }



    Parameters splitParameters = new Parameters();
    splitParameters.set("corpusPieces", p.get("distrib", 10));
    job.add(BuildStageTemplates.getSplitStage(inputPaths, DocumentSource.class, new DocumentSplit.FileIdOrder(), splitParameters));
    job.add(getParsePostingsStage());
    job.add(getWritePostingsStage("writePostings", "windows", indexName));

    job.connect("inputSplit", "parsePostings", ConnectionAssignmentType.Each);
    job.connect("parsePostings", "writePostings", ConnectionAssignmentType.Combined);

    if (spaceEfficient) {
      job.add(getParseFilterStage());
      job.add(getReduceFilterStage());
      job.connect("inputSplit", "parseFilter", ConnectionAssignmentType.Each);
      job.connect("parseFilter", "reduceFilter", ConnectionAssignmentType.Each);
      job.connect("reduceFilter", "parsePostings", ConnectionAssignmentType.Each, new TextFeature.FileOrder().getOrderSpec(), (int) p.get("distrib", -1));
    }

    return job;
  }

  @Override
  public String getName(){
    return "build-window";
  }

  @Override
  public String getHelpString() {
    return "galago build-window [flags] --indexPath=<index> (--inputPath+<input>)+\n\n"
            + "  Builds a Galago StructuredIndex window index file using TupleFlow. Program\n"
            + "  uses one thread for each CPU core on your computer.  While some debugging output\n"
            + "  will be displayed on the screen, most of the status information will\n"
            + "  appear on a web page.  A URL should appear in the command output \n"
            + "  that will direct you to the status page.\n\n"
            + "  Arg: --spaceEfficient=true will produce an identical window index using "
            + "  a two-pass space efficient algorithm. \n\n"
            + "  Ordered or unordered windows can be generated. We match the #od and\n"
            + "  #uw operator definitions (See galago query language). Width of an ordered window\n"
            + "  is the maximum distance between words. Width of an unordered window is\n"
            + "  the differencebetween the location of the last word and the location of \n"
            + "  the first word.\n\n"
            + "  <input>:  Can be either a file or directory, and as many can be\n"
            + "          specified as you like.  Galago can read html, xml, txt, \n"
            + "          arc (Heritrix), trectext, trecweb and corpus files.\n"
            + "          Files may be gzip compressed (.gz).\n"
            + "  <index>:  The directory path of the existing index (over the same corpus).\n\n"
            + "Algorithm Flags:\n"
            + "  --n={int >= 2}:          Selects the number of terms in each window (any reasonable value is possible).\n"
            + "                           [default = 2]\n"
            + "  --width={int >= 1}:      Selects the width of the window (Note: ordered windows are different to unordered windows).\n"
            + "                           [default = 1]\n"
            + "  --ordered={true|false}:  Selects ordered or unordered windows.\n"
            + "                           [default = true]\n"
            + "  --threshold={int >= 1}:  Selects the minimum number length of any inverted list.\n"
            + "                           Larger values will produce smaller indexes.\n"
            + "                           [default = 2]\n"
            + "  --usedocfreq={true|false}: Determines if the threshold is applied to term freq or doc freq.\n"
            + "                           [default = false]\n"
            + "  --stemming={true|false}: Selects to stem terms with which to build a stemmed ngram inverted list.\n"
            + "                           [default=true]\n"
            + "  --fields+{field-name}:   Selects field parts to index.\n"
            + "                           [omitted]\n"
            + "  --spaceEfficient={true|false}: Selects whether to use a space efficient algorithm.\n"
            + "                           (The cost is an extra pass over the input data).\n"
            + "                           [default=false]\n"
            + "  --positionalIndex={true|false}: Selects whether to write positional data to the index file.\n"
            + "                           (The benefit is a large decrease in space usage).\n"
            + "                           [default=true]\n\n"
            + getTupleFlowParameterString();
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    // build-fast index input
    if (!p.containsKey("indexPath") && !p.containsKey("inputPath")) {
      output.println(getHelpString());
      return;
    }

    Job job;
    BuildWindowIndex build = new BuildWindowIndex();
    job = build.getIndexJob(p);

    runTupleFlowJob(job, p, output);
  }
}
