/*
 * BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import org.lemurproject.galago.core.index.disk.DocumentIndicatorWriter;
import org.lemurproject.galago.core.index.disk.DocumentPriorWriter;
import java.util.List;
import org.lemurproject.galago.core.parse.FileLineParser;
import org.lemurproject.galago.core.parse.IndicatorExtractor;
import org.lemurproject.galago.core.parse.NumberKeyValuePairs;
import org.lemurproject.galago.core.parse.LineSplitter;
import org.lemurproject.galago.core.parse.PriorExtractor;
import org.lemurproject.galago.core.tools.App.AppFunction;
import org.lemurproject.galago.core.types.DocumentFeature;
import org.lemurproject.galago.core.types.DocumentIndicator;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.Step;

/**
 * Reads a file in a standard format
 * Writes an indicator index from the data in the file
 *  
 * Format : Document Identifier \t [true | false] \n
 * 
 * Document Identifiers not in index 
 *  will not be ignored.
 * 
 * @author sjh
 */
public class BuildSpecialPart extends AppFunction {

  static PrintStream output;

  public Stage getSpecialJobStage(String jobName, Parameters p){

    Parameters parserParams = new Parameters();
    parserParams.set("inputPath", new ArrayList());
    for(String filePath : (List<String>) p.getAsList("inputPath")){
      parserParams.getList("inputPath").add( new File(filePath).getAbsolutePath() );
    }

    Parameters splitterParams = new Parameters();
    splitterParams.set("split", p.get("split", "\t"));
    
    Parameters indexParams = new Parameters();
    indexParams.set("indexPath", p.getString("indexPath"));
    
    Stage stage = new Stage(jobName);
    stage.add(new Step(FileLineParser.class, parserParams));
    stage.add(new Step(LineSplitter.class, splitterParams));
    stage.add(Utility.getSorter(new KeyValuePair.KeyOrder()));
    stage.add(new Step(NumberKeyValuePairs.class, indexParams));

    return stage;
  }
  
  public Job getIndicatorJob(Parameters p) throws IOException, ClassNotFoundException {
    String indexPath = new File(p.getString("indexPath")).getAbsolutePath(); // fail if no path.
    p.set("indexPath", indexPath);
    assert (new File(indexPath).isDirectory());
    
    // get all defaulty steps.
    Stage stage = getSpecialJobStage("indicatorIndexer", p);
    
    // add final steps
    stage.add(new Step(IndicatorExtractor.class, p));
    stage.add(Utility.getSorter(new DocumentIndicator.DocumentOrder()));
    
    Parameters writerParams = new Parameters();
    writerParams.set("filename", indexPath + File.separator + p.getString("partName"));
    writerParams.set("default", p.get("default", false));
    stage.add(new Step(DocumentIndicatorWriter.class, writerParams));

    Job job = new Job();
    job.add(stage);

    return job;
  }

  public Job getPriorJob(Parameters p) throws ClassNotFoundException {
    String indexPath = new File(p.getString("indexPath")).getAbsolutePath(); // fail if no path.
    p.set("indexPath", indexPath);
    assert (new File(indexPath).isDirectory());

    // get all defaulty steps.
    Stage stage = getSpecialJobStage("priorIndexer", p);
    
    stage.add(new Step(PriorExtractor.class, p));
    stage.add(Utility.getSorter(new DocumentFeature.DocumentOrder()));
    
    Parameters writerParams = new Parameters();
    writerParams.set("filename", indexPath + File.separator + p.getString("partName"));
    stage.add(new Step(DocumentPriorWriter.class, writerParams));

    Job job = new Job();
    job.add(stage);

    return job;
  }

  @Override
  public String getHelpString() {
    return "galago build-special [flags] --indexPath=<index> (--inputPath=<input>)+\n\n"
            + "  Builds a Galago Structured Index Part file with TupleFlow,\n"
            + "  Can build either an indicator part or prior part.\n\n"
            + "<indicator-input>:  One or more indicator files in format:\n"
            + "           < document-identifier <split> [true | false] >\n\n"
            + "<prior-input>:  One or more indicator files in format:\n"
            + "           < document-identifier <split> [(log)-probability] >\n\n"
            + "<index>:  The directory path of the index to add to.\n\n"
            + "Algorithm Flags:\n"
            + "  --type={indicator|prior}: Sets the type of index part to build.\n"
            + "                            [default=prior]\n\n"
            + "  --partName={String}:      Sets the name of index part.\n"
            + "                 indicator: [default=prior]\n"
            + "                     prior: [default=indicator]\n"
            + "  --split={String}        : Sets the character that splits prior or indicator values\n"
            + "                            eg: <document-name>\\t<prior-value> \n"
            + "                            eg: <document-name>\\t<indicator-value> \n"
            + "                            [default = \"\\t\"]\n\n"
            + "  --default={true|false|float}: Sets the default value for the index part.\n"
            + "                 indicator: [default=false]\n"
            + "                     prior: [default=-inf\n\n"
            + "  --priorType={raw|prob|logprob}: Sets the type of prior to read. (Only for prior parts)\n"
            + "                            [default=raw]\n\n"
            + App.getTupleFlowParameterString();
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    if (!p.containsKey("indexPath") && !p.containsKey("inputPath")) {
      output.println(getHelpString());
      return;
    }

    Job job = null;
    BuildSpecialPart build = new BuildSpecialPart();
    String type = p.get("type", "prior");
    if (type.equals("indicator")) {
      job = build.getIndicatorJob(p);
    } else if (type.equals("prior")) {
      job = build.getPriorJob(p);
    }

    App.runTupleFlowJob(job, p, output);
  }
}
