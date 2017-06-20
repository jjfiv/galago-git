/*
 * BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.tools.apps;

import org.lemurproject.galago.contrib.index.disk.ForwardIndexWriter;
import org.lemurproject.galago.contrib.parse.IndexDocIdSplit;
import org.lemurproject.galago.contrib.parse.ForwardIndexGenerate;
import org.lemurproject.galago.contrib.parse.ForwardIndexReduce;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.core.types.IndexSplit;
import org.lemurproject.galago.core.types.DocumentTermInfo;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.TupleflowAppUtil;
import org.lemurproject.galago.tupleflow.execution.StepInformation;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.utility.VersionInfo;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;


/**
 * Opens a specified, pre-existing index and creates a forward index from 
 * the document terms.
 *
 * The documents in the index are split amongst distributed processes that
 * open the document ID (within split process range), tokenize the document
 * terms and produces a KeyValuePair consisting of document ID and a DocTermsInfo 
 * object containing max frequency in document, frequency in collection,
 * and term and term positions and offsets in each document.
 *
 * @author smh
 */

public class BuildForwardIndexPart extends AppFunction {

  static PrintStream output;

  public Stage getFwindexJobStage (String jobName, Parameters p) {

    Parameters splitParams = Parameters.create ();
    splitParams.set ("indexPath", p.getString ("indexPath"));

    Stage stage = new Stage (jobName);
    stage.add (new StepInformation (IndexDocIdSplit.class, splitParams));
    stage.add (Utility.getSorter (new IndexSplit.IndexBegIdOrder ()));
 
    return stage;

  }  //- end method getFwindexJobStage


  public Job getFwindexJob (Parameters buildParameters) throws IOException, ClassNotFoundException {

    //- Add galago version, version build datetime and index build datetime
    //  to the build Parameters.
    VersionInfo.setGalagoVersionAndBuildDateTime ();
    String galagoVersion = VersionInfo.getGalagoVersion ();
    String fwIndexBuildDateTime = VersionInfo.getIndexBuildDateTime ();
    String galagoVersionBuildDateTime = VersionInfo.getGalagoVersionBuildDateTime ();
    buildParameters.set ("galagoVersion", galagoVersion);
    buildParameters.set ("fwIndexBuildDateTime", fwIndexBuildDateTime);
    buildParameters.set ("galagoVersionBuildDateTime", galagoVersionBuildDateTime);

    //- Collection wide stats are passed to the forward index writer in the tupleflow 
    //  parameters rather than a separate data stream.
    buildParameters.set ("fwindexStatistics/docsInCollection", 0L);
    buildParameters.set ("fwindexStatistics/totalTermsInCollection", 0L);
    buildParameters.set ("fwindexStatistics/maxTermFreqInCollection", 0L);

    String indexPath = new File (buildParameters.getString("indexPath")).getAbsolutePath ();
    buildParameters.set ("indexPath", indexPath);
    assert (new File (indexPath).isDirectory ());

    buildParameters.set ("filename", indexPath + "/fwindex");

    //- Define and sort in doc ID order index splits.
    Stage stage = getFwindexJobStage ("forwardIndexer", buildParameters);

    //- Generate sorted DocumentTermInfo tuples.
    stage.add (new StepInformation (ForwardIndexGenerate.class, buildParameters));
    stage.add (Utility.getSorter (new DocumentTermInfo.DocidTermBegPosOrder ()));

    //- Create DocTermsInfo objects from reduced DocumentTermInfo tuples.  Convert to
    //  KeyValuePairs (key:doc ID  value:serialized DocTermInfo object) for output.
    stage.add (new StepInformation (ForwardIndexReduce.class, buildParameters));

    //- Write the KeyValuePairs to disk.
    stage.add (new StepInformation (ForwardIndexWriter.class, buildParameters));

    Job job = new Job ();
    job.add (stage);

    return job;

  }  //- end method getFwindexJob


  @Override
  public String getName () {
    return "build-fwindex";
  }


  @Override
  public String getHelpString () {
    return "galago build-fwindex [flags] --indexPath=<index> \n"
            + "  Builds a Galago Forward Index Part file using TupleFlow. \n"
            + "  A forward index lists all the terms in a specified document ID key, \n"
            + "  along with term positions and offsets.  The index part produced \n"
            + "  is named fwindex. \n\n"

            + "<index>   The directory path to the index from which to build \n"
            + "          the index part.\n\n"

            + TupleflowAppUtil.getTupleFlowParameterString();
  }


  @Override
  public void run (Parameters p, PrintStream output) throws Exception {

    if (!p.containsKey ("indexPath")) {
      output.println (getHelpString ());
      return;
    }

    Job job = null;
    BuildForwardIndexPart build = new BuildForwardIndexPart ();
    job = build.getFwindexJob (p);
    TupleflowAppUtil.runTupleFlowJob (job, p, output);

  }  //- end method run

}  //- end class BuildForwardIndexPart
