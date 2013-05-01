/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.links.pagerank;

import java.io.File;
import java.io.IOException;
import org.lemurproject.galago.core.types.PageRankScore;
import org.lemurproject.galago.tupleflow.ExNihiloSource;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Step;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.TypeReader;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verification;

/**
 *
 * @author sjh
 */
public class ConvergenceTester implements ExNihiloSource<PageRankScore> {

  private final File convFile;
  private final String prevScoreStream;
  private final String currScoreStream;
  private final TupleFlowParameters tp;
  private final double delta;

  public ConvergenceTester(TupleFlowParameters p) {
    convFile = new File(p.getJSON().getString("convFile") + "/inst." + p.getInstanceId());
    prevScoreStream = p.getJSON().getString("prevScoreStream");
    currScoreStream = p.getJSON().getString("currScoreStream");
    delta = p.getJSON().getDouble("delta");
    tp = p;
  }

  @Override
  public void run() throws IOException {
    TypeReader<PageRankScore> prevReader = tp.getTypeReader(prevScoreStream);
    TypeReader<PageRankScore> currReader = tp.getTypeReader(currScoreStream);

    PageRankScore prev = prevReader.read();
    PageRankScore curr = currReader.read();

    boolean converged = true;

    while (prev != null || curr != null) {
      // check difference
      if (prev.docName.equals(curr.docName)) {
        if (Math.abs(prev.score - curr.score) > delta) {
          converged = false;
          break;
        }
        prev = prevReader.read();
        curr = currReader.read();

      } else {
        // MAJOR PROBLEM -- misaligned document lists... we dropped one.
        System.err.println("DOCUMENT MISSING...: " + prev.docName + " - " + curr.docName + "\nAttempting to recover.");
        if (Utility.compare(prev.docName, curr.docName) < 0) {
          prev = prevReader.read();
        } else {
          curr = currReader.read();
        }
      }
    }

    if (!converged) {
      convFile.createNewFile();
    }
  }

  @Override
  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    // Do nothing - we never call process.
  }

  public static void verify(TupleFlowParameters fullParameters, ErrorHandler handler) {
    Parameters parameters = fullParameters.getJSON();

    String[] requiredParameters = {"convFile", "prevScoreStream", "currScoreStream", "delta"};

    if (!Verification.requireParameters(requiredParameters, parameters, handler)) {
      return;
    }

    Verification.verifyTypeReader(parameters.getString("prevScoreStream"), PageRankScore.class, new String[]{"+docName"}, fullParameters, handler);
    Verification.verifyTypeReader(parameters.getString("currScoreStream"), PageRankScore.class, new String[]{"+docName"}, fullParameters, handler);

  }
}
