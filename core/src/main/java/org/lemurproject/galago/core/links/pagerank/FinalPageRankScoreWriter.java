/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.links.pagerank;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.lemurproject.galago.core.types.PageRankScore;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.PageRankScore")
public class FinalPageRankScoreWriter implements Processor<PageRankScore> {

  private BufferedWriter writer;

  public FinalPageRankScoreWriter(TupleFlowParameters params) throws IOException {
    File outputFile = new File(params.getJSON().getString("output"));
    writer = new BufferedWriter(new FileWriter(outputFile));
  }

  @Override
  public void process(PageRankScore prs) throws IOException {
    writer.write(prs.docName + " " + prs.score + "\n");
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
