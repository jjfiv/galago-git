/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.links.pagerank;

import java.io.IOException;
import org.lemurproject.galago.core.types.PageRankJumpScore;
import org.lemurproject.galago.core.types.PageRankScore;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.PageRankScore", order = {"+docName"})
@OutputClass(className = "org.lemurproject.galago.core.types.PageRankJumpScore")
public class ComputeRandomJump extends StandardStep<PageRankScore, PageRankJumpScore> {

  private final double lambda;
  double scoreSum = 0.0;
  long pageCount = 0;
  Counter documents;

  public ComputeRandomJump(TupleFlowParameters p) {
    lambda = p.getJSON().getDouble("lambda");
    documents = p.getCounter("Documents");
  }

  @Override
  public void process(PageRankScore score) throws IOException {
    scoreSum += score.score;
    pageCount += 1;
    if (documents != null) {
      documents.increment();
    }
  }

  @Override
  public void close() throws IOException {

//    System.err.println("TOTAL PAGERANK SCORE: " + scoreSum);
//    System.err.println("TOTAL PAGERANK JUMP-1: " + (scoreSum * lambda));

    scoreSum = scoreSum * lambda;

    double jump = 0.0;
    if (pageCount > 0) {
      jump = scoreSum / pageCount;
    }
    jump = jump;

    processor.process(new PageRankJumpScore(jump));
    processor.close();
  }
}
