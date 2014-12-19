/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.links.pagerank;

import java.io.IOException;
import org.lemurproject.galago.core.types.PageRankJumpScore;
import org.lemurproject.galago.core.types.PageRankScore;
import org.lemurproject.galago.utility.debug.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.TypeReader;
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
  private final long docCount;

//  String instance;
  double scoreSum = 0.0;
  Counter documents;
  double extraRndJump;

  public ComputeRandomJump(TupleFlowParameters p) throws IOException {
//    instance = "jumper-"+p.getInstanceId();
    lambda = p.getJSON().getDouble("lambda");
    documents = p.getCounter("Documents");
    docCount = p.getJSON().getLong("docCount");

    String stream = p.getJSON().getString("extraJumpStream");
    TypeReader<PageRankJumpScore> reader1 = p.getTypeReader(stream);
    //TypeReader<PageRankJumpScore> reader2 = p.getTypeReader(stream2);

    extraRndJump = 0.0;
    PageRankJumpScore js;
    while ((js = reader1.read()) != null) {
      extraRndJump += js.score;
    }
  }

  @Override
  public void process(PageRankScore score) throws IOException {
    scoreSum += score.score;
    documents.increment();
  }

  @Override
  public void close() throws IOException {

//    System.err.println(instance+" TOTAL PAGERANK SCORE: " + scoreSum);
//    System.err.println(instance+" TOTAL PAGERANK JUMP-1: " + (scoreSum * lambda) + " docs = " + docCount );
//    System.err.println(instance+" instance PAGERANK JUMP: " + (scoreSum * lambda) / docCount);
//    System.err.println("instance extra PAGERANK JUMP: " + extraRndJump );

    scoreSum = scoreSum * lambda;
    double jump = scoreSum / docCount;

    // extraRndJump has already been divided by docCount
    processor.process(new PageRankJumpScore(jump + extraRndJump));
    processor.close();
  }
}
