/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.links.pagerank;

import org.lemurproject.galago.core.types.PageRankJumpScore;
import org.lemurproject.galago.core.types.PageRankScore;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.execution.Verified;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.PageRankScore", order = {"+docName"})
@OutputClass(className = "org.lemurproject.galago.core.types.PageRankScore")
public class PageRankScoreCombiner extends StandardStep<PageRankScore, PageRankScore> {

  private static final Logger logger = Logger.getLogger("PageRankScoreCombiner");
  double rndJump;
  TypeReader<PageRankScore> partialScores;
  PageRankScore curr;
  double totalScore = 0.0;
//  String instance;
  double totalWalk = 0.0;
  double pageCount = 0.0;
  
  public PageRankScoreCombiner(TupleFlowParameters p) throws IOException {
//    instance = "combiner-" + p.getInstanceId();

    String stream1 = p.getJSON().getString("jumpStream1");

    TypeReader<PageRankJumpScore> reader1 = p.getTypeReader(stream1);

    rndJump = 0.0;
    PageRankJumpScore js;
    while ((js = reader1.read()) != null) {
      rndJump += js.score;
//      System.err.println(instance + " part-instance rnd-jump :" + js.score + " sum :" + rndJump);
    }

//    System.err.println(instance + " instance rnd-jump :" + rndJump);

    String scoreStream = p.getJSON().getString("scoreStream");
    partialScores = p.getTypeReader(scoreStream);
    curr = partialScores.read();
  }

  @Override
  public void process(PageRankScore docScore) throws IOException {

    PageRankScore newDocScore = new PageRankScore(docScore.docName, rndJump);

    // This should never happen -- but I want to be sure.
    while (curr != null && Utility.compare(docScore.docName, curr.docName) > 0) {
      logger.log(Level.INFO, "Processing : {0}, IGNORED PARTIAL SCORE!!: {1}-{2}", new Object[]{docScore.docName, curr.docName, curr.score});
      curr = partialScores.read();
    }

    while (curr != null && Utility.compare(docScore.docName, curr.docName) == 0) {
      
      totalWalk += curr.score;
      
      newDocScore.score += curr.score;
      curr = partialScores.read();
    }
    // now curr points to the next document.
    pageCount += 1.0;
    totalScore += newDocScore.score;

    processor.process(newDocScore);
  }

  @Override
  public void close() throws IOException {

//    System.err.println(instance + " Total WALK Mass = " + totalWalk);
//    System.err.println(instance + " pages = " + pageCount);
//    System.err.println(instance + " COMBINED TOTAL MASS = " + totalScore);

    while (curr != null) {
      logger.log(Level.INFO, "On-Close : IGNORED PARTIAL SCORE!!: {1}-{2}", new Object[]{curr.docName, curr.score});
      curr = partialScores.read();
    }

    processor.close();
  }
}
