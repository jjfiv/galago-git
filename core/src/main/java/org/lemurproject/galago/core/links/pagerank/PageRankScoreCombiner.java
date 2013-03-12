/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.links.pagerank;

import java.io.IOException;
import java.util.logging.Level;
import org.lemurproject.galago.core.types.PageRankJumpScore;
import org.lemurproject.galago.core.types.PageRankScore;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.TypeReader;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.PageRankScore", order = {"+docName"})
@OutputClass(className = "org.lemurproject.galago.core.types.PageRankScore")
public class PageRankScoreCombiner extends StandardStep<PageRankScore, PageRankScore> {

  private static final Logger logger = LoggerFactory.getLogger("PageRankScoreCombiner");
  double rndJump;
  TypeReader<PageRankScore> partialScores;
  PageRankScore curr;

  public PageRankScoreCombiner(TupleFlowParameters p) throws IOException {
    String stream1 = p.getJSON().getString("jumpStream1");
    String stream2 = p.getJSON().getString("jumpStream2");

    TypeReader<PageRankJumpScore> reader1 = p.getTypeReader(stream1);
    TypeReader<PageRankJumpScore> reader2 = p.getTypeReader(stream2);

    rndJump = 0.0;
    PageRankJumpScore js;
    while ((js = reader1.read()) != null) {
      rndJump += js.score;
    }
    while ((js = reader2.read()) != null) {
      rndJump += js.score;
    }

    String scoreStream = p.getJSON().getString("scoreStream");
    partialScores = p.getTypeReader(scoreStream);
    curr = partialScores.read();
  }

  @Override
  public void process(PageRankScore docScore) throws IOException {

    PageRankScore newDocScore = new PageRankScore(docScore.docName, rndJump);

    // This should never happen -- but I want to be sure.
    while (curr != null && Utility.compare(docScore.docName, curr.docName) > 0) {
      logger.info("Processing : {0}, IGNORED PARTIAL SCORE!!: {1}-{2}", new Object[]{docScore.docName, curr.docName, curr.score});
      curr = partialScores.read();
    }

    while (curr != null && Utility.compare(docScore.docName, curr.docName) == 0) {
      newDocScore.score += curr.score;
      curr = partialScores.read();
    }
    // now curr points to the next document.

    processor.process(newDocScore);
  }

  @Override
  public void close() throws IOException {
    while (curr != null) {
      logger.info("On-Close : IGNORED PARTIAL SCORE!!: {1}-{2}", new Object[]{curr.docName, curr.score});
      curr = partialScores.read();
    }

    processor.close();
  }
}
