/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.links.pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import org.lemurproject.galago.core.links.LinkDestNamer;
import org.lemurproject.galago.core.types.ExtractedLink;
import org.lemurproject.galago.core.types.PageRankJumpScore;
import org.lemurproject.galago.core.types.PageRankScore;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.Processor;
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
public class ComputeRandomWalk extends StandardStep<PageRankScore, PageRankScore> {

  private static final Logger logger = LoggerFactory.getLogger("ComputeRandomWalk");
  private final TypeReader<ExtractedLink> linkReader;
  private final Processor<PageRankJumpScore> jumpWriter;
  private final double lambda;
  private ExtractedLink currentLink;
  double rndJumpScore = 0.0;
  long pageCount = 0;
  double totalWalk = 0.0;
  Counter documents;

  public ComputeRandomWalk(TupleFlowParameters p) throws IOException {

    // open streams:
    String linkStream = p.getJSON().getString("linkStream");
    linkReader = p.getTypeReader(linkStream);

    String jumpStream = p.getJSON().getString("jumpStream");
    jumpWriter = p.getTypeWriter(jumpStream);

    currentLink = linkReader.read();

    lambda = p.getJSON().getDouble("lambda");

    documents = p.getCounter("Documents");
  }

  @Override
  public void process(PageRankScore docScore) throws IOException {
    if (documents != null) {
      documents.increment();
    }

    pageCount += 1;

    List<String> linkedDocuments = new ArrayList();

    while (currentLink != null && Utility.compare(docScore.docName, currentLink.srcName) > 0.0) {
      // This shouldn't happen....
      logger.info("Processing : {0}, IGNORED LINK: {1}-{2}", new Object[]{docScore.docName, currentLink.srcName, currentLink.destName});
      currentLink = linkReader.read();
    }

    // collect all out-going links
    while (currentLink != null && Utility.compare(docScore.docName, currentLink.srcName) == 0.0) {
      // docuemnts are NOT allowed to link to themselves.
      if (!docScore.docName.equals(currentLink.destName)) {
        linkedDocuments.add(currentLink.destName);
      }
      currentLink = linkReader.read();
    }

    // if there are no links - emit a random jump of size score * (1-lambda)
    if (linkedDocuments.isEmpty()) {
      rndJumpScore += (1.0 - lambda) * docScore.score;

    } else {

      totalWalk += (1.0 - lambda) * docScore.score;

      double rndWalkScore = (1.0 - lambda) * docScore.score / (double) linkedDocuments.size();
      for (String destination : linkedDocuments) {
        // if the destination is external -- add score mass to rndJump
        if (destination.startsWith(LinkDestNamer.EXTERNAL_PREFIX)) {
          rndJumpScore += rndWalkScore;
        }

        processor.process(new PageRankScore(destination, rndWalkScore));
      }
    }
  }

  @Override
  public void close() throws IOException {

//    System.err.println("TOTAL PAGERANK JUMP-2: " + (rndJumpScore));
//    System.err.println("TOTAL PAGERANK WALK: " + totalWalk);

    jumpWriter.process(new PageRankJumpScore(rndJumpScore / pageCount));
    jumpWriter.close();
    processor.close();
  }
}
