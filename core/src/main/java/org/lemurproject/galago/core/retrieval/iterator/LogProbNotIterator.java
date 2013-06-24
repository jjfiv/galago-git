/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Performs a log-space probablistic not operation 
 * 
 * returns log(1 - exp(p))
 *
 * @author sjh
 */
public class LogProbNotIterator extends TransformIterator implements ScoreIterator {
  private final ScoreIterator scorer;
  private final NodeParameters np;

  public LogProbNotIterator(NodeParameters params, ScoreIterator scorer){
    super(scorer);
    this.scorer = scorer;
    this.np = params;
  }
  
  /**
   * Logically this node would actually identify all documents 
   * that are not a candidate of it's child scorer.
   *  - To avoid scoring all documents, we assert that this iterator
   *    scores all documents with non-background probabilities.
   */
  @Override
  public boolean hasAllCandidates(){
    return true;
  }
  
  @Override
  public boolean hasMatch(long identifier){
    return true;
  }
  
  @Override
  public double score() {
    double score = scorer.score();
    // check if the score is a log-space probability:
    if(score < 0){
      return Math.log(1 - Math.exp(score));
    }
    throw new RuntimeException("LogProbNot operator requires a log probability, for document: " + context.document + " iterator received: " + score);
  }

  @Override
  public double maximumScore() {
    if(scorer.maximumScore() < 0){
      return Math.log(1 - Math.exp(scorer.maximumScore()));
    }
    return 0;
  }

  @Override
  public double minimumScore() {
    if(scorer.minimumScore() < 0){
      return Math.log(1 - Math.exp(scorer.minimumScore()));
    }
    return Utility.tinyLogProbScore;
  }
  
  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "score";
    String className = this.getClass().getSimpleName();
    String parameters = np.toString();
    long document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = Double.toString(score());
    List<AnnotatedNode> children = Collections.singletonList(this.iterator.getAnnotatedNode());

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
