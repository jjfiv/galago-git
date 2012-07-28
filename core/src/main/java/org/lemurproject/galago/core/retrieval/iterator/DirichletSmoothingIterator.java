/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.index.LengthsReader.LengthsIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.scoring.ScoringFunction;

/**
 *
 * @author sjh
 */
@RequiredStatistics(statistics = {"collectionLength", "nodeFrequency", "collectionProbability"})
@RequiredParameters(parameters = {"mu"})
public class DirichletSmoothingIterator extends ConjunctionIterator implements MovableScoreIterator {

  private NodeParameters np;
  private double backgroundProbability;
  private double mu;
  private LengthsIterator lengthItr;
  private MovableCountIterator countItr;

  public DirichletSmoothingIterator(NodeParameters np, LengthsIterator lengths, MovableCountIterator counts) throws IOException {
    super(np, new MovableIterator[]{lengths, counts});
    this.np = np;
    this.mu = np.get("mu", 1500D);

    double cf = np.getLong("nodeFrequency");
    double cl = np.getLong("collectionLength");
    // ensure that the collection frequency is > 0 
    // (defaults to 0.5 - this should be a static somewhere). 
    cf = (cf == 0)? 0.5 : cf;
    this.backgroundProbability = cf / cl;

    this.backgroundProbability = np.getDouble("collectionProbability");
    
    this.lengthItr = lengths;
    this.countItr = counts;
  }

  @Override
  public double score() {
    double count = 0;
    if(countItr.currentCandidate() == this.context.document){
      count = countItr.count();
    }  
    
    double length = 0;
    if(lengthItr.currentCandidate() == this.context.document){
      length = lengthItr.getCurrentLength();
    }

    // Dirichlet smoothing function
    double numerator = count + (mu * backgroundProbability);
    double denominator = length + mu;
    return Math.log(numerator / denominator);
  }

  @Override
  public double maximumScore() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public double minimumScore() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getEntry() throws IOException {
    return this.currentCandidate() + " " + this.score();
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "score";
    String className = this.getClass().getSimpleName();
    String parameters = np.toString();
    int document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = Double.toString(score());
    List<AnnotatedNode> children = new ArrayList();
    children.add(this.lengthItr.getAnnotatedNode());
    children.add(this.countItr.getAnnotatedNode());

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
