// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Note that this reject iterator had to change, since indicator iterators shouldn't hit on things that return false anymore; which provides actual boolean retrieval.
 *
 * This iterator considers all documents, but only has a "match" if the condition is false.
 *
 * @author irmarc
 */
public class RejectIterator extends TransformIterator implements ScoreIterator, CountIterator, ExtentIterator {
  private final BaseIterator dataIter;
  private final IndicatorIterator condIter;
  private final IteratorType dataKind;

  private RejectIterator(IndicatorIterator condIter, IteratorType kind, BaseIterator inner, LengthsIterator negativeHack) throws IOException {
    super(negativeHack);
    this.dataKind = kind;
    this.dataIter = inner;
    this.condIter = condIter;
    syncTo(0);
  }

  public RejectIterator(NodeParameters p, IndicatorIterator indicator, CountIterator counter, LengthsIterator lengths) throws IOException {
    this(indicator, IteratorType.COUNT, counter, lengths);
  }

  public RejectIterator(NodeParameters p, IndicatorIterator indicator, ScoreIterator scorer, LengthsIterator lengths) throws IOException {
    this(indicator, IteratorType.SCORE, scorer, lengths);
  }

  public RejectIterator(NodeParameters p, IndicatorIterator indicator, ExtentIterator extents, LengthsIterator lengths) throws IOException {
    this(indicator, IteratorType.EXTENT, extents, lengths);
  }

  protected boolean condition(ScoringContext c){
    return ! condIter.indicator(c);
  }

  @Override
  public void movePast(long document) throws IOException {
    iterator.movePast(document);
    dataIter.movePast(document);
    condIter.movePast(document);
  }
  @Override
  public void syncTo(long document) throws IOException {
    iterator.syncTo(document);
    dataIter.syncTo(document);
    condIter.syncTo(document);
  }

  @Override
  public boolean hasMatch(ScoringContext sc) {
    return condition(sc);
  }

  @Override
  public ExtentArray extents(ScoringContext c) {
    assert(dataKind == IteratorType.EXTENT);
    if(condition(c)) {
      return ((ExtentIterator) dataIter).extents(c);
    } else {
      return ExtentArray.EMPTY;
    }
  }

  @Override
  public int count(ScoringContext c) {
    assert(dataKind == IteratorType.EXTENT || dataKind == IteratorType.COUNT);
    if(condition(c)) {
      return ((CountIterator) dataIter).count(c);
    } else {
      return 0;
    }
  }

  @Override
  public ExtentArray data(ScoringContext c) {
    return extents(c);
  }

  @Override
  public boolean indicator(ScoringContext c) {
    assert(dataKind == IteratorType.INDICATOR || dataKind == IteratorType.COUNT);
    if(condition(c)) {
      return ((IndicatorIterator) dataIter).indicator(c);
    } else {
      return false;
    }
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext sc) throws IOException {
    String className = this.getClass().getSimpleName();
    String parameters = "";
    long document = currentCandidate();
    boolean atCandidate = hasMatch(sc);
    List<AnnotatedNode> children = new ArrayList<>();
    children.add(condIter.getAnnotatedNode(sc));
    children.add(dataIter.getAnnotatedNode(sc));

    String type = "unknown";
    String returnValue = "unknown";
    switch(dataKind) {
      case COUNT:
        type = "count";
        returnValue = Integer.toString(count(sc));
        break;
      case EXTENT:
        type = "extents";
        returnValue = extents(sc).toString();
        break;
      case SCORE:
        type = "score";
        returnValue = Double.toString(score(sc));
        break;
    }
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }

  @Override
  public double score(ScoringContext c) {
    assert(dataKind == IteratorType.SCORE);
    if(condition(c)) {
      return ((ScoreIterator) dataIter).score(c);
    }
    return Utility.tinyLogProbScore;
  }

  @Override
  public double maximumScore() {
    return ((ScoreIterator) dataIter).maximumScore();
  }

  @Override
  public double minimumScore() {
    return ((ScoreIterator) dataIter).minimumScore();
  }
}
