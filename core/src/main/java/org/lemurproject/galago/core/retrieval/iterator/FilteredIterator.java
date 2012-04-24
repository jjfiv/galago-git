// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * #filter ( AbstractIndicator ScoreIterator ) : Only scores documents that
 * where the AbstractIndicator is on
 *
 * 8/23/2011 (irmarc) - Changed this to an abstract class to implement most of
 * what's needed for a filter. RequireIterator (require) and RejectIterator
 * (reject) now finish the job up.
 *
 * @author sjh
 */
@RequiredParameters(parameters = {"shareNodes"})
public abstract class FilteredIterator implements MovableCountIterator, MovableScoreIterator, MovableExtentIterator, ContextualIterator {

  protected ScoringContext context;
  protected MovableIndicatorIterator indicator;
  protected MovableCountIterator counter;
  protected MovableScoreIterator scorer;
  protected MovableExtentIterator extents;
  protected MovableIterator mover;
  protected boolean sharedChildren;

  public FilteredIterator(NodeParameters parameters, MovableIndicatorIterator indicator, MovableCountIterator counter) {
    this.sharedChildren = parameters.get("shareNodes", false);
    this.indicator = indicator;
    this.scorer = null;
    this.counter = counter;
    this.mover = counter;
    if (MovableExtentIterator.class.isAssignableFrom(counter.getClass())) {
      this.extents = (MovableExtentIterator) counter;
    } else {
      this.extents = null;
    }
  }

  public FilteredIterator(NodeParameters parameters, MovableIndicatorIterator indicator, MovableScoreIterator scorer) {
    this.sharedChildren = parameters.get("shareNodes", false);
    this.indicator = indicator;
    this.counter = null;
    this.extents = null;
    this.scorer = scorer;
    this.mover = scorer;
  }

  public FilteredIterator(NodeParameters parameters, MovableIndicatorIterator indicator, MovableExtentIterator extents) {
    this.sharedChildren = parameters.get("shareNodes", false);
    this.indicator = indicator;
    this.scorer = null;
    this.extents = extents;
    this.mover = extents;
    // Try to treat it as a counter if possible
    if (MovableCountIterator.class.isAssignableFrom(extents.getClass())) {
      this.counter = (MovableCountIterator) extents;
    } else {
      this.counter = null;
    }
  }

  @Override
  public void reset() throws IOException {
    indicator.reset();
    mover.reset();
  }

  @Override
  public ExtentArray extents() {
    return extents.extents();
  }

  @Override
  public ExtentArray getData() {
    return extents.getData();
  }

  @Override
  public int count() {
    return counter.count();
  }

  @Override
  public int maximumCount() {
    return counter.maximumCount();
  }

  @Override
  public double score() {
    return scorer.score();
  }

  @Override
  public double maximumScore() {
    return scorer.maximumScore();
  }

  @Override
  public double minimumScore() {
    return scorer.minimumScore();
  }

  @Override
  public void setContext(ScoringContext context) {
    this.context = context;
  }

  @Override
  public ScoringContext getContext() {
    return this.context;
  }

  @Override
  public boolean isDone() {
    return mover.isDone();
  }

  @Override
  public int currentCandidate() {
    return mover.currentCandidate();
  }

  @Override
  public boolean hasAllCandidates() {
    return false;
  }

  @Override
  public void next() throws IOException {
    moveTo(currentCandidate() + 1);
  }

  @Override
  public void movePast(int identifier) throws IOException {
    moveTo(identifier + 1);
  }

  @Override
  public void moveTo(int identifier) throws IOException {
    if (!isDone()) {
      indicator.moveTo(identifier);
      mover.moveTo(identifier);
      if (!sharedChildren) {
        findBestCandidate();
      }
    }
  }

  // Stops the iterators on the first document
  // that passes the indicator and is contained by
  // the mover list
  // -- this is the aggressive option
  private void findBestCandidate() throws IOException {
    int lowestBest = 0;
    while (!isDone()) {
      lowestBest = Math.max(lowestBest, indicator.currentCandidate());
      lowestBest = Math.max(lowestBest, mover.currentCandidate());
      indicator.moveTo(lowestBest);
      mover.moveTo(lowestBest);
      if (this.atCandidate(lowestBest)) {
        return;
      }
      // ensure we progress.
      lowestBest += 1;
    }
  }

  @Override
  public String getEntry() throws IOException {
    throw new UnsupportedOperationException("Filter nodes don't have singular values");
  }

  @Override
  public long totalEntries() {
    return Math.min(indicator.totalEntries(), mover.totalEntries());
  }

  @Override
  public int compareTo(MovableIterator other) {
    if (isDone() && !other.isDone()) {
      return 1;
    }
    if (other.isDone() && !isDone()) {
      return -1;
    }
    if (isDone() && other.isDone()) {
      return 0;
    }
    return currentCandidate() - other.currentCandidate();
  }
}
