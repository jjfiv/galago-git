/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;

/**
 *
 * @author sjh
 */
public abstract class ConjunctionIterator implements MovableIterator {

  protected MovableIterator[] iterators;
  protected MovableIterator[] drivingIterators;
  boolean hasAllCandidates;

  public ConjunctionIterator(MovableIterator[] queryIterators) {
    // first check that the iterators are all MovableIterators:
    this.iterators = new MovableIterator[queryIterators.length];
    for (int i = 0; i < queryIterators.length; i++) {
      assert (MovableIterator.class.isAssignableFrom(queryIterators[i].getClass())) : "Can not cast " + queryIterators[i].getClass().getName() + " to a " + MovableIterator.class.getName();
      this.iterators[i] = (MovableIterator) queryIterators[i];
    }

    // count the number of iterators that dont have
    // a non-default data for all candidates
    int drivingIteratorCount = 0;
    for (MovableIterator iterator : this.iterators) {
      if (!iterator.hasAllCandidates()) {
        drivingIteratorCount++;
      }
    }

    if (drivingIteratorCount == 0) {
      // if all iterators will report matches for all documents
      // make sure this information is communicated up.
      hasAllCandidates = true;
      drivingIterators = iterators;

    } else {
      // otherwise this disjunction is discriminative
      // and will not report matches for all documents
      //
      // the driving iterators will ensure this iterator
      //   does not stop at all documents
      hasAllCandidates = false;
      drivingIterators = new MovableIterator[drivingIteratorCount];
      int i = 0;
      for (MovableIterator iterator : this.iterators) {
        if (iterator.hasAllCandidates()) {
          drivingIterators[i] = iterator;
          i++;
        }
      }
    }
  }

  @Override
  public boolean moveTo(int candidate) throws IOException {
    for (MovableIterator iterator : iterators) {
      iterator.moveTo(candidate);
    }
    return !isDone();
    // if we are not sharing children - we can be more aggressive here.
  }

  // these functions are final to ensure that they are never overridden
  @Override
  public final void movePast(int candidate) throws IOException {
    this.moveTo(candidate + 1);
  }

  // these functions are final to ensure that they are never overridden
  @Override
  public final boolean next() throws IOException {
    this.moveTo(currentCandidate() + 1);
    return !isDone();
  }

  @Override
  public int currentCandidate() {
    int candidateMax = Integer.MIN_VALUE;
    int candidateMin = Integer.MAX_VALUE;
    for (MovableIterator iterator : drivingIterators) {
      candidateMax = Math.max(candidateMax, iterator.currentCandidate());
      candidateMin = Math.min(candidateMin, iterator.currentCandidate());
    }
    if (candidateMax == candidateMin) {
      return candidateMax;
    } else {
      return candidateMax - 1;
    }
  }

  @Override
  public boolean atCandidate(int candidate) {
    boolean flag = true;
    for (MovableIterator iterator : drivingIterators) {
      flag &= iterator.atCandidate(candidate);
    }
    return flag;
  }

  @Override
  public boolean isDone() {
    boolean flag = false;
    for (MovableIterator iterator : drivingIterators) {
      flag |= iterator.isDone();
    }
    return flag;
  }

  @Override
  public void reset() throws IOException {
    for (MovableIterator iterator : iterators) {
      iterator.reset();
    }
  }

  @Override
  public boolean hasAllCandidates() {
    return hasAllCandidates;
  }

  @Override
  public int compareTo(ValueIterator other) {
    if (isDone() && !other.isDone()) {
      return 1;
    }
    if (other.isDone() && !isDone()) {
      return -1;
    }
    if (isDone() && other.isDone()) {
      return 0;
    }
    return this.currentCandidate() - other.currentCandidate();
  }
}
