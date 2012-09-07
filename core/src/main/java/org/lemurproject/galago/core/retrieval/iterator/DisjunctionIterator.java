/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;

/**
 *
 * @author sjh
 */
public abstract class DisjunctionIterator implements MovableIterator {

  protected MovableIterator[] iterators;
  protected MovableIterator[] drivingIterators;
  protected ScoringContext context;
  protected boolean hasAllCandidates;

  public DisjunctionIterator(MovableIterator[] queryIterators) {
    // first check that the iterators are all MovableIterators:
    this.iterators = queryIterators;

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
        if (!iterator.hasAllCandidates()) {
          drivingIterators[i] = iterator;
          i++;
        }
      }
    }
  }

  @Override
  public void syncTo(int candidate) throws IOException {
    for (MovableIterator iterator : iterators) {
      iterator.syncTo(candidate);
    }
  }

  @Override
  public void movePast(int candidate) throws IOException {
    for (MovableIterator iterator : this.drivingIterators) {
      iterator.movePast(candidate);
    }
  }

  @Override
  public int currentCandidate() {
    // the current candidate is the smallest of the set
    int candidate = Integer.MAX_VALUE;
    for (MovableIterator iterator : drivingIterators) {
      if (!iterator.isDone()) {
        candidate = Math.min(candidate, iterator.currentCandidate());
      }
    }
    return candidate;
  }

  @Override
  public boolean hasMatch(int candidate) {
    for (MovableIterator iterator : drivingIterators) {
      if (iterator.hasMatch(candidate)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isDone() {
    for (MovableIterator iterator : drivingIterators) {
      if (!iterator.isDone()) {
        return false;
      }
    }
    return true;
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
  public long totalEntries() {
    long total = 0;
    for (MovableIterator i : this.iterators) {
      if (i.hasAllCandidates()) {
        return i.totalEntries();
      } else {
        total += i.totalEntries();
      }
    }
    return total;
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
    return this.currentCandidate() - other.currentCandidate();
  }
  
  @Override
  public void setContext(ScoringContext context) {
    this.context = context;
  }

  @Override
  public ScoringContext getContext() {
    return context;
  }  
}
