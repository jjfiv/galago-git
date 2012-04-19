/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;

/**
 *
 * @author sjh
 */
@RequiredParameters(parameters={"shareNodes"})
public abstract class ConjunctionIterator implements MovableIterator {

  protected MovableIterator[] iterators;
  protected MovableIterator[] drivingIterators;
  protected boolean hasAllCandidates;
  protected boolean sharedChildren;

  public ConjunctionIterator(NodeParameters parameters, MovableIterator[] queryIterators) {
    this.sharedChildren = parameters.get("shareNodes", false);
    this.iterators = queryIterators;

    // count the number of iterators that dont have
    // a non-default data for all candidates
    int drivingIteratorCount = 0;
    for (MovableIterator iterator : this.iterators) {
      if (!iterator.hasAllCandidates()) {
        drivingIteratorCount++;
      }
    }

    if (drivingIteratorCount <= 0) {
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
  public void moveTo(int candidate) throws IOException {
    for (MovableIterator iterator : iterators) {
      iterator.moveTo(candidate);
    }
    
    // if we are not sharing children - we can be more aggressive here.
    if (!sharedChildren) {
      int currCandidate = currentCandidate();
      while (!isDone()) {
        for (MovableIterator iterator : iterators) {
          iterator.moveTo(currCandidate);

          // if we skip too far:
          //   don't bother to move the other children
          //   we will need to pick a different candidate
          if(!iterator.atCandidate(currCandidate)){
            break;
          }
        }

        if (atCandidate(currCandidate)) {
          return;
        }
        currCandidate = Math.max(currCandidate + 1, currentCandidate());
      }
    }
  }

  // these functions are final to ensure that they are never overridden
  @Override
  public final void movePast(int candidate) throws IOException {
    this.moveTo(candidate + 1);
  }

  // these functions are final to ensure that they are never overridden
  @Override
  public final void next() throws IOException {
    this.moveTo(currentCandidate() + 1);
  }

  @Override
  public int currentCandidate() {
    int candidateMax = Integer.MIN_VALUE;
    int candidateMin = Integer.MAX_VALUE;
    for (MovableIterator iterator : drivingIterators) {
      if (iterator.isDone()) {
        return Integer.MAX_VALUE;
      }
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
    for (MovableIterator iterator : drivingIterators) {
      if (iterator.isDone() || !iterator.atCandidate(candidate)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isDone() {
    for (MovableIterator iterator : drivingIterators) {
      if (iterator.isDone()) {
        return true;
      }
    }
    return false;
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
    long min = Integer.MAX_VALUE;
    for (MovableIterator iterator : iterators) {
      min = Math.min(min, iterator.totalEntries());
    }
    return min;
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
}
