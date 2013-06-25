/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public abstract class ConjunctionIterator implements BaseIterator {

  protected BaseIterator[] iterators;
  protected BaseIterator[] drivingIterators;
  protected boolean hasAllCandidates;

  public ConjunctionIterator(NodeParameters parameters, BaseIterator[] queryIterators) {
    this.iterators = queryIterators;

    // count the number of iterators that dont have
    // a non-default data for all candidates
    int drivingIteratorCount = 0;
    for (BaseIterator iterator : this.iterators) {
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
      //   does not stop at ALL documents
      hasAllCandidates = false;
      drivingIterators = new BaseIterator[drivingIteratorCount];
      int i = 0;
      for (BaseIterator iterator : this.iterators) {
        if (!iterator.hasAllCandidates()) {
          drivingIterators[i] = iterator;
          i++;
        }
      }
    }
  }

  @Override
  public void syncTo(long candidate) throws IOException {
    for (BaseIterator iterator : iterators) {
      long prev = iterator.currentCandidate();
      iterator.syncTo(candidate);
    }
  }

  @Override
  public void movePast(long candidate) throws IOException {
    for (BaseIterator iterator : this.drivingIterators) {
      iterator.movePast(candidate);
    }
  }

  @Override
  public long currentCandidate() {
    long candidateMin = Long.MAX_VALUE;
    long candidateMax = -1; // impossibly small candidate //
    for(int i=0; i<drivingIterators.length;i++){
      if (!drivingIterators[i].isDone()) {
        long otherCandidate = drivingIterators[i].currentCandidate();
        candidateMin = (candidateMin <= otherCandidate)? candidateMin : otherCandidate;
        candidateMax = (candidateMax >= otherCandidate)? candidateMax : otherCandidate;
      }
    }
    if (candidateMax == candidateMin) {
      return candidateMax;
    } else {
      return candidateMax - 1;
    }
  }

  @Override
  public boolean hasMatch(long candidate) {
    for (BaseIterator iterator : drivingIterators) {
      if (iterator.isDone() || !iterator.hasMatch(candidate)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isDone() {
    for (BaseIterator iterator : drivingIterators) {
      if (iterator.isDone()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void reset() throws IOException {
    for (BaseIterator iterator : iterators) {
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
    for (BaseIterator iterator : iterators) {
      long otherMin = iterator.totalEntries();
      min = (min <= otherMin)? min : otherMin;
    }
    return min;
  }

  @Override
  public int compareTo(BaseIterator other) {
    if (isDone() && !other.isDone()) {
      return 1;
    }
    if (other.isDone() && !isDone()) {
      return -1;
    }
    if (isDone() && other.isDone()) {
      return 0;
    }
    return Utility.compare(currentCandidate(), other.currentCandidate());
  }
}
