// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.MovableValueIterator;

/**
 * This is a marker interface that represents any kind of
 * iterator over an inverted list or query operator.
 * 
 * @author trevor
 */
public interface MovableIterator extends StructuredIterator,
        Comparable<MovableIterator> {

  /**
   * returns the current candidate
   *  - if isDone - returns Integer.MAX_VALUE
   */
  public int currentCandidate();

  /**
   * returns true if the iterator is at this candidate
   */
  public boolean atCandidate(int identifier);

  /**
   * returns true if the iterator has data for ALL candidates
   *  - e.g. priors, lengths, names.
   * 
   * These iterators are assumed to be background information
   * for iterators without all candidates
   */
  public boolean hasAllCandidates();

  /**
   * Moves to the next candidate
   *  - this functions can be implemented as 
   *    moveTo(currentCandidate() + 1)
   * 
   * -- boolean indicates if the iterator isDone()
   */
  public boolean next() throws IOException;

  /**
   * Moves to the next candidate
   *  - this functions can be implemented as 
   *    moveTo(identifier + 1)
   */
  public void movePast(int identifier) throws IOException;

  /**
   * Moves the iterator to the next candidate
   *  - should check if the iterator can be passive or aggressive
   *  - That is if the iterator shares children/leaf nodes with other iterators
   * 
   * -- boolean indicates if the iterator isDone()
   */
  public boolean moveTo(int identifier) throws IOException;

  /**
   * returns the iterator to the first candidate
   */
  public void reset() throws IOException;

  /**
   * return true if the iterator has no more candidates
   */
  public boolean isDone();
  
  /**
   * Returns a string representation of the current candidate + value
   */
  public String getEntry() throws IOException;

  /**
   * Returns an over estimate of the total entries in the iterator
   */
  public long totalEntries();
  
}

