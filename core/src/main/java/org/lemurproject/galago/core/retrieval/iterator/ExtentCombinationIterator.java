// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.ArrayList;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Abstract class to provide some of the higher-level navigation and logic based on implementation carried out
 * in any implementing subclass. Any iterator that operates on multiple extent iterators should subclass this class.
 *
 * @author irmarc
 */
public abstract class ExtentCombinationIterator implements ExtentValueIterator, CountValueIterator {

  /**
   * The iterators this iterator manages.
   */
  protected ExtentValueIterator[] iterators;
  /**
   * The currently loaded set of extents. Cannot be null, but can be empty.
   */
  protected ExtentArray extents;

  /**
   * Returns a string representation of the currently loaded extents.
   * @return
   * @throws IOException
   */
  @Override
  public String getEntry() throws IOException {
    ArrayList<String> strs = new ArrayList<String>();
    ExtentArrayIterator eai = new ExtentArrayIterator(extents);
    while (!eai.isDone()) {
      strs.add(String.format("[%d, %d]", eai.currentBegin(), eai.currentEnd()));
      eai.next();
    }
    return Utility.join(strs.toArray(new String[0]), ",");
  }

  /**
   * Return the currently loaded extents.
   */
  @Override
  public ExtentArray extents() {
    return extents;
  }

  /**
   * Return the currently loaded extents.
   */
  @Override
  public ExtentArray getData() {
    return extents;
  }

  /**
   * Returns the number of extents in the current document. Note that this is
   * <i>not</i> the same as the number of positions found in the document.
   */
  @Override
  public int count() {
    return extents().size();
  }

  @Override
  public int maximumCount() {
    return Integer.MAX_VALUE;
  }

  /**
   * Returns whether or not this iterator is currently evaluating the identifier passed.
   *  - And checks that there is some extent in the extent array
   */
  @Override
  public boolean hasMatch(int identifier) {
    return ((currentCandidate() == identifier) && (extents.size() > 0));
  }

  /**
   * Moves this iterator past the identifier passed in.
   * @param identifier
   * @throws IOException
   */
  @Override
  public void movePast(int identifier) throws IOException {
    moveTo(identifier + 1);
  }

  @Override
  public boolean next() throws IOException {
    return moveTo(currentCandidate() + 1);
  }

  /**
   * The comparison first performs a "done" check on both iterators. Any iterator that is done is
   * considered to have the highest possible value according to ordering. If neither iterator is done,
   * then the comparison is a standard integer compare based on the current identifiers being evaluated
   * by the two iterators.
   * @param other
   * @return
   */
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
    return currentCandidate() - other.currentCandidate();
  }

  /**
   * This forms the core policy of the iterator. Which extents get loaded ultimately determine the
   * behavior of the other functions. See #OrderedWindowIterator or #ExtentInsideIterator for some good examples.
   */
  public abstract void loadExtents();
}
