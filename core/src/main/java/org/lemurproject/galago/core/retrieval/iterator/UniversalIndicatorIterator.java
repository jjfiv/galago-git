// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Implements the #all indicator operator.
 * @author irmarc
 */
public class UniversalIndicatorIterator extends AbstractIndicator {

  private int document;
  private boolean done;
  private boolean sharedChildren;

  public UniversalIndicatorIterator(Parameters globalParams, NodeParameters p, ValueIterator[] children) {
    super(p, children);
    sharedChildren = globalParams.get("shareNodes", false);

    // guarantees the correct order for the MoveIterators
    Arrays.sort(iterators, new Comparator<ValueIterator>() {

      public int compare(ValueIterator it1, ValueIterator it2) {
        return (int) (it1.totalEntries() - it2.totalEntries());
      }
    });

    try {
      document = MoveIterators.moveAllToSameDocument(iterators);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    done = (document == Integer.MAX_VALUE);
  }

  public int currentCandidate() {
    return document;
  }

  public void reset() throws IOException {
    for (ValueIterator i : iterators) {
      i.reset();
    }
    moveTo(0);
  }

  public boolean atCandidate(int doc) {
    return (document == doc) && MoveIterators.allHasMatch(iterators, doc);
  }

  public boolean hasAllCandidates() {
    //for(MovableIterator i : iterators){
    //  if(!i.hasAllCandidates()){
    //    return false;
    //  }
    //}
    //return true;
    return false;
  }

  public boolean indicator(int doc) {
    return atCandidate(doc);
  }

  public boolean isDone() {
    return done;
  }

  public boolean moveTo(int identifier) throws IOException {
    for (ValueIterator iterator : iterators) {
      iterator.moveTo(identifier);
    }
    // candidate is the highest document
    document = MoveIterators.findMaximumDocument(iterators);

    // if we share children with other nodes - be passive
    if (sharedChildren) {
      if (!atCandidate(document)) {
        // back off one document -- this allows a movePast to consider the next document correctly
        document--;
      }

      // otherwise our children are our own - be aggresive
    } else {
      document = MoveIterators.moveAllToSameDocument(iterators);
    }

    done = (document == Integer.MAX_VALUE);
    return !done;
  }

  public String getEntry() throws IOException {
    return Integer.toString(document);
  }

  /**
   * Uses the min of all lists. This is inaccurate, but taking the union is
   * a horrible idea.
   * @return
   */
  public long totalEntries() {
    long min = Integer.MAX_VALUE;
    for (ValueIterator iterator : iterators) {
      min = Math.min(min, iterator.totalEntries());
    }
    return min;
  }
}
