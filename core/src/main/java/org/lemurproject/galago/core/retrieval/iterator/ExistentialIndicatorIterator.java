// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 * Implements the #any indicator operator.
 * @author irmarc
 */
public class ExistentialIndicatorIterator extends AbstractIndicator {

  private int document;
  private boolean done;

  public ExistentialIndicatorIterator(NodeParameters p, ValueIterator[] children) {
    super(p, children);
    updateState();
  }
  
  public boolean moveTo(int identifier) throws IOException {
    for (ValueIterator iterator : iterators) {
      iterator.moveTo(identifier);
    }
    updateState();
    return !done;
  }

  private void updateState() {
    int candidate = Integer.MAX_VALUE;
    for (ValueIterator iterator : iterators) {
      if (!iterator.isDone()) {
        candidate = Math.min(candidate, iterator.currentCandidate());
      }
    }
    document = candidate;
    if (document == Integer.MAX_VALUE) {
      done = true;
    }
  }

  public int currentCandidate() {
    return document;
  }

  public boolean isDone(){
    return done;
  }

  public void reset() throws IOException {
    for(ValueIterator i : iterators){
      i.reset();
    }
    done = false;
    updateState();
  }

  public boolean atCandidate(int doc){
    return (doc == document)
            && MoveIterators.anyHasMatch(iterators, doc);
  }

  public boolean hasAllCandidates(){
    //for(MovableIterator i : iterators){
    //  if(i.hasAllCandidates()){
    //    return true;
    //  }
    //}
    return false;
  }
  
  public boolean indicator(int doc){
    return atCandidate(doc);
  }
  
  public String getEntry() throws IOException {
    return Integer.toString(document);
  }

  /**
   * Uses the max of all lists. [ESTIMATION]
   * @return
   */
  public long totalEntries() {
    long max = Integer.MIN_VALUE;
    for (ValueIterator iterator : iterators) {
      max = Math.max(max, iterator.totalEntries());
    }
    return max;
  }
}
