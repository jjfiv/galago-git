/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.io.IOException;
import java.util.Collection;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public abstract class DisjointIndexesIterator implements ValueIterator {

  Collection<ValueIterator> allIterators;
  ValueIterator head;
  PriorityQueue<ValueIterator> queue;

  public DisjointIndexesIterator(Collection<ValueIterator> iterators) {
    allIterators = iterators;
    queue = new PriorityQueue(iterators);
    head = queue.poll();
  }

  public boolean isDone() {
    return queue.isEmpty() && head.isDone();
  }

  public int currentCandidate() {
    return head.currentCandidate();
  }

  public boolean hasMatch(int identifier) {
    return (head.currentCandidate() == identifier);
  }

  public void movePast(int identifier) throws IOException {
    moveTo(currentCandidate() + 1);
  }

  public boolean next() throws IOException {
    return moveTo(currentCandidate() + 1);
  }

  public boolean moveTo(int identifier) throws IOException {
    queue.offer(head);
    while (!queue.isEmpty()) {
      head = queue.poll();
      head.moveTo(identifier);
      if (head.hasMatch(identifier)) {
        return true;
      } else if (!head.isDone()) {
        return false;
      }
      // otherwise head is done + it has been removed from the queue.
    }
    //if the queue is empty - set head == null
    return false;
  }

  public String getEntry() throws IOException {
    return head.getEntry();
  }

  public long totalEntries() {
    long count = 0;
    for (ValueIterator i : allIterators) {
      count += i.totalEntries();
    }
    return count;
  }

  public void reset() throws IOException {
    queue = new PriorityQueue();
    for (ValueIterator i : allIterators) {
      i.reset();
      queue.offer(i);
    }
    while(!queue.isEmpty()){
      head = queue.poll();
      if(!head.isDone()){
        return;
      }
    }
  }

  public int compareTo(ValueIterator o) {
    return Utility.compare(this.currentCandidate(), o.currentCandidate());
  }
}
