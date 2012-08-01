/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.io.IOException;
import java.util.Collection;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public abstract class DisjointIndexesIterator extends ValueIterator {

  Collection<MovableIterator> allIterators;
  MovableIterator head;
  PriorityQueue<MovableIterator> queue;

  public DisjointIndexesIterator(Collection<MovableIterator> iterators) {
    allIterators = iterators;
    queue = new PriorityQueue(iterators);
    head = queue.poll();
  }

  @Override
  public void setContext(ScoringContext context) {
    this.context = context;
    
    // this is a special case - otherwise children iterators will not receive the context.
    for(MovableIterator i : this.allIterators){
      i.setContext(context);
    }
  }

  @Override
  public ScoringContext getContext() {
    return context;
  }  
  
  @Override
  public boolean isDone() {
    return queue.isEmpty() && head.isDone();
  }

  @Override
  public int currentCandidate() {
    return head.currentCandidate();
  }

  @Override
  public boolean hasMatch(int identifier) {
    return (head.currentCandidate() == identifier);
  }

  @Override
  public void movePast(int identifier) throws IOException {
    moveTo(identifier + 1);
  }

  @Override
  public void moveTo(int identifier) throws IOException {
    queue.offer(head);
    while (!queue.isEmpty()) {
      head = queue.poll();
      head.moveTo(identifier);
      if (queue.isEmpty()) {
        // if the queue is empty - we're done
        return;
      } else if (!head.isDone()
              && !queue.isEmpty()
              && head.compareTo(queue.peek()) < 0) {
        // otherwise check if head is still the head
        return;
      }

      // if we are here - head may be done - or may not be the head anymore
      if (!head.isDone()) {
        queue.offer(head);
      }
    }
  }

  @Override
  public String getEntry() throws IOException {
    return head.getEntry();
  }

  @Override
  public long totalEntries() {
    long count = 0;
    for (MovableIterator i : allIterators) {
      count += i.totalEntries();
    }
    return count;
  }

  @Override
  public void reset() throws IOException {
    queue = new PriorityQueue();
    for (MovableIterator i : allIterators) {
      i.reset();
      queue.offer(i);
    }
    while (!queue.isEmpty()) {
      head = queue.poll();
      if (!head.isDone()) {
        return;
      }
    }
  }

  @Override
  public boolean hasAllCandidates() {
    boolean flag = true;
    for (MovableIterator i : allIterators) {
      flag &= i.hasAllCandidates();
    }
    return flag;
  }

  @Override
  public int compareTo(MovableIterator o) {
    return Utility.compare(this.currentCandidate(), o.currentCandidate());
  }

  @Override
  public String getKeyString() throws IOException {
    return ((ValueIterator) head).getKeyString();
  }

  @Override
  public byte[] getKeyBytes() throws IOException {
    return ((ValueIterator) head).getKeyBytes();
  }
}
