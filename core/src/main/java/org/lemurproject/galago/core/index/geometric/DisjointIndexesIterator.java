/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.io.IOException;
import java.util.Collection;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskIterator;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public abstract class DisjointIndexesIterator extends DiskIterator {

  Collection<BaseIterator> allIterators;
  BaseIterator head;
  PriorityQueue<BaseIterator> queue;

  public DisjointIndexesIterator(Collection<BaseIterator> iterators) {
    allIterators = iterators;
    queue = new PriorityQueue(iterators);
    head = queue.poll();
  }

  @Override
  public void setContext(ScoringContext context) {
    this.context = context;

    for(BaseIterator i : this.allIterators){
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
  public long currentCandidate() {
    return head.currentCandidate();
  }

  @Override
  public boolean hasMatch(long identifier) {
    return (head.currentCandidate() == identifier);
  }

  @Override
  public void movePast(long identifier) throws IOException {
    syncTo(identifier + 1);
  }

  @Override
  public void syncTo(long identifier) throws IOException {
    queue.offer(head);
    while (!queue.isEmpty()) {
      head = queue.poll();
      head.syncTo(identifier);
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
  public String getValueString(ScoringContext c) throws IOException {
    return head.getValueString(c);
  }

  @Override
  public long totalEntries() {
    long count = 0;
    for (BaseIterator i : allIterators) {
      count += i.totalEntries();
    }
    return count;
  }

  @Override
  public void reset() throws IOException {
    queue = new PriorityQueue();
    for (BaseIterator i : allIterators) {
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
    for (BaseIterator i : allIterators) {
      flag &= i.hasAllCandidates();
    }
    return flag;
  }

  @Override
  public int compareTo(BaseIterator o) {
    return Utility.compare(this.currentCandidate(), o.currentCandidate());
  }

  @Override
  public String getKeyString() throws IOException {
    return ((DiskIterator) head).getKeyString();
  }
}
