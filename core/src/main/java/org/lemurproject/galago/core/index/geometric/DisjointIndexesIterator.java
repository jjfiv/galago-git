/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.io.IOException;
import java.util.Collection;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.index.MovableValueIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public abstract class DisjointIndexesIterator extends MovableValueIterator {

  Collection<MovableIterator> allIterators;
  MovableIterator head;
  PriorityQueue<MovableIterator> queue;

  public DisjointIndexesIterator(Collection<MovableIterator> iterators) {
    allIterators = iterators;
    queue = new PriorityQueue(iterators);
    head = queue.poll();
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
  public boolean atCandidate(int identifier) {
    return (head.currentCandidate() == identifier);
  }

  @Override
  public void movePast(int identifier) throws IOException {
    moveTo(currentCandidate() + 1);
  }

  @Override
  public boolean next() throws IOException {
    return moveTo(currentCandidate() + 1);
  }

  @Override
  public boolean moveTo(int identifier) throws IOException {
    queue.offer(head);
    while (!queue.isEmpty()) {
      head = queue.poll();
      head.moveTo(identifier);
      if (head.atCandidate(identifier)) {
        return true;
      } else if (!head.isDone()) {
        return false;
      }
      // otherwise head is done + it has been removed from the queue.
    }
    //if the queue is empty - set head == null
    return false;
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
    for(MovableIterator i : allIterators){
      flag &= i.hasAllCandidates();
    }
    return flag;
  }

  @Override
  public int compareTo(MovableIterator o) {
    return Utility.compare(this.currentCandidate(), o.currentCandidate());
  }
}
