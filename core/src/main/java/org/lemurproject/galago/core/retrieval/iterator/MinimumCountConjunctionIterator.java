/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.index.AggregateReader;
import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Provides the highest possible count of all the interior iterators. As this is
 * a conjunction node, that means the infimum of the set of interior iterators.
 *
 *
 * @author irmarc
 */
public class MinimumCountConjunctionIterator extends ConjunctionIterator implements MovableCountIterator {

  int collectionEstimateIndex;
  int max = Integer.MAX_VALUE;
  protected byte[] key;

  public MinimumCountConjunctionIterator(NodeParameters p, MovableCountIterator[] iterators)
          throws IOException {
    super(p, iterators);
    max = (int) p.get("maximumCount", Integer.MAX_VALUE);
    buildKey(iterators);
  }

  @Override
  public byte[] key() {
    return key;
  }

  protected void buildKey(MovableCountIterator[] iterators) {
    int keysize = 2;
    for (int i = 0; i < iterators.length; i++) {
      keysize += iterators[i].key().length;
    }
    key = new byte[keysize];
    keysize = 2;
    key[0] = 'M' >>> 8; // conjunction marker;                                                                                              
    key[1] = 'M' & 0xFF;
    for (int i = 0; i < iterators.length; i++) {
      MovableCountIterator it = iterators[i];
      byte[] inner = it.key();
      System.arraycopy(inner, 0, key, keysize, inner.length);
      keysize += inner.length;
    }
  }

  @Override
  public String getEntry() throws IOException {
    String[] entries = new String[iterators.length];
    for (int i = 0; i < iterators.length; i++) {
      entries[i] = ((MovableIterator) iterators[i]).getEntry();
    }
    String combined = Utility.join(entries);
    return String.format("<%s>", combined);
  }

  @Override
  public int count() {
    int infimum = Integer.MAX_VALUE;
    for (int i = 0; i < iterators.length; i++) {
      infimum = Math.min(((MovableCountIterator) iterators[i]).count(), infimum);
    }
    return infimum;
  }

  @Override
  public int maximumCount() {
    if (max == Integer.MAX_VALUE) {
      for (int i = 0; i < iterators.length; i++) {
        max = Math.min(max, ((MovableCountIterator) iterators[i]).maximumCount());
      }
    }
    return max;
  }

  public int getCFDifference(int currentDocument) {
    // If it's not from the estimating iterator, nothing to do
    if (!iterators[collectionEstimateIndex].hasMatch(currentDocument)) {
      return 0;
    }

    // Otherwise we have a chance
    int actual = Integer.MAX_VALUE;
    int estimated = 0;
    for (int i = 0; i < iterators.length; i++) {
      if (iterators[i].hasMatch(currentDocument)) {
        actual = Math.min(((MovableCountIterator) iterators[i]).count(), actual);
      } else {
        actual = 0;
      }
      if (i == collectionEstimateIndex) {
        estimated = ((MovableCountIterator) iterators[i]).count();
      }
    }

    int diff = estimated - actual;
    if (diff > 0) {
      ////CallTable.increment("cf_correct");
      ////CallTable.increment("cf_correct_amount", diff);
    }
    return diff;
  }

  public int getDFDifference(int currentDocument) {
    // If it's not from the estimating iterator, nothing to do
    if (!iterators[collectionEstimateIndex].hasMatch(currentDocument)) {
      return 0;
    }

    // Otherwise we have a chance
    for (int i = 0; i < iterators.length; i++) {
      if (!iterators[i].hasMatch(currentDocument)) {
        ////CallTable.increment("df_correct");
        return 1;
      }
    }

    return 0;
  }

  public int getBestCollectionFrequency() {
    int cf = Integer.MAX_VALUE;
    for (int i = 0; i < iterators.length; i++) {
      if (AggregateReader.AggregateIterator.class.isAssignableFrom(iterators[i].getClass())) {
        NodeStatistics ns = ((AggregateReader.AggregateIterator) iterators[i]).getStatistics();
        int nf = (int) ns.nodeFrequency;
        if (nf < cf) {
          cf = nf;
          collectionEstimateIndex = i;
        }
      }
    }
    return cf;
  }

  public int getBestDocumentFrequency() {
    int df = Integer.MAX_VALUE;
    for (int i = 0; i < iterators.length; i++) {
      if (AggregateReader.AggregateIterator.class.isAssignableFrom(iterators[i].getClass())) {
        NodeStatistics ns = ((AggregateReader.AggregateIterator) iterators[i]).getStatistics();
        int nf = (int) ns.nodeDocumentCount;
        if (nf < df) {
          df = nf;
          collectionEstimateIndex = i;
        }
      }
    }
    return df;
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "mincount";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    int document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = Integer.toString(count());
    List<AnnotatedNode> children = new ArrayList();
    for (MovableIterator child : this.iterators) {
      children.add(child.getAnnotatedNode());
    }
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
