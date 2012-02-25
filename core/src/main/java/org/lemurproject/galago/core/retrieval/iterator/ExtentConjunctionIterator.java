// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public abstract class ExtentConjunctionIterator extends ExtentCombinationIterator {

  protected int document;
  protected boolean done;
  protected boolean sharedChildren;

  public ExtentConjunctionIterator(Parameters globalParameters, NodeParameters parameters, ExtentValueIterator[] extIterators) throws IOException {
    this.sharedChildren = globalParameters.get("shareNodes", false);
    this.done = false;
    this.document = 0;
    iterators = new ExtentValueIterator[extIterators.length];
    for (int i = 0; i < extIterators.length; i++) {
      iterators[i] = extIterators[i];
    }
    this.extents = new ExtentArray();
  }

  public int currentCandidate() {
    return document;
  }

  public boolean isDone() {
    return done;
  }

  public boolean moveTo(int identifier) throws IOException {
    extents.reset();

    for (ValueIterator iterator : iterators) {
      iterator.moveTo(identifier);
      if (iterator.isDone()) {
        done = true;
      }
    }

    document = MoveIterators.findMaximumDocument(iterators);
    if (!done && MoveIterators.allHasMatch(iterators, document)) {
      // try to load some extents (subclass does this)
      extents.reset();
      loadExtents();
    }
    if (!this.sharedChildren && !this.atCandidate(document)) {
      findDocument();
    }

    return !done;
  }

  public void reset() throws IOException {
    for (ExtentIterator iterator : iterators) {
      iterator.reset();
    }
    done = false;
    document = 0;
    moveTo(0);
  }

  public long totalEntries() {
    long min = Long.MAX_VALUE;
    for (ValueIterator iterator : iterators) {
      min = Math.min(min, iterator.totalEntries());
    }
    return min;
  }

  private void findDocument() throws IOException {
    while (!done) {
      // find a document that might have some matches
      document = MoveIterators.moveAllToSameDocument(iterators);

      // if we're done, quit now
      if (document == Integer.MAX_VALUE) {
        done = true;
        break;
      }

      // try to load some extents (subclass does this)
      extents.reset();
      loadExtents();

      // were we successful? if so, quit, otherwise keep looking for documents
      if (extents.size() > 0) {
        break;
      }
      iterators[0].movePast(document);
    }
  }
}
