// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 * Acts as a wrapper around other iterators. It'll take any of the
 * iterators, since it's up to the implementation of getStatus to use
 * them properly. The only method that needs to be implemented is getStatus,
 * which indicates the status of the indicator (1 = true, 0 = false).
 *
 * Some examples:
 *
 * #any ( term1 term2 term3 ) : Status is 1 when any of the terms are in the document. 0 otherwise.
 * #all ( term1 term2 term3 ) : Status is 1 when all of the terms are in the document. 0 otherwise.
 * #mincount:5 ( #ow1( president bush ) ) : Status is 1 when the phrase "president bush" occurs at least 5 times in the document.
 * #maxcount:3 ( juice ) : Status is 1 when the term "juice" occurs no more than 3 times in the document.
 *
 * Use cases:
 *
 * #require ( AbstractIndicator ScoreIterator ) : Only scores documents that where the AbstractIndicator is on
 * #reject ( AbstractIndicator ScoreIterator ) : Only scores documents that where the AbstractIndicator is off
 * Retrieval.runBooleanQuery - this operation returns a set of documents that meet the requirements. Top node has to
 * be some kind of AbstractIndicator.
 *
 * @author irmarc
 */
public abstract class AbstractIndicator
        implements IndicatorIterator {

  protected ValueIterator[] iterators;

  public AbstractIndicator(NodeParameters p, ValueIterator[] childIterators) {
    this.iterators = childIterators;
  }

  public void movePast(int identifier) throws IOException {
    moveTo(identifier + 1);
  }

  public boolean next() throws IOException {
    if (this.isDone()) {
      return ! this.isDone();
    }
    return moveTo(this.currentCandidate() + 1);
  }

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
}
