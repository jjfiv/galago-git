// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.disk;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.index.source.CountSource;
import org.lemurproject.galago.core.index.stats.NodeAggregateIterator;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;

/**
 *
 * @author jfoley, sjh
 */
public class DiskCountIterator extends SourceIterator
  implements NodeAggregateIterator, CountIterator {
  
  CountSource countSrc;
  
  public DiskCountIterator(CountSource src) {
    super(src);
    countSrc = src;
  }

  @Override
  public String getValueString() throws IOException {
    return String.format("%s,%d,%d", getKeyString(), currentCandidate(), count());
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "counts";
    String className = this.getClass().getSimpleName();
    String parameters = this.getKeyString();
    long document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = Integer.toString(count());
    List<AnnotatedNode> children = Collections.EMPTY_LIST;
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }

  @Override
  public NodeStatistics getStatistics() {
    return countSrc.getStatistics();
  }

  @Override
  public int count() {
    return (int) countSrc.count(context.document);
  }

}
