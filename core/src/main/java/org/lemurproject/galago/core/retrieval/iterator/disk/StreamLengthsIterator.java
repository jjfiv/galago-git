// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.disk;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.index.disk.StreamLengthsSource;
import org.lemurproject.galago.core.index.stats.CollectionAggregateIterator;
import org.lemurproject.galago.core.index.stats.CollectionStatistics;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.SourceIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;

/**
 * StreamLengthsIterator wraps the disk-based StreamLengthsSource
 * @author jfoley
 * @see StreamLengthsSource
 */
public class StreamLengthsIterator extends SourceIterator<StreamLengthsSource> implements CountIterator, LengthsIterator, CollectionAggregateIterator {

  public StreamLengthsIterator(StreamLengthsSource src) throws IOException {
    super(src);
  }

  @Override
  public String getValueString() throws IOException {
    return currentCandidate() + "," + getCurrentLength();
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "lengths";
    String className = this.getClass().getSimpleName();
    String parameters = getKeyString();
    int document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = Integer.toString(getCurrentLength());
    List<AnnotatedNode> children = Collections.EMPTY_LIST;
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }

  @Override
  public int count() {
    return (int) source.count(context.document);
  }

  @Override
  public int getCurrentLength() {
    return count();
  }

  @Override
  public int maximumCount() {
    return Integer.MAX_VALUE;
  }

  @Override
  public CollectionStatistics getStatistics() {
    CollectionStatistics cs = new CollectionStatistics();
    cs.fieldName = source.key();
    cs.collectionLength = source.collectionLength;
    cs.documentCount = source.totalDocumentCount;
    cs.nonZeroLenDocCount = source.nonZeroDocumentCount;
    cs.maxLength = source.maxLength;
    cs.minLength = source.minLength;
    cs.avgLength = source.avgLength;
    return cs;
  }
  
}
