// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.disk;

import java.io.IOException;
import java.util.Collections;
import org.lemurproject.galago.core.index.BTreeReader.BTreeIterator;
import org.lemurproject.galago.core.index.disk.StreamExtentSource;
import org.lemurproject.galago.core.index.stats.NodeAggregateIterator;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.ExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.SourceIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.util.ExtentArray;

/**
 *
 * @author jfoley
 */
public class StreamExtentIterator extends SourceIterator<StreamExtentSource> implements NodeAggregateIterator, CountIterator, ExtentIterator {

  public StreamExtentIterator(StreamExtentSource src) throws IOException {
    super(src);
  }
  
  @Override
  public String getValueString() throws IOException {
    StringBuilder builder = new StringBuilder();
    builder.append(getKeyString());
    builder.append(",");
    builder.append(currentCandidate());
    ExtentArray e = extents();
    for (int i = 0; i < e.size(); ++i) {
      builder.append(",");
      builder.append(e.begin(i));
    }
    return builder.toString();
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "extents";
    String className = this.getClass().getSimpleName();
    String parameters = this.getKeyString();
    int document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = extents().toString();
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, Collections.EMPTY_LIST);
  }

  @Override
  public NodeStatistics getStatistics() {
    NodeStatistics stats = new NodeStatistics();
    stats.node = getKeyString();
    stats.nodeFrequency = source.totalPositionCount;
    stats.nodeDocumentCount = source.documentCount;
    stats.maximumCount = source.maximumPositionCount;
    return stats;
  }

  @Override
  public int count() {   
    return (int) source.count(context.document);
  }

  @Override
  public int maximumCount() {
    return source.maximumPositionCount;
  }

  @Override
  public ExtentArray extents() {
    return source.extents(context.document);
  }

  @Override
  public ExtentArray getData() {
    return extents();
  }
  
}
