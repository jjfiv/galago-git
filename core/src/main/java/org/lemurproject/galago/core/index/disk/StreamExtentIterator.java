// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.index.BTreeReader.BTreeIterator;
import org.lemurproject.galago.core.index.stats.NodeAggregateIterator;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.ExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.SourceIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author jfoley
 */
public class StreamExtentIterator extends SourceIterator<StreamExtentSource> implements NodeAggregateIterator, CountIterator, ExtentIterator {

  StreamExtentIterator(BTreeIterator it) throws IOException {
    super(new StreamExtentSource(it));
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
    List<AnnotatedNode> children = Collections.EMPTY_LIST;
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
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
    if(!source.isDone() && context.document == currentCandidate()) {
      return (int) source.count(context.document);
    }
    return 0;
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
