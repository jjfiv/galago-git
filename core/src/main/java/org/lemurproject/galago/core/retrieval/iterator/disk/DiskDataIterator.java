// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.disk;

import org.lemurproject.galago.core.index.source.DataSource;
import org.lemurproject.galago.core.retrieval.iterator.DataIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author jfoley
 */
public class DiskDataIterator<DataType> extends SourceIterator implements DataIterator<DataType> {

  DataSource<DataType> dataSource;

  public DiskDataIterator(DataSource<DataType> src) {
    super(src);
    this.dataSource = src;
  }

  @Override
  public String getValueString(ScoringContext c) throws IOException {
    DataType dt = data(c);
    if (dt == null) {
      return "null-value";
    }
    return dt.toString();
  }

  @Override
  public DataType data(ScoringContext c) {
    return dataSource.data(c.document);
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "data";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c);
    String returnValue = getValueString(c);
    String extraInfo = data(c).toString();
    List<AnnotatedNode> children = Collections.emptyList();

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, extraInfo, children);
  }
}
