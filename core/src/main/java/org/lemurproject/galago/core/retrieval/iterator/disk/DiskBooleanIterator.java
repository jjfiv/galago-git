// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.disk;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.index.source.BooleanSource;
import org.lemurproject.galago.core.retrieval.iterator.IndicatorIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;

/**
 *
 * @author jfoley
 */
public class DiskBooleanIterator extends SourceIterator implements IndicatorIterator {

  BooleanSource boolSrc;

  public DiskBooleanIterator(BooleanSource src) {
    super(src);
    this.boolSrc = src;
  }

  @Override
  public String getValueString(ScoringContext c) throws IOException {
    return String.format("%s:%d:%s", getKeyString(), currentCandidate(), indicator(c));
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "indicator";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c.document);
    String returnValue = Boolean.toString(indicator(c));
    List<AnnotatedNode> children = Collections.EMPTY_LIST;

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }

  @Override
  public boolean indicator(ScoringContext c) {
    return boolSrc.indicator(c.document);
  }
}
