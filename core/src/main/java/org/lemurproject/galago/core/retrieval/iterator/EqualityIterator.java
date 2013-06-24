/*
 * BSD License (http://lemurproject.org/galago-license)

 */
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.index.disk.FieldIndexReader;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 *
 * @author irmarc
 */
public class EqualityIterator extends FieldComparisonIterator {

  public EqualityIterator(NodeParameters p, FieldIndexReader.ListIterator fieldIterator) {
    super(p, fieldIterator);
    parseField(p);
  }

  @Override
  public boolean indicator(ScoringContext c) {
    if (currentCandidate() != c.document) {
      return false;
    } else if (format.equals("string")) {
      return (fieldIterator.stringValue().equals(strValue));
    } else if (format.equals("int")) {
      return (fieldIterator.intValue() == intValue);
    } else if (format.equals("long")) {
      return (fieldIterator.longValue() == longValue);
    } else if (format.equals("float")) {
      return (fieldIterator.floatValue() == floatValue);
    } else if (format.equals("double")) {
      return (fieldIterator.doubleValue() == doubleValue);
    } else if (format.equals("date")) {
      return (fieldIterator.dateValue() == dateValue);
    } else {
      throw new RuntimeException(String.format("Don't have any plausible format for tag %s\n",
              format));
    }
  }
}
