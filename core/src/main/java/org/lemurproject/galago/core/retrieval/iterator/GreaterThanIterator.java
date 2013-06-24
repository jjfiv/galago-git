/*
 * BSD License (http://lemurproject.org/galago-license)

 */
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.index.disk.FieldIndexReader;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 *
 * @author irmarc
 */
public class GreaterThanIterator extends FieldComparisonIterator {

  public GreaterThanIterator(NodeParameters p, FieldIndexReader.ListIterator fieldIterator) {
    super(p, fieldIterator);
    parseField(p);
  }

  public boolean indicator(long identifier) {
    if (currentCandidate() != identifier) {
      return false;
    } else if (format.equals("string")) {
      return (fieldIterator.stringValue().compareTo(strValue) > 0);
    } else if (format.equals("int")) {
      return (fieldIterator.intValue() > intValue);
    } else if (format.equals("long")) {
      return (fieldIterator.longValue() > longValue);
    } else if (format.equals("float")) {
      return (fieldIterator.floatValue() > floatValue);
    } else if (format.equals("double")) {
      return (fieldIterator.doubleValue() > doubleValue);
    } else if (format.equals("date")) {
      return (fieldIterator.dateValue() > dateValue);
    } else {
      throw new RuntimeException(String.format("Don't have any plausible format for tag %s\n",
              format));
    }
  }
}
