/*
 * BSD License (http://lemurproject.org/galago-license)

 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.text.ParseException;
import java.text.DateFormat;
import org.lemurproject.galago.core.index.disk.FieldIndexReader;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 *
 * @author irmarc
 */
public class InBetweenIterator extends FieldComparisonIterator {

  String strValue2;
  int intValue2;
  long longValue2;
  float floatValue2;
  double doubleValue2;
  long dateValue2;

  public InBetweenIterator(NodeParameters p, FieldIndexReader.ListIterator fieldIterator) {
    super(p, fieldIterator);
    parseField(p);
  }

  @Override
  protected void parseField(NodeParameters p) {
    try {
      if (format.equals("string")) {
        strValue = p.getString("0");
        strValue2 = p.getString("1");
      } else if (format.equals("int")) {
        intValue = Integer.parseInt(p.getString("0"));
        intValue2 = Integer.parseInt(p.getString("1"));
      } else if (format.equals("long")) {
        longValue = Long.parseLong(p.getString("0"));
        longValue2 = Long.parseLong(p.getString("1"));
      } else if (format.equals("float")) {
        floatValue = Float.parseFloat(p.getString("0"));
        floatValue2 = Float.parseFloat(p.getString("1"));
      } else if (format.equals("double")) {
        doubleValue = Double.parseDouble(p.getString("0"));
        doubleValue2 = Double.parseDouble(p.getString("1"));
      } else if (format.equals("date")) {
        DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT);
        dateValue = df.parse(p.getString("0")).getTime();
        dateValue2 = df.parse(p.getString("1")).getTime();
      } else {
        throw new RuntimeException(String.format("Don't have any plausible format for tag %s\n",
                format));
      }
    } catch (ParseException pe) {
      throw new RuntimeException(pe);
    }
  }

  @Override
  public boolean indicator(ScoringContext c) {
    if (currentCandidate() != c.document) {
      return false;
    } else if (format.equals("string")) {
      return (fieldIterator.stringValue(c).compareTo(strValue) > 0)
              && (fieldIterator.stringValue(c).compareTo(strValue2) < 0);
    } else if (format.equals("int")) {
      return (fieldIterator.intValue(c) > intValue)
              && (fieldIterator.intValue(c) < intValue2);
    } else if (format.equals("long")) {
      return (fieldIterator.longValue(c) > longValue)
              && (fieldIterator.longValue(c) < longValue2);
    } else if (format.equals("float")) {
      return (fieldIterator.floatValue(c) > floatValue)
              && (fieldIterator.floatValue(c) < floatValue2);
    } else if (format.equals("double")) {
      return (fieldIterator.doubleValue(c) > doubleValue)
              && (fieldIterator.doubleValue(c) < doubleValue2);
    } else if (format.equals("date")) {
      return (fieldIterator.dateValue(c) > dateValue)
              && (fieldIterator.dateValue(c) < dateValue2);
    } else {
      throw new RuntimeException(String.format("Don't have any plausible format for tag %s\n",
              format));
    }
  }
}
