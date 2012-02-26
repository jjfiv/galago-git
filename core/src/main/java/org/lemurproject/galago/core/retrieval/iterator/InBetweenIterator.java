/*
 * BSD License (http://lemurproject.org/galago-license)

 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.text.ParseException;
import java.text.DateFormat;
import java.util.Date;
import org.lemurproject.galago.core.index.disk.FieldIndexReader;
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

  public boolean indicator(int identifier) {
    if (currentCandidate() != identifier) {
      return false;
    } else if (format.equals("string")) {
      return (iterator.stringValue().compareTo(strValue) > 0)
              && (iterator.stringValue().compareTo(strValue2) < 0);
    } else if (format.equals("int")) {
      return (iterator.intValue() > intValue)
              && (iterator.intValue() < intValue2);
    } else if (format.equals("long")) {
      return (iterator.longValue() > longValue)
              && (iterator.longValue() < longValue2);
    } else if (format.equals("float")) {
      return (iterator.floatValue() > floatValue)
              && (iterator.floatValue() < floatValue2);
    } else if (format.equals("double")) {
      return (iterator.doubleValue() > doubleValue)
              && (iterator.doubleValue() < doubleValue2);
    } else if (format.equals("date")) {
      return (iterator.dateValue() > dateValue)
              && (iterator.dateValue() < dateValue2);
    } else {
      throw new RuntimeException(String.format("Don't have any plausible format for tag %s\n",
              format));
    }
  }
}
