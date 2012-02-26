/*
 * BSD License (http://lemurproject.org/galago-license)

 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import org.lemurproject.galago.core.index.disk.FieldIndexReader;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 * Abstract superclass for comparing fields to values given.
 * The values to match against may involve more than one (i.e. in
 * the between operator), so that extraction is not done here.
 * All navigation functions are taken care of here though.
 *
 * @author irmarc
 */
public abstract class FieldComparisonIterator extends AbstractIndicator {

  FieldIndexReader.ListIterator iterator;
  String format;
  String strValue;
  int intValue;
  long longValue;
  float floatValue;
  double doubleValue;
  long dateValue;

  public FieldComparisonIterator(NodeParameters p, FieldIndexReader.ListIterator fieldIterator) {
    super(p, new ValueIterator[]{fieldIterator});
    this.iterator = fieldIterator;
    this.format = fieldIterator.getFormat();
  }

  public void reset() throws IOException {
    iterator.reset();
  }

  public boolean isDone() {
    return iterator.isDone();
  }

  public int currentCandidate() {
    return iterator.currentCandidate();
  }
  
  public boolean atCandidate(int identifier) {
    return !isDone() && iterator.currentCandidate() == identifier;
  }

  public boolean hasAllCandidates(){
    return iterator.hasAllCandidates();
  }

  public boolean moveTo(int identifier) throws IOException {
    return iterator.moveTo(identifier);
  }

  public String getEntry() throws IOException {
    return iterator.getEntry();
  }

  public long totalEntries() {
    return iterator.totalEntries();
  }

  protected void parseField(NodeParameters p) {
    try {
      if (format.equals("string")) {
        strValue = p.getString("0");
      } else if (format.equals("int")) {
        intValue = Integer.parseInt(p.getString("0"));
      } else if (format.equals("long")) {
        longValue = Long.parseLong(p.getString("0"));
      } else if (format.equals("float")) {
        floatValue = Float.parseFloat(p.getString("0"));
      } else if (format.equals("double")) {
        doubleValue = Double.parseDouble(p.getString("0"));
      } else if (format.equals("date")) {
        DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT);
        dateValue = df.parse(p.getString("0")).getTime();
      } else {
        throw new RuntimeException(String.format("Don't have any plausible format for tag %s\n",
                format));
      }
    } catch (ParseException pe) {
      throw new RuntimeException(pe);
    }
  }
}
