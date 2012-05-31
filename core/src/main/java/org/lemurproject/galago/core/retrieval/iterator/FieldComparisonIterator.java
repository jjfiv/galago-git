/*
 * BSD License (http://lemurproject.org/galago-license)

 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.index.disk.FieldIndexReader;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 * Abstract superclass for comparing fields to values given.
 * The values to match against may involve more than one (i.e. in
 * the between operator), so that extraction is not done here.
 * All navigation functions are taken care of here though.
 *
 * @author irmarc
 */
public abstract class FieldComparisonIterator extends TransformIterator implements MovableIndicatorIterator {
  NodeParameters p;
  FieldIndexReader.ListIterator fieldIterator;
  String format;
  String strValue;
  int intValue;
  long longValue;
  float floatValue;
  double doubleValue;
  long dateValue;

  public FieldComparisonIterator(NodeParameters p, FieldIndexReader.ListIterator fieldIterator) {
    super(fieldIterator);
    this.p = p;
    this.fieldIterator = fieldIterator;
    this.format = fieldIterator.getFormat();
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

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "indicator";
    String className = this.getClass().getSimpleName();
    String parameters = p.toString();
    int document = currentCandidate();
    boolean atCandidate = atCandidate(this.context.document);
    String returnValue = Boolean.toString( this.indicator( this.context.document ) );
    List<AnnotatedNode> children = Collections.singletonList( this.iterator.getAnnotatedNode() );
    
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
