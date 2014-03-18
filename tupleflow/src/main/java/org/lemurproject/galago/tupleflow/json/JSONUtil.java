// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.json;

import java.util.List;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import org.lemurproject.galago.tupleflow.Parameters;

public class JSONUtil {
  public static Object parseString(String value) {
    if(value == null || value.equalsIgnoreCase("null")) {
      return null;
    } else if(value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
      return Boolean.parseBoolean(value.toLowerCase());
    }

    // recognize long or double
    boolean isNumber = false;
    boolean hasDot = false;
    for(int i=0; i<value.length(); i++) {
      char ch = value.charAt(i);
      if(i == 0 && (ch == '-' || ch == '+')) {
        continue;
      }
      if(Character.isDigit(ch)) {
        isNumber = true;
      } else if(ch == '.' && !hasDot) {
        hasDot = true;
      } else {
        isNumber = false;
        break;
      }
    }

    try {
      if(isNumber) {
        if(hasDot) {
          return Double.parseDouble(value);
        } else {
          return Long.parseLong(value);
        }
      }
    } catch (NumberFormatException nfe) {
      // we tried our best to make it a number, fall back to string
    }

    return value;
  }
  
  public static final XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
  public static void writeXML(Parameters p, XMLStreamWriter xml) throws XMLStreamException {
    for(String key : p.keySet()) {
      xml.writeStartElement(key);
      writeValue(p.get(key), xml);
      xml.writeEndElement(); //</key>
    }
  }

  private static void writeValue(Object v, XMLStreamWriter xml) throws XMLStreamException {
    if (v == null) {
      xml.writeCharacters("null");
    } else if (v instanceof Boolean) {
      xml.writeCharacters(Boolean.toString((Boolean) v));
    } else if (v instanceof Integer) {
      xml.writeCharacters(Integer.toString((Integer) v));
    } else if (v instanceof Long) {
      xml.writeCharacters(Long.toString((Long) v));
    } else if (v instanceof Float) {
            xml.writeCharacters(Float.toString((Float) v));
    } else if (v instanceof Double) {
            xml.writeCharacters(Double.toString((Double) v));
    } else if (v instanceof List) {
      for(Object item : (List) v) {
        xml.writeStartElement("item");
        writeValue(item, xml);
        xml.writeEndElement();
      }
    } else if (v instanceof Parameters) {
      writeXML((Parameters) v, xml);
    } else if (v instanceof String) {
      xml.writeCharacters((String) v);
    } else {
      throw new IllegalArgumentException("toXML(" + v + ") is not supported!");
    }
  }
}

