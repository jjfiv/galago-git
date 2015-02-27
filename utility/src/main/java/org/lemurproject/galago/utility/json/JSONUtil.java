// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.utility.json;

import org.lemurproject.galago.utility.Parameters;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.util.List;

public class JSONUtil {
  public static Object parseString(String value) {
    if(value == null || value.equalsIgnoreCase("null")) {
      return new Parameters.NullMarker();
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
    if (v instanceof Parameters.NullMarker) {
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

  public static String escape(String input) {
    StringBuilder output = new StringBuilder();

    for(int i=0; i<input.length(); i++) {
      char ch = input.charAt(i);
      int chx = (int) ch;

      // let's not put any nulls in our strings
      assert(chx != 0);

      if(ch == '\n') {
        output.append("\\n");
      } else if(ch == '\t') {
        output.append("\\t");
      } else if(ch == '\r') {
        output.append("\\r");
      } else if(ch == '\\') {
        output.append("\\\\");
      } else if(ch == '"') {
        output.append("\\\"");
      } else if(ch == '\b') {
        output.append("\\b");
      } else if(ch == '\f') {
        output.append("\\f");
      } else if(chx >= 0x10000) {
        assert false : "Java stores as u16, so it should never give us a character that's bigger than 2 bytes. It literally can't.";
      } else if(chx > 127) {
        output.append(String.format("\\u%04x", chx));
      } else {
        output.append(ch);
      }
    }

    return output.toString();
  }

  public static String unescape(String input) {
    StringBuilder builder = new StringBuilder();

    int i = 0;
    while (i < input.length()) {
      char delimiter = input.charAt(i); i++; // consume letter or backslash

      if(delimiter == '\\' && i < input.length()) {

        // consume first after backslash
        char ch = input.charAt(i); i++;

        if(ch == '\\' || ch == '/' || ch == '"' || ch == '\'') {
          builder.append(ch);
        }
        else if(ch == 'n') builder.append('\n');
        else if(ch == 'r') builder.append('\r');
        else if(ch == 't') builder.append('\t');
        else if(ch == 'b') builder.append('\b');
        else if(ch == 'f') builder.append('\f');
        else if(ch == 'u') {

          StringBuilder hex = new StringBuilder();

          // expect 4 digits
          if (i+4 > input.length()) {
            throw new RuntimeException("Not enough unicode digits! ");
          }
          for (char x : input.substring(i, i + 4).toCharArray()) {
            if(!Character.isLetterOrDigit(x)) {
              throw new RuntimeException("Bad character in unicode escape.");
            }
            hex.append(Character.toLowerCase(x));
          }
          i+=4; // consume those four digits.

          int code = Integer.parseInt(hex.toString(), 16);
          builder.append((char) code);
        } else {
          throw new RuntimeException("Illegal escape sequence: \\"+ch);
        }
      } else { // it's not a backslash, or it's the last character.
        builder.append(delimiter);
      }
    }

    return builder.toString();
  }
}

