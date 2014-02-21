// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.json;

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
      }
    }

    if(isNumber) {
      if(hasDot) {
        return Double.parseDouble(value);
      } else {
        return Long.parseLong(value);
      }
    }

    return value;
  }
}
