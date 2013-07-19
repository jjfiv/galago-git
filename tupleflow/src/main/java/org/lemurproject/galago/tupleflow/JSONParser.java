// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

// Parsing in JSON
public class JSONParser {

  Reader reader;
  char delimiter = ' ';
  Parameters.Type valueType;
  int line = 1;
  int col = 0;

  public JSONParser(Reader input) {
    this.reader = new BufferedReader(input);
  }

  /**
   * Creates a new parameter object by inserting data from the reader
   */
  public Parameters parse() throws IOException {
    return parse(new Parameters());
  }

  /**
   * Common interface to read a character, this keeps track of position.
   *
   * @return
   * @throws IOException
   */
  private int getc() throws IOException {
    int val = reader.read();
    if ((char) val == '\n') {
      line++;
      col = 0;
    } else {
      col++;
    }
    return val;
  }

  /**
   * This is a common error routine for printing out the current location in the
   * file before printing the rest of the message.
   *
   * @param msg
   * @throws IOException
   */
  private void error(String msg) throws IOException {
    throw new IOException("At L:" + line + " C:" + col + ". " + msg);
  }

  /**
   * Adds to an existing parameter object by inserting data from the reader
   */
  public Parameters parse(Parameters jp) throws IOException {
    skipWhitespace();
    if (delimiter != '{') {
      error("Expected top-level JSON object definition (starting with '{'), got " + delimiter);
    }
    jp = parseParameters(jp);
    skipWhitespace(); // eat any remaining whitespace
    // Need to catch output as an int in order to use the assert
    int last = getc();
    assert (last == -1); // Makes sure we got to the end
    reader.close();
    return jp;
  }

  private Parameters parseParameters(Parameters container) throws IOException {
    // Need to move past the opening '{' and find the next meaningful character
    delimiter = (char) getc();
    skipWhitespace();
    String key;
    Object value;
    while (delimiter != '}') {
      skipWhitespace();
      key = parseString();
      skipWhitespace();
      if (delimiter != ':') {
        error("Expected ':' while parsing string-value. Got " + delimiter);
      } else {
        // Saw the colon - now advance
        delimiter = (char) getc();
      }
      skipWhitespace();
      value = parseValue();
      skipWhitespace();
      switch (valueType) {
        case MAP:
          container.set(key, (Parameters) value);
          break;
        case LIST:
          container.set(key, (List) value);
          break;
        case STRING:
          container.set(key, (String) value);
          break;
        case LONG:
          container.set(key, ((Long) value).longValue());
          break;
        case DOUBLE:
          container.set(key, ((Double) value).doubleValue());
          break;
        case BOOLEAN:
          container.set(key, ((Boolean) value).booleanValue());
          break;
      }
      if (delimiter != '}' && delimiter != ',') {
        error("Expected '}' or ',' while parsing map. Got " + delimiter);
      }
      if (delimiter == ',') {
        delimiter = (char) getc();
      }
    }
    // Advance past closing '}'
    delimiter = (char) getc();
    valueType = Parameters.Type.MAP;
    return container;
  }

  private List parseList() throws IOException {
    // Have to move past the opening '['
    delimiter = (char) getc();
    // skip any whitespace
    skipWhitespace();
    ArrayList container = new ArrayList();
    while (delimiter != ']') {
      skipWhitespace();
      container.add(parseValue());
      skipWhitespace();
      if (delimiter != ',' && delimiter != ']') {
        error("Expected ',' or ']', got " + delimiter);
      }
      // Advance if it's a comma
      if (delimiter == ',') {
        delimiter = (char) getc();
      }
    }
    // Advance past closing ']'
    delimiter = (char) getc();
    valueType = Parameters.Type.LIST;
    return container;
  }

  // Will only move forward if the current delimiter is whitespace.
  // Otherwise has no effect
  //
  // TODO - support one-line comments here
  private void skipWhitespace() throws IOException {
    while (Character.isWhitespace(delimiter)) {
      delimiter = (char) getc();
    }
  }

  // Need to parse to a general delimiter:
  // " leads a string
  // - or [0-9] leads a string
  // { leads a map
  // [ leads an array
  // t leads true
  // f leads false
  // n leads null (do we allow nulls? I guess to make is full JSON...)
  private Object parseValue() throws IOException {
    // Decide on character
    switch (delimiter) {
      case '[':
        valueType = Parameters.Type.LIST;
        return parseList();
      case '{':
        valueType = Parameters.Type.MAP;
        return parseParameters(new Parameters());
      case 't':
        valueType = Parameters.Type.BOOLEAN;
        return parseTrue();
      case 'f':
        valueType = Parameters.Type.BOOLEAN;
        return parseFalse();
      case 'n':
        valueType = Parameters.Type.STRING;
        return parseNull(); // hmm...
      case '"':
        valueType = Parameters.Type.STRING;
        return parseString();
      case '0':
      case '1':
      case '2':
      case '3':
      case '4':
      case '5':
      case '6':
      case '7':
      case '8':
      case '9':
      case '-':
        return parseNumber();
    }
    // If we make it here - problem
    error(String.format("Expected leading character for value type. Got '%s'", delimiter));
    return null;
  }

  // Lead character's done. Need to do 'rue'
  private boolean parseTrue() throws IOException {
    if (getc() == 'r' && getc() == 'u' && getc() == 'e') {
      delimiter = (char) getc();
    } else {
      error("Value led with 't', but was not a literal 'true' value.\n");
    }
    return true;
  }

  // Lead character is 'n', need to finish w/ 'ull'
  private String parseNull() throws IOException {
    if (getc() == 'u' && getc() == 'l' && getc() == 'l') {
      delimiter = (char) getc();
    } else {
      error("Value led with 'n', but was not a literal 'null' value.\n");
    }
    return null;
  }

  // Lead character's done. Need to do 'alse'
  private boolean parseFalse() throws IOException {
    if (getc() == 'a' && getc() == 'l' && getc() == 's' && getc() == 'e') {
      delimiter = (char) getc();
    } else {
      error("Value led with 'f', but was not a literal 'false' value.\n");
    }
    return false;
  }

  // Parses a string - either a label or a value
  private String parseString() throws IOException {
    StringBuilder builder = new StringBuilder();
    char trail;
    int value;
    // Need to make sure we're on a '"'
    while (delimiter != '"') {
      value = getc();
      if (value == -1) {
        error("Missing opening quote for string.");
      }
      delimiter = (char) value;
    }
    // Now reading string content
    delimiter = (char) getc();
    trail = ' ';
    while (true) {
      if (trail == '\\') {
        switch (delimiter) {
          case '"':
          case '\\':
          case '/':
          case 'b':
          case 'f':
          case 'n':
          case 'r':
          case 't':
            builder.append(delimiter);
            break;
          case 'u': {
            builder.append(delimiter);
            for (int i = 0; i < 4; i++) {
              delimiter = (char) getc();
              if ((delimiter >= 'a' && delimiter <= 'f') || (delimiter >= 'A' && delimiter <= 'F') || (delimiter >= '0' && delimiter <= '9')) {
                builder.append(delimiter);
              } else {
                error(String.format("Illegal hex character used: '%c'", delimiter));
              }
            }
          }
          break;
          default:
            error(String.format("Escape character followed by illegal character: '%c'", delimiter));
        }
        trail = ' '; // Don't put anything there b/c the current chars were escaped
      } else {
        if (delimiter == '"') {
          break;
        }
        builder.append(delimiter);
        trail = delimiter;
      }
      value = getc();
      if (value == -1) {
        error("Missing closing quote for string.");
      }
      delimiter = (char) value;
    }
    // Read first thing *after* the close quote
    delimiter = (char) getc();
    return builder.toString();
  }

  // Parses a number. Decides if it's a double on the fly
  private Object parseNumber() throws IOException {
    boolean hasDot = false;
    char c;
    int value;
    StringBuilder builder = new StringBuilder();
    c = delimiter;
    // Read more digits
    do {
      builder.append(c);
      value = getc();
      if (value == -1) {
        error("File ended while reading in number.");
      }
      c = (char) value;
    } while (Character.isDigit(c));
    // If we see a dot, do that subroutine
    if (c == '.') {
      hasDot = true;
      builder.append(c);
      value = getc();
      if (value == -1) {
        error("File ended while reading in number.");
      }
      c = (char) value;
      // better be a number
      if (!Character.isDigit(c)) {
        error("Encountered invalid fractional character: " + c);
      }
      // Append that one and others
      while (Character.isDigit(c)) {
        builder.append(c);
        value = getc();
        if (value == -1) {
          error("File ended while reading in number.");
        }
        c = (char) value;
      }
    }
    // if it's an e or E, cover that subroutine
    if (c == 'e' || c == 'E') {
      builder.append(c);
      c = (char) getc();
      // Better be valid
      if (c != '+' && c != '-' && !Character.isDigit(c)) {
        error("Expected: '+', '-', or [0-9], got : " + c);
      }
      builder.append(c);
      // Read some (optional) digits
      c = (char) getc();
      while (Character.isDigit(c)) {
        builder.append(c);
        value = getc();
        if (value == -1) {
          error("File ended while reading in number.");
        }
        c = (char) value;
      }
    }
    // Finally done, set that last character as the found delimiter,
    // and infer type. We don't need to advance b/c we stopped on the
    // non-digit delimiter.
    delimiter = c;
    if (hasDot) {
      valueType = Parameters.Type.DOUBLE;
      return Double.parseDouble(builder.toString());
    } else {
      valueType = Parameters.Type.LONG;
      return Long.parseLong(builder.toString());
    }
  }
}
