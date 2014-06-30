// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.utility.json;

import org.lemurproject.galago.utility.Parameters;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

// Parsing in JSON
public class JSONParser {

  private static enum Type {
    STRING, LONG, DOUBLE, MAP, BOOLEAN, LIST
  }

  Reader reader;
  char delimiter = ' ';
  Type valueType;

  // position
  public int line = 1;
  public int col = 0;
  public String fileName;

  public JSONParser(Reader input, String fileName) {
    this.fileName = fileName;
    this.reader = new BufferedReader(input);
  }

    /**
     * Creates a new parameter object by inserting data from the reader
     */
    public Parameters parse() throws IOException {
        return parse(Parameters.instance());
    }

  /**
   * Common interface to read a character, this keeps track of position.
   *
   * @return character code or -1 if EOF
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
   * @throws IOException
   */
  private void error(String msg) throws IOException {
    throw new IOException("F: "+fileName+" L:" + line + " C:" + col + ". " + msg);
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
                    container.set(key, (List<?>) value);
                    break;
                case STRING:
                    container.set(key, (String) value);
                    break;
                case LONG:
                    container.set(key, ((Long) value).longValue());
                    break;
                case DOUBLE:
                    container.set(key, (Double) value);
                    break;
                case BOOLEAN:
                    container.set(key, (Boolean) value);
                    break;
            }
            if (delimiter != '}' && delimiter != ',') {
                error("Expected '}' or ',' while parsing map. Got " + delimiter);
            }
            if (delimiter == ',') {
                delimiter = (char) getc();
                skipWhitespace();
            }
        }
        // Advance past closing '}'
        delimiter = (char) getc();
        valueType = Type.MAP;
        return container;
    }

    private List parseList() throws IOException {
        // Have to move past the opening '['
        delimiter = (char) getc();
        // skip any whitespace
        skipWhitespace();
        ArrayList<Object> container = new ArrayList<Object>();
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
                skipWhitespace();
            }
        }
        // Advance past closing ']'
        delimiter = (char) getc();
        valueType = Type.LIST;
        return container;
    }

  // Will only move forward if the current delimiter is whitespace.
  // Otherwise has no effect
  //
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
        valueType = Type.LIST;
        return parseList();
      case '{':
        valueType = Type.MAP;
        return parseParameters(Parameters.instance());
      case 't':
        valueType = Type.BOOLEAN;
        return parseTrue();
      case 'f':
        valueType = Type.BOOLEAN;
        return parseFalse();
      case 'n':
        valueType = Type.STRING;
        return parseNull(); // hmm...
      case '"':
        valueType = Type.STRING;
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
    while (true) {
      if(delimiter == '"') {
        break;
      }

      if(delimiter == -1) {
        error("Found EOF in the middle of a string!");
      }

      if(delimiter == '\\') {
        // prepare ye escape codes:
        int nextCode = getc();
        if(nextCode == -1) {
          error("EOF in escape sequence.");
        }
        char ch = (char) nextCode;
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
          for(int i=0; i<4; i++) {
            int x = getc();
            if(x == -1) { error("Unexpected EOF in unicode escape."); }
            char hch = Character.toLowerCase((char) x);
            if(!Character.isLetterOrDigit(x)) {
              error("Bad character in unicode escape.");
            }
            hex.append(hch);
          }
          int code = Integer.parseInt(hex.toString(), 16);
          builder.append((char) code);
        }
        else {
          error("Illegal escape sequence: \\"+ch);
        }
        delimiter = (char) getc();
        continue;
      }

      builder.append(delimiter);
      delimiter = (char) getc();
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
      valueType = Type.DOUBLE;
      return Double.parseDouble(builder.toString());
    } else {
      valueType = Type.LONG;
      return Long.parseLong(builder.toString());
    }
  }
}
