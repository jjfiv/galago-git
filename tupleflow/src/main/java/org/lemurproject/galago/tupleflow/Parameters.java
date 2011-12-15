// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import gnu.trove.TObjectByteHashMap;
import gnu.trove.TObjectDoubleHashMap;
import gnu.trove.TObjectLongHashMap;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

/**
 *
 * @author irmarc
 */
public class Parameters implements Serializable {

  // Parsing in JSON
  private static class JSONParser {

    Reader reader;
    char delimiter = ' ';
    Type valueType;

    public JSONParser(Reader input) {
      this.reader = new BufferedReader(input);
    }

    public Parameters parse() throws IOException {
      skipWhitespace();
      if (delimiter != '{') {
        throw new IOException("Expected top-level JSON object definition (starting with '{'), got " + delimiter);
      }
      Parameters jp = parseParameters();
      skipWhitespace(); // eat any remaining whitespace
      // Need to catch output as an int in order to use the assert
      int last = reader.read();
      assert (last == -1); // Makes sure we got to the end
      reader.close();
      return jp;
    }

    private Parameters parseParameters() throws IOException {
      // Need to move past the opening '{' and find the next meaningful character
      delimiter = (char) reader.read();
      skipWhitespace();
      Parameters container = new Parameters();
      String key;
      Object value;
      while (delimiter != '}') {
        skipWhitespace();
        key = parseString();
        skipWhitespace();
        if (delimiter != ':') {
          throw new IOException("Expected ':' while parsing string-value. Got " + delimiter);
        } else {
          // Saw the colon - now advance
          delimiter = (char) reader.read();
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
          throw new IOException("Expected '}' or ',' while parsing map. Got " + delimiter);
        }
        if (delimiter == ',') {
          delimiter = (char) reader.read();
        }
      }

      // Advance past closing '}'
      delimiter = (char) reader.read();
      valueType = Type.MAP;
      return container;
    }

    private List parseList() throws IOException {
      // Have to move past the opening '['
      delimiter = (char) reader.read();
      ArrayList container = new ArrayList();
      while (delimiter != ']') {
        skipWhitespace();
        container.add(parseValue());
        skipWhitespace();
        if (delimiter != ',' && delimiter != ']') {
          throw new IOException("Expected ',' or ']', got " + delimiter);
        }

        // Advance if it's a comma
        if (delimiter == ',') {
          delimiter = (char) reader.read();
        }
      }

      // Advance past closing ']'
      delimiter = (char) reader.read();
      valueType = Type.LIST;
      return container;
    }

    // Will only move forward if the current delimiter is whitespace.
    // Otherwise has no effect
    //
    // TODO - support one-line comments here
    private void skipWhitespace() throws IOException {
      while (Character.isWhitespace(delimiter)) {
        delimiter = (char) reader.read();
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
          return parseParameters();
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
      throw new IOException(String.format("Expected leading character for value type. Got '%s'", delimiter));
    }

    // Lead character's done. Need to do 'rue'
    private boolean parseTrue() throws IOException {
      if (reader.read() == 'r' && reader.read() == 'u' && reader.read() == 'e') {
        delimiter = (char) reader.read();
        return true;
      } else {
        throw new IOException("Value led with 't', but was not a literal 'true' value.\n");
      }
    }

    // Lead character is 'n', need to finish w/ 'ull'
    private String parseNull() throws IOException {
      if (reader.read() == 'u' && reader.read() == 'l' && reader.read() == 'l') {
        delimiter = (char) reader.read();
        return null;
      } else {
        throw new IOException("Value led with 'n', but was not a literal 'null' value.\n");
      }
    }

    // Lead character's done. Need to do 'alse'
    private boolean parseFalse() throws IOException {
      if (reader.read() == 'a' && reader.read() == 'l'
              && reader.read() == 's' && reader.read() == 'e') {
        delimiter = (char) reader.read();
        return false;
      } else {
        throw new IOException("Value led with 'f', but was not a literal 'false' value.\n");
      }
    }

    // Parses a string - either a label or a value
    private String parseString() throws IOException {
      StringBuilder builder = new StringBuilder();
      char trail;
      int value;

      // Need to make sure we're on a '"'
      while (delimiter != '"') {
        value = reader.read();
        if (value == -1) {
          throw new IllegalArgumentException("Missing closing quote for string.");
        }
        delimiter = (char) value;
      }

      // Now reading string content
      delimiter = (char) reader.read();
      trail = ' ';
      while (!(delimiter == '"' && trail != '\\')) {
        builder.append(delimiter);
        trail = delimiter;
        value = reader.read();
        if (value == -1) {
          throw new IllegalArgumentException("Missing closing quote for string.");
        }
        delimiter = (char) value;
      }

      // Read first thing *after* the close quote
      delimiter = (char) reader.read();

      if (builder.length() == 0) {
        throw new IOException("Found a string of zero-length.");
      }
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
        value = reader.read();
        if (value == -1) {
          throw new IllegalArgumentException("File ended while reading in number.");
        }
        c = (char) value;
      } while (Character.isDigit(c));

      // If we see a dot, do that subroutine
      if (c == '.') {
        hasDot = true;
        builder.append(c);
        value = reader.read();
        if (value == -1) {
          throw new IllegalArgumentException("File ended while reading in number.");
        }
        c = (char) value;

        // better be a number
        if (!Character.isDigit(c)) {
          throw new IOException("Encountered invalid fractional character: " + c);
        }

        // Append that one and others
        while (Character.isDigit(c)) {
          builder.append(c);
          value = reader.read();
          if (value == -1) {
            throw new IllegalArgumentException("File ended while reading in number.");
          }
          c = (char) value;
        }
      }

      // if it's an e or E, cover that subroutine
      if (c == 'e' || c == 'E') {
        builder.append(c);
        c = (char) reader.read();

        // Better be valid
        if (c != '+' && c != '-' && !Character.isDigit(c)) {
          throw new IOException("Expected: '+', '-', or [0-9], got : " + c);
        }
        builder.append(c);

        // Read some (optional) digits
        c = (char) reader.read();
        while (Character.isDigit(c)) {
          builder.append(c);
          value = reader.read();
          if (value == -1) {
            throw new IllegalArgumentException("File ended while reading in number.");
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

// Constructor - we always start empty, and add to it
// Most of these are constructed statically
  public Parameters() {
    _keys = new HashMap<String, Type>();
    _longs = null;
    _bools = null;
    _doubles = null;
    _objects = null;
  }

  public Parameters(Map<String, String> map) {
    _objects = new HashMap();
    _keys = new HashMap<String, Type>();

    for (String key : map.keySet()) {
      String value = map.get(key);
      Type t = determineType(value);
      switch (t) {
        case BOOLEAN:
          set(key, Boolean.parseBoolean(value));
          break;
        case LONG:
          set(key, Long.parseLong(value));
          break;
        case DOUBLE:
          set(key, Double.parseDouble(value));
          break;
        default:
          set(key, value);
      }
    }
  }

  // in this case, we assume everything is a key-value pair of strings
  // TODO : Make this and the above constructor care about types.
  public Parameters(String[] args) throws IOException {
    _keys = new HashMap<String, Type>();

    for (String arg : args) {
      if (arg.startsWith("--")) {
        String pattern = arg.substring(2);
        tokenizeComplexValue(this, pattern);
      } else {
        // We assume that the input is a file of JSON parameters
        Parameters other = Parameters.parse(new File(arg));
        this.copyFrom(other);
      }
    }
  }

  private void tokenizeComplexValue(Parameters map, String pattern) throws IOException {
    int eqPos = pattern.indexOf('=') == -1 ? Integer.MAX_VALUE : pattern.indexOf('=');
    int arPos = pattern.indexOf('/') == -1 ? Integer.MAX_VALUE : pattern.indexOf('/');
    int plPos = pattern.indexOf('+') == -1 ? Integer.MAX_VALUE : pattern.indexOf('+');

    int smallest = (eqPos < arPos) ? (eqPos < plPos ? eqPos : plPos) : (arPos < plPos ? arPos : plPos);
    if (smallest == Integer.MAX_VALUE) {
      // Assume they meant 'true' for the key
      map.set(pattern, true);
    } else {
      if (eqPos == smallest) {
        tokenizeSimpleValue(map, pattern.substring(0, smallest), pattern.substring(smallest + 1, pattern.length()), false);
      } else if (plPos == smallest) {
        tokenizeSimpleValue(map, pattern.substring(0, smallest), pattern.substring(smallest + 1, pattern.length()), true);
      } else {
        String mapKey = pattern.substring(0, smallest);
        if (!map.isMap(mapKey)) {
          map.set(mapKey, new Parameters());
        }
        tokenizeComplexValue(map.getMap(mapKey), pattern.substring(smallest + 1, pattern.length()));
      }
    }
  }

  private void tokenizeSimpleValue(Parameters map, String key, String value, boolean isArray) throws IOException {
    Type t = determineType(value);
    switch (t) {
      case BOOLEAN: {
        boolean v = Boolean.parseBoolean(value);
        if (isArray) {
          if (!map.isList(key)) {
            map.set(key, new ArrayList<Boolean>());
          }
          map.getList(key).add(v);
        } else {
          map.set(key, v);
        }
      }
      break;
      case LONG: {
        long v = Long.parseLong(value);
        if (isArray) {
          if (!map.isList(key)) {
            map.set(key, new ArrayList<Long>());
          }
          map.getList(key).add(v);
        } else {
          map.set(key, v);
        }
      }
      break;
      case DOUBLE: {
        double v = Double.parseDouble(value);
        if (isArray) {
          if (!map.isList(key)) {
            map.set(key, new ArrayList<Double>());
          }
          map.getList(key).add(v);
        } else {
          map.set(key, v);
        }
      }
      break;
      default:
        if (isArray) {
          if (!map.isList(key)) {
            map.set(key, new ArrayList<String>());
          }
          map.getList(key).add(value);
        } else {
          map.set(key, value);
        }
    }
  }

  public static Parameters parse(String data) throws IOException {
    JSONParser jp = new JSONParser(new StringReader(data));
    Parameters p = jp.parse();
    return p;
  }

  public static Parameters parse(byte[] data) throws IOException {
    JSONParser jp = new JSONParser(new InputStreamReader(new ByteArrayInputStream(data)));
    Parameters p = jp.parse();
    return p;
  }

  public static Parameters parse(File f) throws IOException {
    JSONParser jp = new JSONParser(new FileReader(f));
    Parameters p = jp.parse();
    return p;
  }

  // I do not like this method -- how can we rewrite it so it doesn't suck?
  public void copyFrom(Parameters other) {
    if (other.isEmpty()) {
      return;
    }
    for (String key : other._keys.keySet()) {
      Type t = other._keys.get(key);
      switch (t) {
        case LONG:
          set(key, other._longs.get(key));
          break;
        case DOUBLE:
          set(key, other._doubles.get(key));
          break;
        case BOOLEAN:
          set(key, other.getBoolean(key));
          break;
        case STRING:
          set(key, (String) other._objects.get(key));
          break;
        case MAP:
          set(key, (Parameters) other._objects.get(key));
          break;
        case LIST:
          set(key, (List) other._objects.get(key));
          break;
      }
    }

  }

  public void copyTo(Parameters other) {
    for (String key : _keys.keySet()) {
      Type t = _keys.get(key);
      switch (t) {
        case LONG:
          other.set(key, _longs.get(key));
          break;
        case DOUBLE:
          other.set(key, _doubles.get(key));
          break;
        case BOOLEAN:
          other.set(key, getBoolean(key));
          break;
        case STRING:
          other.set(key, (String) _objects.get(key));
          break;
        case MAP:
          other.set(key, (Parameters) _objects.get(key));
          break;
        case LIST:
          other.set(key, (List) _objects.get(key));
          break;
      }
    }
  }

  public static Type determineType(String s) {
    if (Pattern.matches("true|false", s)) {
      return Type.BOOLEAN;
    }

    if (Pattern.matches("\\d+", s)) {
      return Type.LONG;
    }

    try {
      double d = Double.parseDouble(s);
      return Type.DOUBLE;
    } catch (Exception e) {
      // Do nothing - just didn't work
    }

    return Type.STRING;
  }

  @Override
  public Parameters clone() {
    Parameters p = new Parameters();
    this.copyTo(p);
    return p;
  }

  // Getters
  public Set<String> getKeys() {
    return _keys.keySet();
  }

  public List getList(String key) {
    return (List) _objects.get(key);
  }

  public List getAsList(String key) {
    if (isList(key)) {
      return getList(key);
    }

    if (_keys.containsKey(key)) {
      List tmp;
      switch (_keys.get(key)) {
        case LONG:
          tmp = new ArrayList<Long>();
          tmp.add(_longs.get(key));
          return tmp;
        case DOUBLE:
          tmp = new ArrayList<Double>();
          tmp.add(_doubles.get(key));
          return tmp;
        case BOOLEAN:
          tmp = new ArrayList<Boolean>();
          tmp.add(getBoolean(key));
          return tmp;
        case STRING:
          tmp = new ArrayList<String>();
          tmp.add(_objects.get(key));
          return tmp;
        case MAP:
          tmp = new ArrayList<Parameters>();
          tmp.add(_longs.get(key));
          return tmp;
      }
    } else {
      return new ArrayList();
    }
    return new ArrayList();
  }

  public Parameters getMap(String key) {
    return (Parameters) _objects.get(key);
  }

  public String getString(String key) {
    return (String) _objects.get(key);
  }

  public String get(String key, String def) {
    if (_keys.containsKey(key)) {
      return (String) _objects.get(key);
    } else {
      return def;
    }
  }

  public long getLong(String key) {
    return _longs.get(key);
  }

  public long get(String key, long def) {
    if (_keys.containsKey(key)) {
      return _longs.get(key);
    } else {
      return def;
    }
  }

  public double getDouble(String key) {
    return _doubles.get(key);
  }

  public double get(String key, double def) {
    if (_keys.containsKey(key)) {
      return _doubles.get(key);
    } else {
      return def;
    }
  }

  public boolean getBoolean(String key) {
    return (_bools.get(key) == 1 ? true : false);
  }

  public boolean get(String key, boolean def) {
    if (_keys.containsKey(key)) {
      return getBoolean(key);
    } else {
      return def;
    }
  }

  // Setters
  public void set(String key, Parameters value) {
    if (_keys.containsKey(key) && _keys.get(key) != Type.MAP) {
      throw new IllegalArgumentException(String.format("Tried to put key '%s' as JSONParameters, is %s\n",
              key, _keys.get(key)));
    }
    if (_objects == null) {
      _objects = new HashMap();
    }
    _objects.put(key, value);
    _keys.put(key, Type.MAP);
  }

  public void set(String key, Collection value) {
    if (List.class.isAssignableFrom(value.getClass())) {
      set(key, (List) value);
    } else {
      set(key, new ArrayList(value));
    }
  }

  public void set(String key, Object[] value) {
    set(key, Arrays.asList(value));
  }

  public void set(String key, List value) {
    if (_keys.containsKey(key) && _keys.get(key) != Type.LIST) {
      throw new IllegalArgumentException(String.format("Tried to put key '%s' as List, is %s\n",
              key, _keys.get(key)));
    }
    if (_objects == null) {
      _objects = new HashMap();
    }
    _objects.put(key, value);
    _keys.put(key, Type.LIST);
  }

  public void set(String key, String value) {
    if (_keys.containsKey(key) && _keys.get(key) != Type.STRING) {
      throw new IllegalArgumentException(String.format("Tried to put key '%s' as String, is %s\n",
              key, _keys.get(key)));
    }
    if (_objects == null) {
      _objects = new HashMap();
    }
    _objects.put(key, value);
    _keys.put(key, Type.STRING);
  }

  public void set(String key, long value) {
    if (_keys.containsKey(key) && _keys.get(key) != Type.LONG) {
      throw new IllegalArgumentException(String.format("Tried to put key '%s' as Long, is %s\n",
              key, _keys.get(key)));
    }
    if (_longs == null) {
      _longs = new TObjectLongHashMap();
    }
    _longs.put(key, value);
    _keys.put(key, Type.LONG);
  }

  public void set(String key, double value) {
    if (_keys.containsKey(key) && _keys.get(key) != Type.DOUBLE) {
      throw new IllegalArgumentException(String.format("Tried to put key '%s' as Double, is %s\n",
              key, _keys.get(key)));
    }
    if (_doubles == null) {
      _doubles = new TObjectDoubleHashMap();
    }
    _doubles.put(key, value);
    _keys.put(key, Type.DOUBLE);
  }

  public void set(String key, boolean value) {
    if (_keys.containsKey(key) && _keys.get(key) != Type.BOOLEAN) {
      throw new IllegalArgumentException(String.format("Tried to put key '%s' as Boolean, is %s\n",
              key, _keys.get(key)));
    }
    if (_bools == null) {
      _bools = new TObjectByteHashMap();
    }

    byte result = (byte) (value ? 0x1 : 0x0);
    _bools.put(key, result);
    _keys.put(key, Type.BOOLEAN);
  }

  // Deleter
  public boolean remove(String key) {
    if (!_keys.containsKey(key)) {
      return false;
    }
    Type t = _keys.get(key);
    switch (t) {
      case LIST:
      case MAP:
      case STRING:
        _objects.remove(key);
        break;
      case LONG:
        _longs.remove(key);
        break;
      case DOUBLE:
        _doubles.remove(key);
        break;
      case BOOLEAN:
        _bools.remove(key);
        break;
    }
    _keys.remove(key);
    return true;
  }

  // Verifiers
  public boolean isString(String key) {
    return (_keys.containsKey(key) && _keys.get(key) == Type.STRING);
  }

  public boolean isList(String key) {
    return (_keys.containsKey(key) && _keys.get(key) == Type.LIST);
  }

  public boolean isList(String key, Type type) {
    if (isList(key)) {
      List<Object> list = getList(key);
      // empty lists can store anything
      if (list.isEmpty()) {
        return true;
      }
      Object o = list.get(0);
      switch (type) {
        case MAP:
          if (Parameters.class.isAssignableFrom(o.getClass())) {
            return true;
          }
          break;
        case LIST:
          if (List.class.isAssignableFrom(o.getClass())) {
            return true;
          }
          break;
        case STRING:
          if (String.class.isAssignableFrom(o.getClass())) {
            return true;
          }
          break;
        case DOUBLE:
          if (Double.class.isAssignableFrom(o.getClass())) {
            return true;
          }
          break;
        case LONG:
          if (Long.class.isAssignableFrom(o.getClass())) {
            return true;
          }
          break;
        case BOOLEAN:
          if (Boolean.class.isAssignableFrom(o.getClass())) {
            return true;
          }
          break;
      }
    }
    return false;
  }

  public boolean isMap(String key) {
    return (_keys.containsKey(key) && _keys.get(key) == Type.MAP);
  }

  public boolean isDouble(String key) {
    return (_keys.containsKey(key) && _keys.get(key) == Type.DOUBLE);
  }

  public boolean isLong(String key) {
    return (_keys.containsKey(key) && _keys.get(key) == Type.LONG);
  }

  public boolean isBoolean(String key) {
    return (_keys.containsKey(key) && _keys.get(key) == Type.BOOLEAN);
  }

  public boolean containsKey(String key) {
    return _keys.containsKey(key);
  }

  public boolean isEmpty() {
    return (_keys.size() == 0);
  }

  public void write(String filename) throws IOException {
    FileWriter writer = new FileWriter(filename);
    writer.append(this.toString());
    writer.close();
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{ ");

    try {
      String[] keys = _keys.keySet().toArray(new String[0]);
      Arrays.sort(keys);
      for (int i = 0; i < keys.length; i++) {
        String key = keys[i];
        Type vt = _keys.get(key);
        builder.append("\"").append(key).append("\" : ");
        switch (vt) {
          case BOOLEAN:
            boolean b = _bools.get(key) == 0x1 ? true : false;
            builder.append(b);
            break;
          case LONG:
            builder.append(_longs.get(key));
            break;
          case DOUBLE:
            builder.append(_doubles.get(key));
            break;
          case STRING:
          case MAP:
          case LIST:
            builder.append(emitUnknownValue(_objects.get(key)));
            break;
        }

        if (i < (keys.length - 1)) {
          builder.append(" , ");
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

    builder.append(" }");
    return builder.toString();
  }

  private String emitUnknownValue(Object val) throws IOException {
    if (val == null) {
      return "null";
    } else if (List.class.isAssignableFrom(val.getClass())) {
      StringBuilder builder = new StringBuilder();
      builder.append("[ ");
      boolean first = true;
      for (Object v : ((List) val)) {
        if (first) {
          first = false;
        } else {
          builder.append(" , ");
        }

        builder.append(emitUnknownValue(v));
      }

      builder.append(" ]");
      return builder.toString();
    } else if (Parameters.class.isAssignableFrom(val.getClass())) {
      return val.toString();
    } else if (String.class.isAssignableFrom(val.getClass())) {
      return "\"" + val + "\"";
    } else {
      return val.toString();
    }
  }
  // Data structures available in the class
  // Tracks keys and their types
  private HashMap<String, Type> _keys;
  // Holds longs
  private TObjectLongHashMap<String> _longs;
  // holds bools as bytes - 0 == false, 1 == true
  private TObjectByteHashMap<String> _bools;
  // holds doubles
  private TObjectDoubleHashMap<String> _doubles;
  // holds Strings, maps, and lists - no interpretation is attempted.
  private HashMap _objects;
  // faster to use static types for parsing and lookup

  public enum Type {

    BOOLEAN, LONG, DOUBLE, STRING, MAP, LIST
  };
}
