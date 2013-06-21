// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.tupleflow;

import gnu.trove.map.hash.TObjectByteHashMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 *
 * @author irmarc, sjh
 */
public class Parameters implements Serializable {

  private static final long serialVersionUID = 4553653651892088434L;
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
  // backoff parameters allow combination of several different parameter objects without copying
  // Highlander Principle applies: There can only be one backoff -- however, chaining is possible.
  private Parameters _backoff;

  public enum Type {

    BOOLEAN, LONG, DOUBLE, STRING, MAP, LIST
  };

  // Parsing in JSON
  private static class JSONParser {

    Reader reader;
    char delimiter = ' ';
    Type valueType;

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
     * Adds to an existing parameter object by inserting data from the reader
     */
    public Parameters parse(Parameters jp) throws IOException {
      skipWhitespace();
      if (delimiter != '{') {
        throw new IOException("Expected top-level JSON object definition (starting with '{'), got " + delimiter);
      }
      jp = parseParameters(jp);
      skipWhitespace(); // eat any remaining whitespace
      // Need to catch output as an int in order to use the assert
      int last = reader.read();
      assert (last == -1); // Makes sure we got to the end
      reader.close();
      return jp;
    }

    private Parameters parseParameters(Parameters container) throws IOException {
      // Need to move past the opening '{' and find the next meaningful character
      delimiter = (char) reader.read();
      skipWhitespace();
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
      // skip any whitespace
      skipWhitespace();
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
          return parseParameters(new Parameters());
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
          throw new IllegalArgumentException("Missing opening quote for string.");
        }
        delimiter = (char) value;
      }

      // Now reading string content
      delimiter = (char) reader.read();
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
                delimiter = (char) reader.read();
                if ((delimiter >= 'a' && delimiter <= 'f')
                        || (delimiter >= 'A' && delimiter <= 'F')
                        || (delimiter >= '0' && delimiter <= '9')) {
                  builder.append(delimiter);
                } else {
                  throw new IllegalArgumentException(String.format("Illegal hex character used: '%c'", delimiter));
                }
              }
            }
            break;
            default:
              throw new IllegalArgumentException(String.format("Escape character followed by illegal character: '%c'", delimiter));
          }
          trail = ' '; // Don't put anything there b/c the current chars were escaped
        } else {
          if (delimiter == '"') {
            break;
          }
          builder.append(delimiter);
          trail = delimiter;
        }
        value = reader.read();
        if (value == -1) {
          throw new IllegalArgumentException("Missing closing quote for string.");
        }
        delimiter = (char) value;
      }

      // Read first thing *after* the close quote
      delimiter = (char) reader.read();

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

    _backoff = null;
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

    _backoff = null;
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

    _backoff = null;
  }

  public static Parameters parse(String data) throws IOException {
    JSONParser jp = new JSONParser(new StringReader(data));
    Parameters p = jp.parse();
    return p;
  }

  public static Parameters parse(InputStream iStream) throws IOException {
    JSONParser jp = new JSONParser(new InputStreamReader(iStream));
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

  /**
   * Ensures items are not shared across parameter objects
   *  -- identical keys can be overwritten
   *  -- Backoff parameters are copied.
   */
  public void copyFrom(Parameters other) {
    try {
      JSONParser jp = new JSONParser(new StringReader(other.toString()));
      jp.parse(this);
    } catch (IOException ex) {
      Logger.getLogger(Parameters.class.getName()).log(Level.SEVERE, null, ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * To ensure items are not shared across parameter objects
   *  -- identical keys will be overwritten
   *  -- Backoff parameters are copied.
   */
  public void copyTo(Parameters other) {
    try {
      JSONParser jp = new JSONParser(new StringReader(toString()));
      jp.parse(other);
    } catch (IOException ex) {
      Logger.getLogger(Parameters.class.getName()).log(Level.SEVERE, null, ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Does NOT consider backoff parameters.
   */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Parameters)) {
      return false;
    }

    if (o == this) {
      return true;
    }

    // Otherwise determine how similar they are
    Parameters other = (Parameters) o;
    for (Map.Entry<String, Type> entry : _keys.entrySet()) {
      String key = entry.getKey();
      if (!other.containsKey(key)) {
        return false;
      }
      Type ot = other._keys.get(key);
      if (!entry.getValue().equals(ot)) {
        return false;
      }
      switch (ot) {
        case BOOLEAN:
          if (_bools.get(key) != other._bools.get(key)) {
            return false;
          }
          break;
        case LONG:
          if (_longs.get(key) != other._longs.get(key)) {
            return false;
          }
          break;
        case DOUBLE:
          if (_doubles.get(key) != other._doubles.get(key)) {
            return false;
          }
          break;
        case STRING:
        case MAP:
        case LIST:
          if (_objects.get(key).equals(other._objects.get(key)) == false) {
            return false;
          }
          break;
        default:
          throw new IllegalArgumentException("Key somehow has an illegal type: " + _keys.get(key));
      }
    }
    return true;
  }

  @Override
  public Parameters clone() {
    try {
      JSONParser jp = new JSONParser(new StringReader(toString()));
      Parameters p = jp.parse();
      p.setBackoff(_backoff);
      return p;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  // Getters
  public Set<String> getKeys() {
    if (_backoff != null) {
      // have to duplicate this list to get an AddAll function (immutable set)
      Set<String> keys = new HashSet(this._backoff.getKeys());
      keys.addAll(_keys.keySet());
      return keys;
    }
    return _keys.keySet();
  }

  public Type getKeyType(String key) {
    if (_keys.containsKey(key)) {
      return _keys.get(key);
    } else if (_backoff != null) {
      return _backoff.getKeyType(key);
    } else {
      throw new IllegalArgumentException("Key " + key + " does not exist in parameters object.");
    }
  }

  public List getList(String key) {
    if (_keys.containsKey(key)) {
      if (_keys.get(key).equals(Type.LIST)) {
        return (List) _objects.get(key);
      } else {
        throw new IllegalArgumentException("Key " + key + " does not exist as List in parameters object, found " + _keys.get(key));
      }
    } else if (_backoff != null) {
      return _backoff.getList(key);
    } else {
      throw new IllegalArgumentException("Key " + key + " does not exist in parameters object.");
    }
  }

  public List getAsList(String key) {
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
          tmp.add(_objects.get(key));
          return tmp;
        case LIST:
          return (List) _objects.get(key);
      }
    } else if (_backoff != null) {
      return _backoff.getAsList(key);
    } else {
      return new ArrayList();
    }
    return new ArrayList();
  }

  public Parameters getMap(String key) {
    if (_keys.containsKey(key)) {
      if (_keys.get(key).equals(Type.MAP)) {
        return (Parameters) _objects.get(key);
      } else {
        throw new IllegalArgumentException("Key " + key + " does not exist as JSONParameters in parameters object, instead found " + _keys.get(key));
      }
    } else if (_backoff != null) {
      return _backoff.getMap(key);
    } else {
      throw new IllegalArgumentException("Key " + key + " does not exist in parameters object.");
    }
  }

  public String getString(String key) {
    if (_keys.containsKey(key)) {
      if (_keys.get(key).equals(Type.STRING)) {
        return (String) _objects.get(key);
      } else {
        throw new IllegalArgumentException("Key " + key + " does not exist as String in parameters object, instead found " + _keys.get(key));
      }
    } else if (_backoff != null) {
      return _backoff.getString(key);
    } else {
      throw new IllegalArgumentException("Key " + key + " does not exist in parameters object.");
    }
  }

  public Parameters get(String key, Parameters def) {
      if (_keys.containsKey(key)) {
	  if (_keys.get(key).equals(Type.MAP)) {
	      return (Parameters) _objects.get(key);
	  } else {
	      throw new IllegalArgumentException("Key " + key + " does not exist as Map in parameters object, instead found " + _keys.get(key));
	  }
      } else {
	  return def;
      }
  }

  public String get(String key, String def) {
    if (_keys.containsKey(key)) {
      if (_keys.get(key).equals(Type.STRING)) {
        return (String) _objects.get(key);
      } else {
        throw new IllegalArgumentException("Key " + key + " does not exist as String in parameters object, instead found " + _keys.get(key));
      }
    } else if (_backoff != null) {
      return _backoff.get(key, def);
    } else {
      return def;
    }
  }

  public long getLong(String key) {
    if (_keys.containsKey(key)) {
      if (_keys.get(key).equals(Type.LONG)) {
        return _longs.get(key);
      } else {
        throw new IllegalArgumentException("Key " + key + " does not exist as Long in parameters object, instead found " + _keys.get(key));
      }
    } else if (_backoff != null) {
      return _backoff.getLong(key);
    } else {
      throw new IllegalArgumentException("Key " + key + " does not exist in parameters object.");
    }
  }

  public long get(String key, long def) {
    if (_keys.containsKey(key)) {
      if (_keys.get(key).equals(Type.LONG)) {
        return _longs.get(key);
      } else {
        throw new IllegalArgumentException("Key " + key + " does not exist as Long in parameters object, instead found " + _keys.get(key));
      }
    } else if (_backoff != null) {
      return _backoff.get(key, def);
    } else {
      return def;
    }
  }

  public double getDouble(String key) {
    if (_keys.containsKey(key)) {
      if (_keys.get(key).equals(Type.DOUBLE)) {
        return _doubles.get(key);

        // it is possible to cast a long to a double
      } else if (_keys.get(key).equals(Type.LONG)) {
        return _longs.get(key);

      } else {
        throw new IllegalArgumentException("Key " + key + " does not exist as Double/Long in parameters object, instead found " + _keys.get(key));
      }
    } else if (_backoff != null) {
      return _backoff.getDouble(key);
    } else {
      throw new IllegalArgumentException("Key " + key + " does not exist in parameters object.");
    }
  }

  public double get(String key, double def) {
    if (_keys.containsKey(key)) {
      if (_keys.get(key).equals(Type.DOUBLE)) {
        return _doubles.get(key);

        // it is possible to cast a long to a double
      } else if (_keys.get(key).equals(Type.LONG)) {
        return _longs.get(key);

      } else {
        throw new IllegalArgumentException("Key " + key + " does not exist as Double/Long in parameters object, instead found " + _keys.get(key));
      }

    } else if (_backoff != null) {
      return _backoff.get(key, def);
    } else {
      return def;
    }
  }

  public boolean getBoolean(String key) {
    if (_keys.containsKey(key)) {
      if (_keys.get(key).equals(Type.BOOLEAN)) {
        return _bools.get(key) == 1 ? true : false;
      } else {
        throw new IllegalArgumentException("Key " + key + " does not exist as Boolean in parameters object, instead found " + _keys.get(key));
      }
    } else if (_backoff != null) {
      return _backoff.getBoolean(key);
    } else {
      throw new IllegalArgumentException("Key " + key + " does not exist in parameters object.");
    }
  }

  public boolean get(String key, boolean def) {
    if (_keys.containsKey(key)) {
      if (_keys.get(key).equals(Type.BOOLEAN)) {
        return _bools.get(key) == 1 ? true : false;
      } else {
        throw new IllegalArgumentException("Key " + key + " does not exist as Boolean in parameters object, instead found " + _keys.get(key));
      }
    } else if (_backoff != null) {
      return _backoff.get(key, def);
    } else {
      return def;
    }
  }

  public Parameters getBackoff() {
    return _backoff;
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
    // using ArrayList to ensure mutability
    set(key, new ArrayList(Arrays.asList(value)));
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
      if (_keys.get(key) == Type.LONG) {
        // auto cast from long to double -- remove existing value.
        remove(key);
      } else {
        throw new IllegalArgumentException(String.format("Tried to put key '%s' as Double, is %s\n",
                key, _keys.get(key)));
      }
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

  /**
   * Overrides current backoff with this object.
   *
   * @param backoff
   */
  public void setBackoff(Parameters backoff) {
    this._backoff = backoff;
  }

  public void setFinalBackoff(Parameters backoff) {
    if(_backoff == null){
      this._backoff = backoff;
    } else {
      this._backoff.setFinalBackoff(backoff);
    }
  }

  /**
   * WARNING: does not delete keys from backoff
   *  -- manually getBackoff, and delete from there if necessary (?)
   */
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
    if (_keys.containsKey(key) && _keys.get(key) == Type.STRING) {
      return true;
    } else if (_backoff != null && _backoff.isString(key)) {
      return true;
    } else {
      return false;
    }
  }

  public boolean isList(String key) {
    if (_keys.containsKey(key) && _keys.get(key) == Type.LIST) {
      return true;
    } else if (_backoff != null && _backoff.isList(key)) {
      return true;
    } else {
      return false;
    }
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
    if (_keys.containsKey(key) && _keys.get(key) == Type.MAP) {
      return true;
    } else if (_backoff != null && _backoff.isMap(key)) {
      return true;
    } else {
      return false;
    }
  }

  public boolean isDouble(String key) {
    if (_keys.containsKey(key) && _keys.get(key) == Type.DOUBLE) {
      return true;
    } else if (_backoff != null && _backoff.isDouble(key)) {
      return true;
    } else {
      return false;
    }
  }

  public boolean isLong(String key) {
    if (_keys.containsKey(key) && _keys.get(key) == Type.LONG) {
      return true;
    } else if (_backoff != null && _backoff.isLong(key)) {
      return true;
    } else {
      return false;
    }
  }

  public boolean isBoolean(String key) {
    if (_keys.containsKey(key) && _keys.get(key) == Type.BOOLEAN) {
      return true;
    } else if (_backoff != null && _backoff.isBoolean(key)) {
      return true;
    } else {
      return false;
    }
  }

  public boolean containsKey(String key) {
    return ((_keys.containsKey(key)) || (_backoff != null && _backoff.containsKey(key)));
  }

  public boolean isEmpty() {
    return ((_keys.size() == 0) || (_backoff != null && _backoff.isEmpty()));
  }

  public void write(String filename) throws IOException {
    FileWriter writer = new FileWriter(filename);
    writer.append(this.toString());
    writer.close();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{ ");
    try {
      List<String> keys = new ArrayList(getKeys());
      Collections.sort(keys);
      for (int i = 0; i < keys.size(); i++) {
        String key = keys.get(i);
        Type vt = getKeyType(key);

        builder.append("\"").append(key).append("\" : ");
        Object val;
        switch (vt) {
          case BOOLEAN:
            boolean b = getBoolean(key);
            builder.append(b);
            break;
          case LONG:
            builder.append(getLong(key));
            break;
          case DOUBLE:
            builder.append(getDouble(key));
            break;
          case STRING:
            val = getString(key);
            builder.append(emitComplex(val));
            break;
          case MAP:
            val = getMap(key);
            builder.append(emitComplex(val));
            break;
          case LIST:
            val = getList(key);
            builder.append(emitComplex(val));
            break;
        }

        if (i < (keys.size() - 1)) {
          builder.append(" , ");
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

    builder.append(" }");
    return builder.toString();
  }

  public String toPrettyString() {
    return toPrettyString(this, "");
  }

  public String toPrettyString(String prefix) {
    return toPrettyString(this, prefix);
  }

  // PRIVATE FUNCTIONS

  private static String toPrettyString(Object val, String prefix) {
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

        builder.append(toPrettyString(v, prefix + "  "));
      }

      builder.append("]");
      return builder.toString();
    } else if (Parameters.class.isAssignableFrom(val.getClass())) {
      Parameters p = (Parameters) val;

      StringBuilder builder = new StringBuilder();
      builder.append(prefix).append("{\n");

      String internalPrefix = prefix + "  ";

      List<String> keys = new ArrayList(p.getKeys());
      Collections.sort(keys);
      for (int i = 0; i < keys.size(); i++) {
        String key = keys.get(i);
        Type vt = p.getKeyType(key);

        builder.append(internalPrefix).append("\"").append(key).append("\" : ");
        switch (vt) {
          case BOOLEAN:
            builder.append(p.getBoolean(key));
            break;
          case LONG:
            builder.append(p.getLong(key));
            break;
          case DOUBLE:
            builder.append(p.getDouble(key));
            break;
          case STRING:
            builder.append(toPrettyString(p.getString(key), internalPrefix));
            break;
          case MAP:
            builder.append(toPrettyString(p.getMap(key), internalPrefix));
            break;
          case LIST:
            builder.append(toPrettyString(p.getList(key), internalPrefix));
            break;
        }

        if (i < (keys.size() - 1)) {
          builder.append(",\n");
        }
      }

      builder.append("\n").append(prefix).append("}");
      return builder.toString();

    } else if (String.class.isAssignableFrom(val.getClass())) {
      return "\"" + val + "\"";

    } else {
      // Long, Double, Boolean
      return val.toString();
    }
  }

  private static String emitComplex(Object val) throws IOException {
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

        builder.append(emitComplex(v));
      }

      builder.append(" ]");
      return builder.toString();

    } else if (Parameters.class.isAssignableFrom(val.getClass())) {
      return val.toString();

    } else if (String.class.isAssignableFrom(val.getClass())) {
      return "\"" + val + "\"";

    } else {
      // long, double, boolean
      return val.toString();
    }
  }

  /**
   * Parsing functions :
   */
  private static Type determineType(String s) {
    if (Pattern.matches("true|false", s)) {
      return Type.BOOLEAN;
    }

    if (Pattern.matches("\\-?\\d+", s)) {
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

  private static void tokenizeComplexValue(Parameters map, String pattern) throws IOException {
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

  private static void tokenizeSimpleValue(Parameters map, String key, String value, boolean isArray) throws IOException {
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
          // attempt to clean a string: 'string'
          if (value.startsWith("'") && value.endsWith("'") && value.length() > 1) {
            value = value.substring(1, value.length() - 1);
          }
          map.set(key, value);
        }
    }
  }
}
