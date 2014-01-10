// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.tupleflow;

import org.lemurproject.galago.tupleflow.config.JSONParser;
import gnu.trove.map.hash.TObjectByteHashMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;
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
import java.util.AbstractMap;
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
public class Parameters implements Serializable, Map<String,Object> {

  private static final long serialVersionUID = 4553653651892088435L;
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

// Constructor - we always start empty, and add to it
// Most of these are constructed statically
  public Parameters() {
    clear();
  }
  
  public static Parameters parseMap(Map<String,String> map) {
    Parameters self = new Parameters();
    
    for (String key : map.keySet()) {
      String value = map.get(key);
      Type t = determineType(value);
      switch (t) {
        case BOOLEAN:
          self.set(key, Boolean.parseBoolean(value));
          break;
        case LONG:
          self.set(key, Long.parseLong(value));
          break;
        case DOUBLE:
          self.set(key, Double.parseDouble(value));
          break;
        default:
          self.set(key, value);
      }
    }
    
    return self;
  }
  
  public static Parameters parseArgs(String[] args) throws IOException {
    Parameters self = new Parameters();
    
    for (String arg : args) {
      if (arg.startsWith("--")) {
        String pattern = arg.substring(2);
        tokenizeComplexValue(self, pattern);
      } else {
        // We assume that the input is a file of JSON parameters
        Parameters other = Parameters.parseFile(new File(arg));
        self.copyFrom(other);
      }
    }
    
    return self;
  }
  
  public static Parameters parseFile(File f) throws IOException {
    JSONParser jp = new JSONParser(new FileReader(f), f.getPath());
    return jp.parse();
  }
  
  public static Parameters parseFile(String path) throws IOException {
    return parseFile(new File(path));
  }
  
  public static Parameters parseString(String data) throws IOException {
    JSONParser jp = new JSONParser(new StringReader(data), "<from string>");
    return jp.parse();
  }
  
  public static Parameters parseReader(Reader reader) throws IOException {
    JSONParser jp = new JSONParser(reader, "<from reader>");
    return jp.parse();
  }
  
  public static Parameters parseStream(InputStream iStream) throws IOException {
    JSONParser jp = new JSONParser(new InputStreamReader(iStream), "<from stream>");
    Parameters p = jp.parse();
    return p;
  }
  
  public static Parameters parseBytes(byte[] data) throws IOException {
    return parseStream(new ByteArrayInputStream(data));
  }

  /**
   * Ensures items are not shared across parameter objects -- identical keys can
   * be overwritten -- Backoff parameters are copied.
   */
  public void copyFrom(Parameters other) {
    try {
      JSONParser jp = new JSONParser(new StringReader(other.toString()), "<copyFrom>");
      jp.parse(this);
    } catch (IOException ex) {
      Logger.getLogger(Parameters.class.getName()).log(Level.SEVERE, null, ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * To ensure items are not shared across parameter objects -- identical keys
   * will be overwritten -- Backoff parameters are copied.
   */
  public void copyTo(Parameters other) {
    try {
      JSONParser jp = new JSONParser(new StringReader(toString()), "<copyTo>");
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
  
  private Object copyValue(Object input) {
    if(input == null) {
      return input;
    } else if(input instanceof List) {
      ArrayList newl = new ArrayList();      
      for(Object o : (List) input) {
        newl.add(copyValue(o));
      }
      return newl;
    } else if(input instanceof Parameters) {
      return ((Parameters) input).clone();
    } else if(input instanceof Long || input instanceof Double || input instanceof String) {
      return input;
    } else {
      System.err.println("Warning: copy by reference on unknown object-kind: "+input);
      return input;
    }
  }
  
  @Override
  public Parameters clone() {
    Parameters copy = new Parameters();
    // use secret keySet to not copy backoff keys
    for(String key : _keys.keySet()) {
      if(isLong(key)) {
        copy.set(key, getLong(key));
      } else if(isDouble(key)) {
        copy.set(key, getDouble(key));
      } else if(isBoolean(key)) {
        copy.set(key, getBoolean(key));
      } else if(isString(key)) {
        copy.set(key, getString(key));
      } else if(isList(key)) {
        copy.set(key, (List) copyValue(getList(key)));
      } else if(isMap(key)) {
        copy.set(key, (Parameters) copyValue(getMap(key)));
      }
    }
    copy.setBackoff(_backoff);
    return copy;
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
  
  /**
   * If the value is primitive, this reaches into the Parameters object and returns "key" as a string.
   * @param key The key to look for.
   * @return the value of key as a string.
   */
  public String getAsString(String key) {
    if(isString(key)) {
      return getString(key);
    } else if(isLong(key)) {
      return Long.toString(getLong(key));
    } else if(isBoolean(key)) {
      return Boolean.toString(getBoolean(key));
    } else if(isDouble(key)) {
      return Double.toString(getDouble(key));
    }
    throw new IllegalArgumentException("Key "+ key +" does not exist as a primitive in parameters object.");
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
    assert(backoff != this);
    this._backoff = backoff;
  }

  public void setFinalBackoff(Parameters backoff) {
    if (_backoff == null) {
      this._backoff = backoff;
    } else {
      this._backoff.setFinalBackoff(backoff);
    }
  }

  /**
   * WARNING: does not delete keys from backoff -- manually getBackoff, and
   * delete from there if necessary (?)
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
  
  @Override
  public int size() {
    return _keys.size();
  }

  @Override
  public boolean containsKey(Object o) {
    return _keys.containsKey(o);
  }

  @Override
  public boolean containsValue(Object o) {
    if(o instanceof Integer) {
      long l = ((Integer) o).longValue();
      return (_longs != null && _longs.containsValue(l));
    } else if(o instanceof Long) {
      long l = ((Long) o).longValue();
      return (_longs != null && _longs.containsValue(l));
    } else if(o instanceof Boolean) {
      byte b = (byte) (((Boolean) o).booleanValue() ? 0x1 : 0x0);
      return (_bools != null && _bools.containsValue(b));
    } else if(o instanceof Float) {
      double d = ((Float) o).doubleValue();
      return (_doubles != null && _doubles.containsValue(d));
    } else if(o instanceof Double) {
      double d = ((Double) o).doubleValue();
      return (_doubles != null && _doubles.containsValue(d));
    }
    // strings, maps, lists:
    return _objects.containsValue(o);
  }

  @Override
  public Object get(Object o) {
    if(!containsKey(o) || !(o instanceof String))
      return null;
    
    String key = (String) o;
    Type ot = _keys.get(o);
    switch(ot) {
      case BOOLEAN:
        return getBoolean(key);
      case LONG:
        return getLong(key);
      case DOUBLE:
        return getDouble(key);
      default:
      case STRING:
      case MAP:
      case LIST:
        return _objects.get(key);
    }
  }

  @Override
  public Object put(String k, Object v) {
    if(v instanceof Integer) {
      long l = ((Integer) v).longValue();
      set(k, l);
    } else if(v instanceof Long) {
      long l = ((Long) v).longValue();
      set(k, l);
    } else if(v instanceof Boolean) {
      byte b = (byte) (((Boolean) v).booleanValue() ? 0x1 : 0x0);
      set(k, b);
    } else if(v instanceof Float) {
      double d = ((Float) v).doubleValue();
      set(k, d);
    } else if(v instanceof Double) {
      double d = ((Double) v).doubleValue();
      set(k,d);
    } else if(v instanceof List) {
      set(k, (List) v);
    } else if(v instanceof Parameters) {
      set(k, (Parameters) v);
    } else if(v instanceof String) {
      set(k, (String) v);
    } else {
      throw new IllegalArgumentException("put("+k+","+v+") is not supported!");
    }
    
    return v;
  }

  @Override
  public Object remove(Object o) {
    // this is optional per the javadoc of java.util.Map
    throw new UnsupportedOperationException("Not supported because of backoff!");
  }

  @Override
  public void putAll(Map<? extends String, ? extends Object> map) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void clear() {
    _keys = new HashMap<String, Type>();
    _longs = null;
    _bools = null;
    _doubles = null;
    _objects = null;

    _backoff = null;
  }

  @Override
  public Set<String> keySet() {
    if (_backoff != null) {
      HashSet<String> all = new HashSet<String>();
      all.addAll(_backoff.keySet());
      all.addAll(_keys.keySet());
      return all;
    } else {
      return _keys.keySet();
    }
  }

  @Override
  public Collection<Object> values() {
    ArrayList<Object> vals = new ArrayList<Object>();
    for(String key : keySet()) {
      vals.add(get(key));
    }
    return vals;
  }

  @Override
  public Set<Entry<String, Object>> entrySet() {
    HashSet<Entry<String,Object>> entries = new HashSet<Entry<String,Object>>();
    for(String key : keySet()) {
      entries.add(new AbstractMap.SimpleImmutableEntry<String,Object>(key, get(key)));
    }
    return entries;
  }

}
