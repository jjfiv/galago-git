// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.utility;

import org.lemurproject.galago.utility.json.JSONParser;
import org.lemurproject.galago.utility.json.JSONUtil;
import org.lemurproject.galago.utility.tools.Arguments;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author irmarc, sjh
 */
public class Parameters implements Serializable, Map<String,Object> {
  /** Marker object for null literals in JSON, to distinguish from "missing" */
  public static final class NullMarker {
    @Override public boolean equals(Object o) { return o instanceof NullMarker; }
    @Override public int hashCode() { throw new UnsupportedOperationException("hashCode in NullMarker"); }
    @Override public String toString() { return "null"; }
  }

  private static final long serialVersionUID = 4553653651892088435L;

  private Map<String,Object> _data;

  private Parameters _backoff;

  /** Constructor - we always start empty, and add to it. Most of these are constructed statically. */
  private Parameters() {
    clear();
  }
  private Parameters(Map<String, Object> data) {
    _data = data;
    _backoff = null;
  }

  public static Parameters create() {
    return new Parameters();
  }
  /** @deprecated use create instead! */
  @Deprecated
  public static Parameters instance() {
    return create();
  }

  public static Parameters parseMap(Map<String,String> map) {
    Parameters self = Parameters.create();
    
    for (String key : map.keySet()) {
      self.put(key, JSONUtil.parseString(map.get(key)));
    }
    
    return self;
  }

  @SuppressWarnings("unchecked")
  public static <T> Parameters wrap(Map<String, T> data) {
    return new Parameters((Map<String, Object>) data);
  }

	public static Parameters parseFile(File f) throws IOException {
    JSONParser jp = new JSONParser(new InputStreamReader(StreamCreator.openInputStream(f)), f.getPath());
    return jp.parse();
  }
  
  public static Parameters parseFile(String path) throws IOException {
    return parseFile(new File(path));
  }
  
  public static Parameters parseString(String data) throws IOException {
    JSONParser jp = new JSONParser(new StringReader(data), "<from string>");
    return jp.parse();
  }
  
  public static Parameters parseStringOrDie(String data) {
    try {
      JSONParser jp = new JSONParser(new StringReader(data), "<from string>");
      return jp.parse();
    } catch (IOException err) {
      throw new RuntimeException(err);
    }
  }

  public static Parameters parseReader(Reader reader) throws IOException {
    JSONParser jp = new JSONParser(reader, "<from reader>");
    return jp.parse();
  }
  
  public static Parameters parseStream(InputStream iStream) throws IOException {
    JSONParser jp = new JSONParser(new InputStreamReader(iStream), "<from stream>");
    return jp.parse();
  }
  
  public static Parameters parseBytes(byte[] data) throws IOException {
    return parseStream(new ByteArrayInputStream(data));
  }

  public static Parameters parseArray(Object... args) {
    if(args.length % 2 == 1) {
      throw new IllegalArgumentException("Uneven number of parameters in vararg constructor.");
    }
    Parameters result = Parameters.create();
    for(int i=0; i<args.length; i+=2) {
      Object key = args[i];
      Object value = args[i+1];
      if(!(key instanceof String)) {
        throw new IllegalArgumentException("Expected strings as keys; got: "+key);
      }
      result.put((String) key, value);
    }
    return result;
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
   * Recursively compute a reasonable hashCode for Parameter objects. Now they can be keys in a map.
   * @return a hash code.
   */
  @Override
  public int hashCode() {
    int hashCode = 0xdeadbeef;
    hashCode ^= this._data.keySet().hashCode();
    for (String key : this._data.keySet()) {
      hashCode ^= this._data.get(key).hashCode();
    }
    return hashCode;
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
    for (String key : keySet()) {
      if (!other.containsKey(key)) {
        return false;
      }

      Object val = get(key);
      Object oval = other.get(key);

      // if either is null, they should both be
      if(val == null || oval == null) {
        if(val != oval) return false;
      } else if(!val.equals(oval)) {
        return false;
      }
    }
    return true;
  }
  
  private Object copyValue(Object input) {
    if(input == null) {
      throw new IllegalArgumentException("null given to copyValue()");
    } else if(input instanceof List) {
      ArrayList<Object> newl = new ArrayList<>();
      for(Object o : (List) input) {
        newl.add(copyValue(o));
      }
      return newl;
    } else if(input instanceof Parameters) {
      return ((Parameters) input).clone();
    } else if(input instanceof Long || input instanceof Double || input instanceof String || input instanceof Boolean || input instanceof NullMarker) {
      return input;
    } else {
      System.err.println("Warning: copy by reference on unknown object-kind: "+input);
      return input;
    }
  }
  
	@Override
  public Parameters clone() {
    Parameters copy = Parameters.create();
    // use secret keySet to not copy backoff keys
    for(String key : _data.keySet()) {
      if(isLong(key)) {
        copy.set(key, getLong(key));
      } else if(isDouble(key)) {
        copy.set(key, getDouble(key));
      } else if(isBoolean(key)) {
        copy.set(key, getBoolean(key));
      } else if(isString(key)) {
        copy.set(key, getString(key));
      } else if(isList(key) || isMap(key)) {
        copy.put(key, copyValue(get(key)));
      }
    }
    copy.setBackoff(_backoff);
    return copy;
  }

  // Getters
  public Set<String> getKeys() {
    if (_backoff != null) {
      // have to duplicate this list to get an AddAll function (immutable set)
      Set<String> keys = new HashSet<>(this._backoff.getKeys());
      keys.addAll(_data.keySet());
      return keys;
    }
    return _data.keySet();
  }

  public <T> List<T> getList(String key, Class<T> klazz) {
    assert(klazz != null);
    return (List<T>) getList(key);
  }

  public List getList(String key) {
    Object val = getOrThrow(key);
    if (val instanceof List) {
      return (List) val;
    } else {
      throw new IllegalArgumentException("Key " + key + " does not exist as List in parameters object, found " + get(key));
    }
  }

  public List getAsList(String key) {
    Object val = get(key);
    if (val == null || val instanceof NullMarker) {
      return Collections.EMPTY_LIST;
    } else if (val instanceof List) {
      return (List) val;
    } else {
      return Arrays.asList(val);
    }
  }

  public <T> List<T> getAsList(String key, Class<T> ignored) {
    return (List<T>) getAsList(key);
  }

  public Parameters getMap(String key) {
    Object val = getOrThrow(key);
    if(val instanceof Parameters) {
      return (Parameters) val;
    } else {
      throw new IllegalArgumentException("Key " + key + " does not exist as JSONParameters in parameters object, instead found " + val);
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

  /**
   * May return null!
   * @param key key object
   * @return note that string values may be null
   */
  public String getString(String key) {
    Object val = get(key);
    if(val instanceof NullMarker) {
      return null;
    } else if(val instanceof String) {
      return (String) val;
    } else {
      throw new IllegalArgumentException("Key " + key + " does not exist as String in parameters object, instead found " + val);
    }
  }

  public Parameters get(String key, Parameters def) {
    Object val = get(key);
    if(val == null) return def;
    return getMap(key);
  }

  public String get(String key, String def) {
    Object val = get(key);
    if(val == null) return def;
    return getString(key);
  }

  public long getLong(String key) {
    Object val = getOrThrow(key);
    if(val instanceof Long) {
      return (Long) val;
    } else if(val instanceof Integer) {
      return (Integer) val;
    } else {
      throw new IllegalArgumentException("Key " + key + " does not exist as Long in parameters object, instead found " + val);
    }
  }

  public int getInt(String key) {
    Object val = getOrThrow(key);
    if(val instanceof Long) {
      long x = (Long) val;
      if(x > Integer.MAX_VALUE || x < Integer.MIN_VALUE) {
        throw new IllegalArgumentException("Couldn't get "+key+"="+x+" as an integer, it's too big!");
      }
      return (int) x;
    } else if(val instanceof Integer) {
      return (Integer) val;
    } else {
      throw new IllegalArgumentException("Key " + key + " does not exist as an integer in parameters object, instead found " + val);
    }
  }

  public long get(String key, long def) {
    Object val = get(key);
    if(val == null)
      return def;
    return getLong(key);
  }

  public int get(String key, int def) {
    Object val = get(key);
    if(val == null)
      return def;
    return getInt(key);
  }

  public double getDouble(String key) {
    Object val = getOrThrow(key);
    if(val instanceof Double) {
      return (Double) val;
    } else if(val instanceof Float) {
      return (Float) val;
    } else if(val instanceof Long) {
      return (Long) val;
    } else if(val instanceof Integer) {
      return (Integer) val;
    } else {
      throw new IllegalArgumentException("Key " + key + " does not exist as Double/Long in parameters object, instead found " + val);
    }
  }

  public double get(String key, double def) {
    Object val = get(key);
    if(val == null)
      return def;
    return getDouble(key);
  }

  public boolean getBoolean(String key) {
    Object val = getOrThrow(key);
    if(val instanceof Boolean) {
      return (Boolean) val;
    } else {
      throw new IllegalArgumentException("Key " + key + " does not exist as Boolean in parameters object, instead found " + val+" isa "+val.getClass());
    }
  }

  public boolean get(String key, boolean def) {
    Object val = get(key);
    if(val == null)
      return def;
    return getBoolean(key);
  }

  public Parameters getBackoff() {
    return _backoff;
  }

  // Setters
  public void set(String key, Parameters value) {
    put(key, value);
  }

  public <T> void set(String key, Collection<T> value) {
    if (List.class.isAssignableFrom(value.getClass())) {
      put(key, value);
    } else {
      put(key, new ArrayList<>(value));
    }
  }

  public void set(String key, Object[] value) {
    // using ArrayList copy to ensure mutability
    put(key, new ArrayList<>(Arrays.asList(value)));
  }

  public void set(String key, String value) {
    put(key, value);
  }

  public void set(String key, long value) {
    put(key, value);
  }

  public void set(String key, double value) {
    put(key, value);
  }

  public void set(String key, boolean value) {
    put(key, value);
  }

  /**
   * Overrides current backoff with this object.
   */
  public void setBackoff(Parameters backoff) {
    assert(backoff != this);
    this._backoff = backoff;
  }

  /**
   * Set the deepest backoff level possible... this seems like a terrible idea.
   * @param backoff the parameters to insert deep in the tree.
   * @deprecated nobody seems to be using this.
   */
	@Deprecated
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
    return _data.remove(key) != null;
  }

  // Verifiers
  public boolean isString(String key) {
    Object val = get(key);
    return val instanceof NullMarker || val instanceof String;
  }

  public boolean isList(String key) {
    return get(key) instanceof List;
  }

  public <T> boolean isList(String key, Class<T> klazz) {
    if (isList(key)) {
      List<Object> list = getList(key, Object.class);
      // empty lists can store anything
      if (list.isEmpty()) return true;

      Object o = list.get(0);
      if(klazz.isAssignableFrom(o.getClass()))
        return true;
    }
    return false;
  }

  public <T> void extendList(String key, T obj) {
    extendList(key, Collections.singleton(obj));
  }

  public <T> void extendList(String key, Collection<T> coll) {
    Object forKey = get(key);
    if(forKey == null) {
      forKey = new ArrayList();
      put(key, forKey);
    }
    if(!(forKey instanceof List))
      throw new IllegalArgumentException("Key '"+key+"' is not a list, can't add to it.");

    List<T> kl = (List<T>) forKey;
    kl.addAll(coll);
  }

  public boolean isMap(String key) {
    return get(key) instanceof Parameters;
  }

  public boolean isDouble(String key) {
    return get(key) instanceof Double || get(key) instanceof Float;
  }

  public boolean isLong(String key) {
    return get(key) instanceof Long || get(key) instanceof Integer;
  }

  public boolean isBoolean(String key) {
    return get(key) instanceof Boolean;
  }

  public boolean containsKey(String key) {
    return _data.containsKey(key);
  }

  public boolean isEmpty() {
    return (_data.isEmpty() && (_backoff != null && _backoff.isEmpty()));
  }

  public void write(String filename) throws IOException {
    FileWriter writer = null;
    try {
      writer = new FileWriter(filename);
      writer.append(this.toString());
    } finally {
      if(writer != null) writer.close();
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{ ");
    try {
      List<String> keys = new ArrayList<>(getKeys());
      Collections.sort(keys);
      for (int i = 0; i < keys.size(); i++) {
        String key = keys.get(i);
        if(i != 0) builder.append(" , ");

        // output key
        builder.append("\"").append(JSONUtil.escape(key)).append("\" : ");

        // output value
        if(isBoolean(key)) {
          builder.append(getBoolean(key));
        } else if(isLong(key)) {
          builder.append(getLong(key));
        } else if(isDouble(key)) {
          builder.append(getDouble(key));
        } else if(isString(key) || isMap(key) || isList(key)) {
          builder.append(emitComplex(get(key)));
        } else {
          throw new IllegalArgumentException("Unknown object kind: "+get(key).getClass()+" {"+key+": "+get(key)+"}");
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

    builder.append(" }");
    return builder.toString();
  }
  
  public String toPrettyString() {
    return toPrettyString(this, "", true);
  }

  public String toPrettyString(String prefix) {
    return toPrettyString(this, prefix, true);
  }

  // PRIVATE FUNCTIONS
  private static String toPrettyString(Object val, String prefix, boolean topLevel) {
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

        builder.append(toPrettyString(v, prefix + "  ", false));
      }

      builder.append("]");
      return builder.toString();
    } else if (Parameters.class.isAssignableFrom(val.getClass())) {
      Parameters p = (Parameters) val;

      StringBuilder builder = new StringBuilder();
      if(topLevel) { // otherwise, this is coming out on the same line
        builder.append(prefix);
      }
      builder.append("{\n");

      String internalPrefix = prefix + "  ";

      List<String> keys = new ArrayList<>(p.getKeys());
      Collections.sort(keys);
      for (int i = 0; i < keys.size(); i++) {
        String key = keys.get(i);

        if(i != 0) builder.append(",\n");

        builder.append(internalPrefix).append("\"").append(JSONUtil.escape(key)).append("\" : ");
        if(p.isBoolean(key)) {
          builder.append(p.getBoolean(key));
        } else if(p.isLong(key)) {
          builder.append(p.getLong(key));
        } else if(p.isDouble(key)) {
          builder.append(p.getDouble(key));
        } else if(p.isString(key)) {
          builder.append(toPrettyString(p.getString(key), internalPrefix, false));
        } else if(p.isMap(key)) {
          builder.append(toPrettyString(p.getMap(key), internalPrefix, false));
        } else if(p.isList(key)) {
          builder.append(toPrettyString(p.getList(key), internalPrefix, false));
        } else throw new UnsupportedOperationException(key+"="+p.get(key));
      }

      builder.append("\n").append(prefix).append("}");
      return builder.toString();

    } else if (String.class.isAssignableFrom(val.getClass())) {
      return "\"" + JSONUtil.escape((String) val) + "\"";

    } else {
      // Long, Double, Boolean
      return val.toString();
    }
  }

  private static String emitComplex(Object val) throws IOException {
    if(val == null) {
      return "null";
    }
    if (List.class.isAssignableFrom(val.getClass())) {
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
      return "\"" + JSONUtil.escape((String) val) + "\"";
    } else {
      // long, double, boolean
      return val.toString();
    }
  }

	public static Parameters parseArgs(String[] args) throws IOException {
		return Arguments.parse(args);
	}

	@Override
  public int size() {
    return _data.size();
  }

  @Override
  public boolean containsKey(Object o) {
    return containsKey((String) o);
  }

  @Override
  public boolean containsValue(Object o) {
    return _data.containsValue(o);
  }

  @Override
  public Object get(Object o) {
    if(_data.containsKey(o)) {
      return _data.get(o);
    } else if(_backoff != null) {
      return _backoff.get(o);
    }
    return null;
  }

  public Object getOrThrow(String key) {
    if(_data.containsKey(key)) {
      return _data.get(key);
    } else if(_backoff != null) {
      return _backoff.getOrThrow(key);
    } else {
      throw new IllegalArgumentException("No key '"+key+"' present in Parameters object.");
    }
  }

  /** Put only if key and value are not null */
  public void putIfNotNull(String k, Object v) {
    if(v == null) return;
    if(k == null) return;
    this.put(k,v);
  }

  @Override
  public Object put(String k, Object v) {
    if(v == null) {
      return _data.put(k, new NullMarker());
    }
    if(this == v) {
      throw new IllegalArgumentException("Stop your recursive Parameter madness!");
    }
    if(v instanceof File) {
      return _data.put(k, ((File) v).getAbsolutePath());
    }
    return _data.put(k, v);
  }

  public void setIfMissing(String k, Object v) {
    if(containsKey(k)) return;
    put(k, v);
  }

  @Override
  public Object remove(Object o) {
    // this is optional per the javadoc of java.util.Map
    throw new UnsupportedOperationException("Not supported because of backoff!");
  }

  @Override
  public void putAll(Map<? extends String, ?> map) {
    for(Entry<? extends String, ?> entry : map.entrySet()) {
      this.put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void clear() {
    _data = new HashMap<>();
    _backoff = null;
  }

  @Override
  public Set<String> keySet() {
    if (_backoff != null) {
      HashSet<String> all = new HashSet<>();
      all.addAll(_backoff.keySet());
      all.addAll(_data.keySet());
      return all;
    } else {
      return _data.keySet();
    }
  }

  @Override
  public Collection<Object> values() {
    ArrayList<Object> vals = new ArrayList<>();
    for(String key : keySet()) {
      vals.add(get(key));
    }
    return vals;
  }

  @Override
  public Set<Entry<String, Object>> entrySet() {
    HashSet<Entry<String,Object>> entries = new HashSet<>();
    for(String key : keySet()) {
      entries.add(new AbstractMap.SimpleImmutableEntry<>(key, get(key)));
    }
    return entries;
  }

}
