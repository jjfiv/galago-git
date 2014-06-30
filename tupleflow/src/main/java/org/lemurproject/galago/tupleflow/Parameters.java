package org.lemurproject.galago.tupleflow;

import org.lemurproject.galago.utility.json.JSONParser;
import org.lemurproject.galago.utility.json.JSONUtil;

import java.io.*;
import java.util.Map;

/**
 * @author jfoley
 * moved to utility.Parameters; left this stub here as part of deprecation during 3.7
 */
@Deprecated
public class Parameters extends org.lemurproject.galago.utility.Parameters {
  @Deprecated
  public Parameters() {
    super();
  }

  @Deprecated
  private Parameters(org.lemurproject.galago.utility.Parameters newParameters) {
    this.copyFrom(newParameters);
  }

  public static Parameters parseMap(Map<String,String> map) {
    Parameters self = new Parameters();

    for (String key : map.keySet()) {
      self.put(key, JSONUtil.parseString(map.get(key)));
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
    return new Parameters(jp.parse());
  }

  public static Parameters parseFile(String path) throws IOException {
    return parseFile(new File(path));
  }

  public static Parameters parseString(String data) throws IOException {
    JSONParser jp = new JSONParser(new StringReader(data), "<from string>");
    return new Parameters(jp.parse());
  }

  public static Parameters parseReader(Reader reader) throws IOException {
    JSONParser jp = new JSONParser(reader, "<from reader>");
    return new Parameters(jp.parse());
  }

  public static Parameters parseStream(InputStream iStream) throws IOException {
    JSONParser jp = new JSONParser(new InputStreamReader(iStream), "<from stream>");
    return new Parameters(jp.parse());
  }

  public static Parameters parseBytes(byte[] data) throws IOException {
    return parseStream(new ByteArrayInputStream(data));
  }

  public static Parameters parseArray(Object... args) {
    if(args.length % 2 == 1) {
      throw new IllegalArgumentException("Uneven number of parameters in vararg constructor.");
    }
    Parameters result = new Parameters();
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
}
