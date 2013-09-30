/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.index.disk;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.lemurproject.galago.core.index.*;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.Utility;

/** 
 * This index stores conflations produced by a stemmer.
 *
 * @author sjh
 */
public class ConflationIndexReader extends KeyValueReader {

  /**
   * Creates a new instance of DiskNameReader
   */
  public ConflationIndexReader(String fileName) throws IOException {
    super(BTreeFactory.getBTreeReader(fileName));
  }

  public ConflationIndexReader(BTreeReader r) {
    super(r);
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    return Collections.EMPTY_MAP;
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new StemIterator(reader);
  }

  @Override
  public BaseIterator getIterator(Node node) throws IOException {
    throw new UnsupportedOperationException(
            "Index doesn't support operator: " + node.getOperator());
  }

  public static class StemIterator extends KeyValueReader.KeyValueIterator {

    private StemIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getValueString() {
      try {
        DataStream valueStream = this.iterator.getValueStream();
        int count = valueStream.readInt();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
          byte[] term = new byte[valueStream.readInt()];
          valueStream.readFully(term);
          if (i > 0) {
            sb.append(",").append(Utility.toString(term));
          } else {
            sb.append(Utility.toString(term));
          }
        }
        return sb.toString();
      } catch (IOException e) {
        return "Unknown";
      }
    }

    @Override
    public String getKeyString() {
      return Utility.toString(getKey());
    }

    @Override
    public KeyToListIterator getValueIterator() throws IOException {
      throw new UnsupportedOperationException("This index file does not support doc int -> doc name mappings");
    }
  }
}
