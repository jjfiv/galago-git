// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.index.GenericIndexReader;
import org.lemurproject.galago.core.index.KeyToListIterator;
import org.lemurproject.galago.core.index.KeyValueReader;
import org.lemurproject.galago.core.index.LengthsReader;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.iterator.CountValueIterator;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Reads documents lengths from a document lengths file.
 * Iterator provides a useful interface for dumping the contents of the file.
 * 
 * @author trevor, sjh
 */
public class DiskLengthsReader extends KeyValueReader implements LengthsReader {

  public DiskLengthsReader(String filename) throws FileNotFoundException, IOException {
    super(filename);
  }

  public DiskLengthsReader(GenericIndexReader r) {
    super(r);
  }

  public int getLength(int document) throws IOException {
    return Utility.uncompressInt(reader.getValueBytes(Utility.fromInt(document)), 0);
  }

  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  public LengthsReader.Iterator getLengthsIterator() throws IOException {
    return (LengthsReader.Iterator) new ValueIterator(new KeyIterator(reader));
  }

  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("lengths", new NodeType(ValueIterator.class));
    return types;
  }

  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("lengths")) {
      return new ValueIterator(new KeyIterator(reader));
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public class KeyIterator extends KeyValueReader.Iterator {

    public KeyIterator(GenericIndexReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getKey() {
      return Integer.toString(Utility.toInt(getKeyBytes()));
    }

    public String getValueString() {
      try {
        return Integer.toString(Utility.uncompressInt(iterator.getValueBytes(), 0));
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    public boolean skipToKey(int key) throws IOException {
      return skipToKey(Utility.fromInt(key));
    }

    public int getCurrentIdentifier() {
      return Utility.toInt(iterator.getKey());
    }

    public int getCurrentLength() throws IOException {
      return Utility.uncompressInt(iterator.getValueBytes(), 0);
    }

    public boolean isDone() {
      return iterator.isDone();
    }

    public ValueIterator getValueIterator() throws IOException {
      return new ValueIterator(this);
    }
  }

  public class ValueIterator extends KeyToListIterator implements CountValueIterator, LengthsReader.Iterator {

    public ValueIterator(KeyIterator it) {
      super(it);
    }

    public String getEntry() throws IOException {
      KeyIterator ki = (KeyIterator) iterator;
      String output = Integer.toString(ki.getCurrentIdentifier()) + ","
              + Integer.toString(ki.getCurrentLength());
      return output;
    }

    public long totalEntries() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public int count() {
      try {
        return ((KeyIterator) iterator).getCurrentLength();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    public boolean skipToKey(int candidate) throws IOException {
      KeyIterator ki = (KeyIterator) iterator;
      return ki.skipToKey(candidate);
    }
    
    public int getCurrentLength() throws IOException {
      KeyIterator ki = (KeyIterator) iterator;
      return ki.getCurrentLength();
    }

    public int getCurrentIdentifier() throws IOException {
      KeyIterator ki = (KeyIterator) iterator;
      return ki.getCurrentIdentifier();
    }
  }
}
