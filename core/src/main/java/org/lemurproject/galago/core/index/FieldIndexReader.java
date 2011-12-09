// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.DataInput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.VByteInput;

/**
 *
 * @author irmarc
 */
public class FieldIndexReader extends KeyListReader {

  public class KeyIterator extends KeyListReader.Iterator {

    public KeyIterator(GenericIndexReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getValueString() {
      ListIterator it;
      long count = -1;
      try {
        it = new ListIterator(iterator);
        count = it.totalEntries();
      } catch (IOException ioe) {
      }
      StringBuilder sb = new StringBuilder();
      sb.append(Utility.toString(getKeyBytes())).append(",");
      sb.append("list of size: ");
      if (count > 0) {
        sb.append(count);
      } else {
        sb.append("Unknown");
      }
      return sb.toString();
    }

    public ValueIterator getValueIterator() throws IOException {
      return new ListIterator(iterator);
    }
  }

  public class ListIterator extends KeyListReader.ListIterator
          implements ValueIterator {

    GenericIndexReader.Iterator iterator;
    VByteInput data;
    long startPosition, endPosition;
    DataStream dataStream;
    int documentCount;
    int options;
    int currentDocument;
    String format = null;
    String strValue;
    int intValue;
    long longValue;
    float floatValue;
    double doubleValue;
    long dateValue;
    byte[] dateBytes = new byte[8];
    int documentIndex;

    public ListIterator(GenericIndexReader.Iterator iterator) throws IOException {
	reset(iterator);
    }

    public void reset() throws IOException {
      currentDocument = 0;
      documentCount = 0;
      documentIndex = 0;
      initialize();
    }

    public void reset(GenericIndexReader.Iterator i) throws IOException {
      iterator = i;
      key = iterator.getKey();
      dataLength = iterator.getValueLength();
      startPosition = iterator.getValueStart();
      endPosition = iterator.getValueEnd();
      reset();
    }

    public boolean skipTo(byte[] key) throws IOException {
      iterator.skipTo(key);
      if (Utility.compare(key, iterator.getKey()) == 0) {
        reset();
        return true;
      }
      return false;
    }

    public String getEntry() {
      StringBuilder builder = new StringBuilder();
      builder.append(getKey());
      builder.append(",");
      builder.append(currentDocument);
      builder.append(",");
      builder.append(printValue());
      return builder.toString();
    }

    private void initialize() throws IOException {
      DataStream valueStream = iterator.getSubValueStream(0, dataLength);
      DataInput stream = new VByteInput(valueStream);

      documentCount = stream.readInt();
      currentDocument = 0;
      documentIndex = 0;

      // Load data stream
      dataStream = iterator.getSubValueStream(valueStream.getPosition(), endPosition);
      data = new VByteInput(dataStream);

      // Determine the current format map based on the key - allows for
      // crossing lists, even though I hate that.
      format = formatMap.getString(Utility.toString(iterator.getKey()));

      loadValue();
    }

    public String getFormat() {
      return format;
    }

    public void setFormat(String f) {
      format = f;
    }

    public boolean next() throws IOException {
      documentIndex = Math.min(documentIndex + 1, documentCount);

      if (!isDone()) {
        loadValue();
        return true;
      }
      return false;
    }

    public boolean moveTo(int document) throws IOException {
      while (!isDone() && document > currentDocument) {
        next();
      }
      return document == currentDocument;
    }

    private void loadValue() throws IOException {
      currentDocument += data.readInt();

      // Need to figure out what to do here
      if (format.equals("string")) {
        strValue = data.readString();
      } else if (format.equals("int")) {
        intValue = data.readInt();
      } else if (format.equals("long")) {
        longValue = data.readLong();
      } else if (format.equals("float")) {
        floatValue = data.readFloat();
      } else if (format.equals("double")) {
        doubleValue = data.readDouble();
      } else if (format.equals("date")) {
        data.readFully(dateBytes);
        dateValue = Utility.toLong(dateBytes);
      } else {
        throw new RuntimeException(String.format("Don't have any plausible format for tag %s\n",
                format));
      }
    }

    private String printValue() throws RuntimeException {
      if (format.equals("string")) {
        return String.format("%s (String)", strValue);
      } else if (format.equals("int")) {
        return String.format("%d (Int)", intValue);
      } else if (format.equals("long")) {
        return String.format("%d (Long)", longValue);
      } else if (format.equals("float")) {
        return String.format("%f (Float)", floatValue);
      } else if (format.equals("double")) {
        return String.format("%f (Double)", doubleValue);
      } else if (format.equals("date")) {

        return String.format("%s (Date)", new Date(dateValue).toString());
      } else {
        throw new RuntimeException(String.format("Don't have any plausible format for tag %s\n",
                format));
      }
    }

    public String stringValue() {
      if (format.equals("string")) {
        return strValue;
      } else {
        throw new RuntimeException(String.format("Incorrect format (requested: %s, found: %d)\n",
                "string", format));
      }
    }

    public int intValue() {
      if (format.equals("int")) {
        return intValue;
      } else {
        throw new RuntimeException(String.format("Incorrect format (requested: %s, found: %d)\n",
                "int", format));
      }
    }

    public long longValue() {
      if (format.equals("long")) {
        return longValue;
      } else {
        throw new RuntimeException(String.format("Incorrect format (requested: %s, found: %d)\n",
                "long", format));
      }
    }

    public float floatValue() {
      if (format.equals("float")) {
        return floatValue;
      } else {
        throw new RuntimeException(String.format("Incorrect format (requested: %s, found: %d)\n",
                "float", format));
      }
    }

    public double doubleValue() {
      if (format.equals("double")) {
        return doubleValue;
      } else {
        throw new RuntimeException(String.format("Incorrect format (requested: %s, found: %d)\n",
                "double", format));
      }
    }

    public long dateValue() {
      if (format.equals("date")) {
        return dateValue;
      } else {
        throw new RuntimeException(String.format("Incorrect format (requested: %s, found: %d)\n",
                "date", format));
      }
    }

    public String getKey() {
      return Utility.toString(iterator.getKey());
    }

    public byte[] getKeyBytes() {
      return iterator.getKey();
    }

    public int currentCandidate() {
      return currentDocument;
    }

    public boolean isDone() {
      return (documentIndex >= documentCount);
    }

    public boolean hasMatch(int identifier) {
      return (currentDocument == identifier);
    }

    public void movePast(int identifier) throws IOException {
      moveTo(identifier + 1);
    }

    public long totalEntries() {
      return this.documentCount;
    }
  }
  Parameters formatMap = new Parameters();

  public FieldIndexReader(GenericIndexReader reader) throws FileNotFoundException, IOException {
    super(reader);
    if (reader.getManifest().isMap("tokenizer")) {
      Parameters tokenizer = reader.getManifest().getMap("tokenizer");
      if (tokenizer.containsKey("formats")) {
        formatMap.copyFrom(tokenizer.getMap("formats"));
      }
    }
  }

  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  public Parameters getManifest() {
    return reader.getManifest();
  }

  public void close() throws IOException {
    reader.close();
  }

  public HashMap<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> nodeTypes = new HashMap<String, NodeType>();
    nodeTypes.put("field", new NodeType(Iterator.class));
    return nodeTypes;
  }

  public ListIterator getField(String fieldname) throws IOException {
    GenericIndexReader.Iterator iterator =
            reader.getIterator(Utility.fromString(fieldname));
    ListIterator it = new ListIterator(iterator);
    return it;
  }

  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("field")) {
      ListIterator it = getField(node.getDefaultParameter());
      if (node.getNodeParameters().containsKey("format")) {
        it.format = node.getNodeParameters().getString("format");
      }
      return it;
    } else {
      throw new UnsupportedOperationException("Node type " + node.getOperator()
              + " isn't supported.");
    }
  }
}
