// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.BTreeValueIterator;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.VByteInput;

import java.io.DataInput;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author irmarc, sjh
 */
public class FieldIndexReader extends KeyListReader {

  Parameters formatMap = new Parameters();

  public FieldIndexReader(BTreeReader reader) throws IOException {
    super(reader);
    if (reader.getManifest().isMap("tokenizer")) {
      Parameters tokenizer = reader.getManifest().getMap("tokenizer");
      if (tokenizer.containsKey("formats")) {
        formatMap.copyFrom(tokenizer.getMap("formats"));
      }
    }
  }

  public FieldIndexReader(String path) throws IOException {
    super(path);
    if (reader.getManifest().isMap("tokenizer")) {
      Parameters tokenizer = reader.getManifest().getMap("tokenizer");
      if (tokenizer.containsKey("formats")) {
        formatMap.copyFrom(tokenizer.getMap("formats"));
      }
    }
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader, formatMap);
  }

  @Override
  public HashMap<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> nodeTypes = new HashMap<String, NodeType>();
    nodeTypes.put("field", new NodeType(ListIterator.class));
    return nodeTypes;
  }

  public ListIterator getField(String fieldname) throws IOException {
    BTreeReader.BTreeIterator iterator =
            reader.getIterator(Utility.fromString(fieldname));
    return new ListIterator(iterator, formatMap);
  }

  @Override
  public ListIterator getIterator(Node node) throws IOException {
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

  public static class KeyIterator extends KeyListReader.KeyValueIterator {

    public Parameters formatMap;
    
    public KeyIterator(BTreeReader reader, Parameters formatMap) throws IOException {
      super(reader);
      this.formatMap = formatMap;
    }

    @Override
    public String getValueString() {
      ListIterator it;
      long count;
      try {
        it = new ListIterator(iterator, formatMap);
        count = it.totalEntries();
      } catch (IOException ioe) {
        count = -1;
      }
      StringBuilder sb = new StringBuilder();
      sb.append(Utility.toString(getKey())).append(",");
      sb.append("list of size: ");
      if (count > 0) {
        sb.append(count);
      } else {
        sb.append("Unknown");
      }
      return sb.toString();
    }

    @Override
    public ListIterator getValueIterator() throws IOException {
      return new ListIterator(iterator, formatMap);
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(getKey());
    }
  }

  public static class ListIterator extends BTreeValueIterator
          implements BaseIterator {

    Parameters formatMap;
    BTreeReader.BTreeIterator iterator;
    //VByteInput data;
    long startPosition, endPosition;
    DataStream dataStream;
    long documentCount;
    long currentDocument;
    long documentIndex;
    String format = null;
    String strValue;
    int intValue;
    long longValue;
    float floatValue;
    double doubleValue;
    long dateValue;
    byte[] dateBytes = new byte[8];

    public ListIterator(BTreeReader.BTreeIterator iterator, Parameters formatMap) throws IOException {
      super(iterator.getKey());
      this.formatMap = formatMap;
      reset(iterator);
    }

    @Override
    public void reset() throws IOException {
      currentDocument = 0;
      documentCount = 0;
      documentIndex = 0;
      initialize();
    }

    @Override
    public void reset(BTreeReader.BTreeIterator i) throws IOException {
      iterator = i;
      key = iterator.getKey();
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

    @Override
    public String getValueString(ScoringContext c) throws IOException {
      return getKeyString() + "," + currentDocument + "," + printValue(c);
    }

    private void initialize() throws IOException {
      DataStream valueStream = iterator.getSubValueStream(0, 20);
      DataInput stream = new VByteInput(valueStream);

      documentCount = stream.readLong(); // 9 bytes
      currentDocument = 0;
      documentIndex = 0;

      // Load data stream
      dataStream = iterator.getSubValueStream(valueStream.getPosition(), endPosition);
      //data = new VByteInput(dataStream);

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

    @Override
    public void syncTo(long document) throws IOException {
      while (!isDone() && document > currentDocument) {
        documentIndex = Math.min(documentIndex + 1, documentCount);
        if (!isDone()) {
          loadValue();
        }
      }
    }

    @Override
    public boolean hasMatch(long identifier) {
      return !isDone() && currentCandidate() == identifier;
    }

    @Override
    public void movePast(long document) throws IOException {
      while (!isDone() && document >= currentDocument) {
        documentIndex = Math.min(documentIndex + 1, documentCount);
        if (!isDone()) {
          loadValue();
        }
      }
    }

    private void loadValue() throws IOException {
      currentDocument += Utility.uncompressLong(dataStream);

      // Need to figure out what to do here
      if (format.equals("string")) {
        int len = Utility.uncompressInt(dataStream);
        byte[] bytes = new byte[len];
        dataStream.readFully(bytes);
        strValue = Utility.toString(bytes);

      } else if (format.equals("int")) {
        intValue = dataStream.readInt();
      } else if (format.equals("long")) {
        longValue = dataStream.readLong();
      } else if (format.equals("float")) {
        floatValue = dataStream.readFloat();
      } else if (format.equals("double")) {
        doubleValue = dataStream.readDouble();
      } else if (format.equals("date")) {
        dataStream.readFully(dateBytes);
        dateValue = Utility.toLong(dateBytes);
      } else {
        throw new RuntimeException(String.format("Don't have any plausible format for tag %s\n",
                format));
      }
    }

    private String printValue(ScoringContext c) throws RuntimeException {
      if (this.currentDocument == c.document) {
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
      return null;
    }

    public String stringValue(ScoringContext c) {
      if (this.currentDocument == c.document) {
        if (format.equals("string")) {
          return strValue;
        } else {
          throw new RuntimeException(String.format("Incorrect format (requested: %s, found: %s)\n",
                  "string", format));
        }
      }
      return null;
    }

    public int intValue(ScoringContext c) {
      if (this.currentDocument == c.document) {
        if (format.equals("int")) {
          return intValue;
        } else {
          throw new RuntimeException(String.format("Incorrect format (requested: %s, found: %s)\n",
                  "int", format));
        }
      }
      return 0;
    }

    public long longValue(ScoringContext c) {
      if (this.currentDocument == c.document) {
        if (format.equals("long")) {
          return longValue;
        } else {
          throw new RuntimeException(String.format("Incorrect format (requested: %s, found: %s)\n",
                  "long", format));
        }
      }
      return 0l;
    }

    public float floatValue(ScoringContext c) {
      if (this.currentDocument == c.document) {
        if (format.equals("float")) {
          return floatValue;
        } else {
          throw new RuntimeException(String.format("Incorrect format (requested: %s, found: %s)\n",
                  "float", format));
        }
      }
      return 0f;
    }

    public double doubleValue(ScoringContext c) {
      if (this.currentDocument == c.document) {
        if (format.equals("double")) {
          return doubleValue;
        } else {
          throw new RuntimeException(String.format("Incorrect format (requested: %s, found: %s)\n",
                  "double", format));
        }
      }
      return 0d;
    }

    public long dateValue(ScoringContext c) {
      if (this.currentDocument == c.document) {
        if (format.equals("date")) {
          return dateValue;
        } else {
          throw new RuntimeException(String.format("Incorrect format (requested: %s, found: %s)\n",
                  "date", format));
        }
      }
      return 0l;
    }

    @Override
    public long currentCandidate() {
      return currentDocument;
    }

    @Override
    public boolean hasAllCandidates() {
      return false;
    }

    @Override
    public boolean isDone() {
      return (documentIndex >= documentCount);
    }

    @Override
    public long totalEntries() {
      return this.documentCount;
    }

    @Override
    public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
      String type = "field";
      String className = this.getClass().getSimpleName();
      String parameters = "";
      long document = currentCandidate();
      boolean atCandidate = hasMatch(c.document);
      String returnValue = printValue(c);
      List<AnnotatedNode> children = Collections.emptyList();

      return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
    }
  }
}
