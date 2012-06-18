// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.index.*;
import org.lemurproject.galago.core.index.BTreeReader.BTreeIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Reads documents lengths from a document lengths file. KeyValueIterator
 * provides a useful interface for dumping the contents of the file.
 *
 * @author trevor, sjh
 */
public class DiskLengthsReader extends KeyListReader implements LengthsReader {

  //final KeyIterator keyIterator;
  // this is a special
  private final LengthsIterator documentLengths;

  public DiskLengthsReader(String filename) throws FileNotFoundException, IOException {
    super(filename);
    KeyIterator keyIterator = new KeyIterator(reader);
    keyIterator.findKey(Utility.fromString("document"));
    documentLengths = (LengthsIterator) keyIterator.getValueIterator();
  }

  public DiskLengthsReader(BTreeReader r) throws IOException {
    super(r);
    KeyIterator keyIterator = new KeyIterator(reader);
    keyIterator.findKey(Utility.fromString("document"));
    documentLengths = (LengthsIterator) keyIterator.getValueIterator();
  }

  @Override
  public int getLength(int document) throws IOException {
    int length = -1;
    synchronized (documentLengths) {
      documentLengths.moveTo(document);
      if (documentLengths.hasMatch(document)) {
        length = documentLengths.getCurrentLength();
      }
    }
    return length;
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  @Override
  public LengthsReader.Iterator getLengthsIterator() throws IOException {
    return (LengthsIterator) new KeyIterator(reader).getValueIterator();
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("lengths", new NodeType(ValueIterator.class));
    return types;
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("lengths")) {
      String key = node.getNodeParameters().get("default", "document");
      KeyIterator ki = new KeyIterator(reader);
      ki.skipToKey(Utility.fromString(key));
      return ki.getValueIterator();
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public class KeyIterator extends KeyListReader.KeyValueIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getValueString() {
      return "length Data";
    }

    @Override
    public org.lemurproject.galago.core.index.ValueIterator getValueIterator() throws IOException {
      return new LengthsIterator(iterator);
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(getKey());
    }
  }

  public class LengthsIterator extends KeyListReader.ListIterator
          implements MovableCountIterator, LengthsReader.Iterator {

    private MappedByteBuffer data;
    // data starts with two integers : (firstdoc, doccount)
    int firstDocument;
    int documentCount;
    // iterator variables
    int currDocument;
    boolean done;

    public LengthsIterator(BTreeReader.BTreeIterator iterator) throws IOException {
      super(iterator.getKey());
      reset(iterator);
    }

    @Override
    public void reset(BTreeIterator it) throws IOException {
      this.data = it.getValueMemoryMap();
      this.firstDocument = data.getInt(0);
      this.documentCount = data.getInt(4);

      // offset is the first document
      this.currDocument = firstDocument;
      this.done = (documentCount == currDocument);
    }

    @Override
    public int currentCandidate() {
      return currDocument;
    }

    @Override
    public boolean hasAllCandidates() {
      return true;
    }

    @Override
    public void next() throws IOException {
      currDocument++;
      if (currDocument == documentCount) {
        currDocument = documentCount - 1;
        done = true;
      }
    }

    @Override
    public void moveTo(int identifier) throws IOException {
      currDocument = identifier;
      if (currDocument == documentCount) {
        currDocument = documentCount - 1;
        done = true;
      }
    }

    @Override
    public void reset() throws IOException {
      this.currDocument = 0;
      this.done = (documentCount == currDocument);
    }

    @Override
    public boolean isDone() {
      return done;
    }

    @Override
    public String getEntry() throws IOException {
      return getCurrentIdentifier() + "," + getCurrentLength();
    }

    @Override
    public long totalEntries() {
      return documentCount;
    }

    @Override
    public AnnotatedNode getAnnotatedNode() throws IOException {
      String type = "lengths";
      String className = this.getClass().getSimpleName();
      String parameters = "";
      int document = currentCandidate();
      boolean atCandidate = hasMatch(this.context.document);
      String returnValue = Integer.toString(getCurrentLength());
      List<AnnotatedNode> children = Collections.EMPTY_LIST;

      return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
    }

    @Override
    public int count() {
      return getCurrentLength();
    }

    @Override
    public int maximumCount() {
      return Integer.MAX_VALUE;
    }

    @Override
    public int getCurrentLength() {
      // check for range.
      if (firstDocument <= currDocument && currDocument < documentCount) {
        return this.data.getInt(8 + (4 * (this.currDocument - firstDocument)));
      }
      return 0;
    }

    @Override
    public int getCurrentIdentifier() {
      return this.currDocument;
    }
  }
}
