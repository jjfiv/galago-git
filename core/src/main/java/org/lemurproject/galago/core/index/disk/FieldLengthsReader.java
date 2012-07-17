package org.lemurproject.galago.core.index.disk;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.index.LengthsReader;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Wraps the WindowIndexReader to act as a lengths reader for a
 * particular field.
 * 
 * @author irmarc
 */
public class FieldLengthsReader implements LengthsReader {

  String field;
  WindowIndexReader reader;

  public FieldLengthsReader(WindowIndexReader reader) {
    this.field = "";
    this.reader = reader;
  }

  @Override
  public int getLength(int document) throws IOException {
    LengthsReader.Iterator li = getLengthsIterator();
    li.moveTo(document);
    if (li.hasMatch(document)) {
      return li.getCurrentLength();
    } else {
      return 0;
    }
  }

  public void setField(String f) {
    this.field = f;
  }

  public Iterator getLengthsIterator(String f) throws IOException {
    return new LengthIterator(reader.getTermExtents(f));
  }

  @Override
  public Iterator getLengthsIterator() throws IOException {
    return new LengthIterator(reader.getTermExtents(field));
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("fieldlengths", new NodeType(LengthIterator.class));
    return types;
  }

  @Override
  public String getDefaultOperator() {
    return "fieldlengths";
  }

  @Override
  public org.lemurproject.galago.core.index.KeyIterator getIterator() throws IOException {
    return reader.getIterator();
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("fieldlengths")
            && node.getNodeParameters().containsKey("part")) {
      String part = node.getNodeParameters().getString("part");
      return new LengthIterator(reader.getTermExtents(part));
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  @Override
  public Parameters getManifest() {
    return reader.getManifest();
  }

  public class LengthIterator extends ValueIterator implements LengthsReader.Iterator {

    private WindowIndexReader.TermExtentIterator extentsIterator;
    int length = -1;

    public LengthIterator(WindowIndexReader.TermExtentIterator counts) {
      this.extentsIterator = counts;
    }

    @Override
    public int getCurrentLength() {
      if (length < 0) {
        length = 0;
        ExtentArray extents = extentsIterator.extents();
        for (int i = 0; i < extents.size(); i++) {
          length += extents.end(i) - extents.begin(i);
        }
      }
      return length;
    }

    @Override
    public int getCurrentIdentifier() {
      return extentsIterator.currentCandidate();
    }

    @Override
    public int currentCandidate() {
      return extentsIterator.currentCandidate();
    }

    @Override
    public boolean hasMatch(int identifier) {
      return (extentsIterator.currentCandidate() == identifier);
    }

    @Override
    public void moveTo(int identifier) throws IOException {
      extentsIterator.moveTo(identifier);
      length = -1;
    }

    @Override
    public void movePast(int identifier) throws IOException {
      extentsIterator.movePast(identifier);
      length = -1;
    }

    @Override
    public String getEntry() throws IOException {
      return extentsIterator.getEntry();
    }

    @Override
    public long totalEntries() {
      return extentsIterator.totalEntries();
    }

    @Override
    public void reset() throws IOException {
      extentsIterator.reset();
      length = -1;
    }

    @Override
    public boolean isDone() {
      return extentsIterator.isDone();
    }

    @Override
    public int compareTo(MovableIterator t) {
      return extentsIterator.compareTo(t);
    }

    @Override
    public boolean hasAllCandidates() {
      return false;
    }

    @Override
    public String getKeyString() throws IOException {
      return "lengths";
    }

    @Override
    public byte[] getKeyBytes() throws IOException {
      return Utility.fromString("lengths");
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
  }
}
