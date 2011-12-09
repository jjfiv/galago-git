// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.DataInput;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.logging.Logger;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.BufferedFileDataStream;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.VByteInput;

/**
 *
 * @author marc
 */
public class TopDocsReader extends AbstractModifier {
  private Logger LOG = Logger.getLogger(getClass().toString());

  public class TopDocument implements Cloneable {

    public int document;
    public int count;
    public int length;

    public boolean equals(Object o) {
      if (o instanceof TopDocument == false) {
        return false;
      }
      return (this.document == ((TopDocument) o).document);
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(document);
      sb.append(",");
      sb.append(count);
      sb.append(",");
      sb.append(length);
      return sb.toString();
    }

    public TopDocument clone() {
      TopDocument copy = new TopDocument();
      copy.document = this.document;
      copy.count = this.count;
      copy.length = this.length;
      return copy;
    }
  }

  public TopDocsReader(GenericIndexReader r) {
    super(r);
  }

  public void printContents(PrintStream out) throws IOException {
    GenericIndexReader.Iterator iterator = reader.getIterator();
    while (!iterator.isDone()) {
      out.printf("Key: %s\n", Utility.toString(iterator.getKey()));
      ArrayList<TopDocument> li = getTopDocs(iterator, -1);
      for (TopDocument td : li) {
        out.printf("\t%s\n", td.toString());
      }
      iterator.nextKey();
    }
  }

  public Object getModification(Node node) throws IOException {
    if (!isEligible(node)) return null;

    // Can make a modifier
    String term = node.getDefaultParameter();
    int limit = (int) node.getNodeParameters().get("limit", 1000);
    GenericIndexReader.Iterator iterator = reader.getIterator(Utility.fromString(term));
    if (iterator == null) return null;

    // Iterator is set - grab the value data and make the specific modifier
    return getTopDocs(iterator, limit);
  }

  public void close() throws IOException {
    reader.close();
  }

  public ArrayList<TopDocument> getTopDocs(GenericIndexReader.Iterator iterator, int limit) throws IOException {
    VByteInput input = new VByteInput(iterator.getValueStream());
    int numEntries = input.readInt();
    int lastDocument = 0;
    ArrayList<TopDocument> topdocs = new ArrayList<TopDocument>();
    int l = (limit == -1) ? numEntries : Math.min(numEntries, limit);
    for (int i = 0; i < l; i++) {
      TopDocument td = new TopDocument();
      td.document = input.readInt();
      td.count = input.readInt();
      td.length = input.readInt();
      topdocs.add(td);
    }
    return topdocs;
  }
}
