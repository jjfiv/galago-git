// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.index.GenericIndexReader;
import org.lemurproject.galago.core.index.KeyToListIterator;
import org.lemurproject.galago.core.index.KeyValueReader;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.iterator.DataIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * Reader for corpus folders
 *  - corpus folder is a parallel index structure:
 *  - one key.index file
 *  - several data files (0 -> n)
 *
 *
 * @author sjh
 */
public class CorpusReader extends KeyValueReader implements DocumentReader {

  boolean compressed;

  public CorpusReader(String fileName) throws FileNotFoundException, IOException {
    super(fileName);
    compressed = reader.getManifest().get("compressed", true);
  }

  public CorpusReader(GenericIndexReader r) {
    super(r);
    compressed = reader.getManifest().get("compressed", true);
  }
  
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  public Document getDocument(int key) throws IOException {
    KeyIterator i = new KeyIterator(reader);
    byte[] k = Utility.fromInt(key);
    if (i.findKey(k)) {
      return i.getDocument();
    } else {
      return null;
    }
  }

  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("corpus", new NodeType(ValueIterator.class));
    return types;
  }

  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("corpus")) {
      return new ValueIterator(new KeyIterator(reader));
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public class KeyIterator extends KeyValueReader.Iterator implements DocumentIterator {

    KeyIterator(GenericIndexReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getKey(){
      return Integer.toString(Utility.toInt(getKeyBytes()));
//      return Utility.toString(this.getKeyBytes());
    }
    
    @Override
    public Document getDocument() throws IOException {
      return Document.deserialize(iterator.getValueBytes(), compressed);
    }

    @Override
    public String getValueString() {
      try {
        return getDocument().toString();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    public ValueIterator getValueIterator() throws IOException {
      return new ValueIterator(this);
    }
  }

  public class ValueIterator extends KeyToListIterator implements DataIterator<Document> {

    public ValueIterator(KeyIterator ki) {
      super(ki);
    }

    public String getEntry() throws IOException {
      return ((KeyIterator) iterator).getDocument().toString();
    }

    public long totalEntries() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public Document getData() {
      try {
        return ((KeyIterator) iterator).getDocument();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }
}