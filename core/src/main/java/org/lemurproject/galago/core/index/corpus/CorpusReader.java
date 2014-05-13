// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import org.lemurproject.galago.core.corpus.DocumentSerializer;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.KeyValueReader;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskDataIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Reader for corpus folders - corpus folder is a parallel index structure: -
 * one key.index file - several data files (0 -> n)
 *
 *
 * @author sjh
 */
public class CorpusReader extends KeyValueReader implements DocumentReader {

  DocumentSerializer serializer;

  public CorpusReader(String fileName) throws IOException {
    super(fileName);
    init();
  }

  public CorpusReader(BTreeReader r) throws IOException {
    super(r);
    init();
  }

  private void init() throws IOException {
    final Parameters manifest = getManifest();
    serializer = DocumentSerializer.instance(manifest);
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader, serializer);
  }

  @Override
  public Document getDocument(byte[] key, DocumentComponents p) throws IOException {
    KeyIterator i = getIterator();
    if (i.findKey(key)) {
      return i.getDocument(p);
    } else {
      return null;
    }
  }

  @Override
  public Document getDocument(long key, DocumentComponents p) throws IOException {
    KeyIterator i = getIterator();
    byte[] k = Utility.fromLong(key);
    if (i.findKey(k)) {
      return i.getDocument(p);
    } else {
      return null;
    }
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("corpus", new NodeType(DiskDataIterator.class));
    return types;
  }

  public CorpusReaderSource getSource() throws IOException {
    return new CorpusReaderSource(reader);
  }

  @Override
  public DiskDataIterator<Document> getIterator(Node node) throws IOException {
    if (node.getOperator().equals("corpus")) {
      return new DiskDataIterator<Document>(getSource());
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public static class KeyIterator extends KeyValueReader.KeyValueIterator implements DocumentIterator {
    private final DocumentSerializer serializer;

    public KeyIterator(BTreeReader reader, DocumentSerializer serializer) throws IOException {
      super(reader);
      this.serializer = serializer;
    }

    @Override
    public String getKeyString() {
      return Long.toString(Utility.toLong(getKey()));
    }

    @Override
    public Document getDocument(DocumentComponents p) throws IOException {
      p.validate();
      return serializer.fromStream(iterator.getValueStream(), p);
    }

    @Override
    public String getValueString() throws IOException {
      try {
        return getDocument(new Document.DocumentComponents()).toString();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    public CorpusReaderSource getSource() throws IOException {
      return new CorpusReaderSource(reader);
    }

    @Override
    public DiskDataIterator<Document> getValueIterator() throws IOException {
      return new DiskDataIterator<Document>(getSource());
    }
  }
}