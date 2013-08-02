// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.DataIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Describes the general contract that must be fulfilled by an implementing class.
 * @author irmarc
 */
public interface Index {

  public interface IndexComponentReader {

    public Parameters getManifest();

    public void close() throws IOException;
  }

  public String getIndexPath();

  public String getDefaultPart();

  public String getIndexPartName(Node node) throws IOException;

  public IndexPartReader getIndexPart(String part) throws IOException;

  public boolean containsDocumentIdentifier(long document) throws IOException;

  public boolean containsPart(String partName);

  // This isn't necessary at the moment
  //public boolean hasChanged() throws IOException;
  public BaseIterator getIterator(Node node) throws IOException;

  public NodeType getNodeType(Node node) throws Exception;

  public IndexPartStatistics getIndexPartStatistics(String part);

  public void close() throws IOException;

  public int getLength(long document) throws IOException;

  public String getName(long document) throws IOException;

  public long getIdentifier(String document) throws IOException;

  public Document getDocument(String document, DocumentComponents p) throws IOException;

  public Map<String, Document> getDocuments(List<String> document, DocumentComponents p) throws IOException;

  public LengthsIterator getLengthsIterator() throws IOException;

  public DataIterator<String> getNamesIterator() throws IOException;

  public Parameters getManifest();

  public Set<String> getPartNames();

  public Map<String, NodeType> getPartNodeTypes(String partName) throws IOException;
}
