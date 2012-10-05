// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.lemurproject.galago.core.index.AggregateReader.IndexPartStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
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
  
  public String getDefaultPart();

  public String getIndexPartName(Node node) throws IOException;

  public IndexPartReader getIndexPart(String part) throws IOException;
  
  public boolean containsDocumentIdentifier(int document) throws IOException;

  public boolean containsPart(String partName);

  public boolean containsModifier(String partName, String modifierName);

  public void modify(ValueIterator iter, Node node) throws IOException;

  // This isn't necessary at the moment
  //public boolean hasChanged() throws IOException;

  public MovableIterator getIterator(Node node) throws IOException;

  public NodeType getNodeType(Node node) throws Exception;

  public IndexPartStatistics getIndexPartStatistics();

  public IndexPartStatistics getIndexPartStatistics(String part);

  public void close() throws IOException;

  public int getLength(int document) throws IOException;

  public String getName(int document) throws IOException;

  public int getIdentifier(String document) throws IOException;

  public Document getDocument(String document, Parameters p) throws IOException;

  public Map<String,Document> getDocuments(List<String> document, Parameters p) throws IOException;
  
  public LengthsReader.LengthsIterator getLengthsIterator() throws IOException;

  public NamesReader.NamesIterator getNamesIterator() throws IOException;

  public Parameters getManifest();

  public Set<String> getPartNames();

  public Map<String, NodeType> getPartNodeTypes(String partName) throws IOException;
}
