// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import gnu.trove.list.array.TIntArrayList;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.AggregateReader.IndexPartStatistics;
import org.lemurproject.galago.core.index.AggregateReader.CollectionStatistics;
import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.query.QueryType;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class RemoveStopwordsTraversalTest extends TestCase {

  public RemoveStopwordsTraversalTest(String testName) {
    super(testName);
  }

  public void testFileRemoval() throws Exception {
    File temp = Utility.createTemporary();

    PrintWriter writer = new PrintWriter(temp);
    writer.println("a");
    writer.println("b");
    writer.close();
    Parameters p = new Parameters();
    p.set("stopwords", temp.getCanonicalPath());
    Parameters qp = new Parameters();
    RemoveStopwordsTraversal traversal = new RemoveStopwordsTraversal(new MockRetrieval(p));
    Node root = StructuredQuery.parse("#combine(#counts:a() #counts:c() #counts:b() #counts:d() #counts:e())");
    Node removed = StructuredQuery.copy(traversal, root);

    assertEquals(5, removed.getInternalNodes().size());
    assertEquals(null, removed.getInternalNodes().get(0).getDefaultParameter());
    assertEquals("c", removed.getInternalNodes().get(1).getDefaultParameter());
    assertEquals(null, removed.getInternalNodes().get(2).getDefaultParameter());
    assertEquals("d", removed.getInternalNodes().get(3).getDefaultParameter());
    assertEquals("e", removed.getInternalNodes().get(4).getDefaultParameter());

    //root = StructuredQuery.parse("#od:5(#extents:a() #extents:c() #extents: b())");
    //removed = StructuredQuery.copy(traversal, root);
    //assertEquals(1, removed.getInternalNodes().size());
    //assertEquals("c", removed.getInternalNodes().get(0).getDefaultParameter());

    temp.delete();
  }

  public void testListRemoval() throws Exception {
    Parameters p = new Parameters();
    String[] stopwords = {"a", "b"};
    p.set("stopwords", stopwords);

    Parameters qp = new Parameters();
    RemoveStopwordsTraversal traversal = new RemoveStopwordsTraversal(new MockRetrieval(p));
    Node root = StructuredQuery.parse("#combine(#counts:a() #counts:c() #counts:b() #counts:d() #counts:e())");
    Node removed = StructuredQuery.copy(traversal, root);

    assertEquals(5, removed.getInternalNodes().size());
    assertEquals(null, removed.getInternalNodes().get(0).getDefaultParameter());
    assertEquals("c", removed.getInternalNodes().get(1).getDefaultParameter());
    assertEquals(null, removed.getInternalNodes().get(2).getDefaultParameter());
    assertEquals("d", removed.getInternalNodes().get(3).getDefaultParameter());
    assertEquals("e", removed.getInternalNodes().get(4).getDefaultParameter());

    //root = StructuredQuery.parse("#od:5(#extents:a() #extents:c() #extents: b())");
    //removed = StructuredQuery.copy(traversal, root);
    //assertEquals(1, removed.getInternalNodes().size());
    //assertEquals("c", removed.getInternalNodes().get(0).getDefaultParameter());

  }

  private class MockRetrieval implements Retrieval {

    Parameters p;

    public MockRetrieval(Parameters p) {
      this.p = p;
    }

    public void close() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public Parameters getGlobalParameters() {
      return p;
    }

    public Parameters getAvailableParts() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public NodeType getNodeType(Node node) throws Exception {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public QueryType getQueryType(Node node) throws Exception {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public Node transformQuery(Node root, Parameters ignored) throws Exception {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public ScoredDocument[] runQuery(Node root) throws Exception {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public ScoredDocument[] runQuery(Node root, Parameters parameters) throws Exception {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public ScoredDocument[] runQuery(Node root, Parameters parameters, TIntArrayList workingSet) throws Exception {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public IndexPartStatistics getIndexPartStatistics() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public IndexPartStatistics getIndexPartStatistics(String partName) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public CollectionStatistics getCollectionStatistics(String nodeString) throws Exception {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public CollectionStatistics getCollectionStatistics(Node node) throws Exception {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public NodeStatistics getNodeStatistics(String nodeString) throws Exception {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public NodeStatistics getNodeStatistics(Node node) throws Exception {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Document getDocument(String identifier, Parameters p) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, Document> getDocuments(List<String> identifier, Parameters p) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getDocumentLength(int docid) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getDocumentLength(String docname) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getDocumentName(int docid) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addNodeToCache(Node node) throws Exception {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addAllNodesToCache(Node node) throws Exception {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }
}
