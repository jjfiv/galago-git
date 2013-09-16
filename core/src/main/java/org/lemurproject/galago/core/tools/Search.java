// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.lemurproject.galago.core.index.corpus.SnippetGenerator;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.SimpleQuery;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class Search {

  public SnippetGenerator generator;
  // protected DocumentStore store;
  protected Retrieval retrieval;

  public Search(Parameters params) throws Exception {
    //this.store = getDocumentStore(params.getAsList("corpus"));
    this.retrieval = RetrievalFactory.instance(params);
    generator = new SnippetGenerator();
  }

  public Retrieval getRetrieval() {
    return retrieval;
  }

  public IndexPartStatistics getIndexPartStatistics(String part) throws IOException {
    return retrieval.getIndexPartStatistics(part);
  }

  public Parameters getAvailiableParts() throws IOException {
    return retrieval.getAvailableParts();
  }

  public void close() throws IOException {
//    store.close();
    retrieval.close();
  }

  public static class SearchResult {

    public String queryAsString;
    public Node query;
    public Node transformedQuery;
    public List<SearchResultItem> items;

    public SearchResult() {
      items = new LinkedList<SearchResultItem>();
    }
  }

  public static class SearchResultItem {

    public int rank;
    public String identifier;
    public String displayTitle;
    public String url;
    public Map<String, String> metadata;
    public String summary;
    public double score;
    public Document document;
  }

  public String getSummary(Document document, Set<String> query) throws IOException {
    if (document.metadata.containsKey("description")) {
      String description = document.metadata.get("description");

      if (description.length() > 10) {
        return generator.highlight(description, query);
      }
    }

    return generator.getSnippet(document.text, query);
  }

  public static Node parseQuery(String query, Parameters parameters) {
    String queryType = parameters.get("queryType", "complex");

    if (queryType.equals("simple")) {
      return SimpleQuery.parseTree(query);
    }

    return StructuredQuery.parse(query);
  }

  public Document getDocument(String identifier, DocumentComponents p) throws IOException {
    return retrieval.getDocument(identifier, p);
  }
  public Map<String,Document> getDocuments(List<String> identifier, DocumentComponents p) throws IOException {
    return retrieval.getDocuments(identifier, p);
  }

  public long xCount(String nodeString) throws Exception {
    return this.retrieval.getNodeStatistics(nodeString).nodeFrequency;
  }

  public long docCount(String nodeString) throws Exception {
    return this.retrieval.getNodeStatistics(nodeString).nodeDocumentCount;
  }

  public SearchResult runQuery(String query, Parameters p, boolean summarize) throws Exception {
    Node root = StructuredQuery.parse(query);
    Node transformed = retrieval.transformQuery(root, p);
    SearchResult result = runTransformedQuery(transformed, p, summarize);
    result.query = root;
    result.queryAsString = query;
    return result;
  }

  public SearchResult runTransformedQuery(Node root, Parameters p, boolean summarize) throws Exception {
    int startAt = (int) p.getLong("startAt");
    int count = (int) p.getLong("resultCount");

    List<ScoredDocument> results = retrieval.executeQuery(root, p).scoredDocuments;
    
    SearchResult result = new SearchResult();
    Set<String> queryTerms = StructuredQuery.findQueryTerms(root);
    generator.setStemming(root.toString().contains("part=stemmedPostings"));

    result.transformedQuery = root;
    
    DocumentComponents p1 = new DocumentComponents();

    for (int i = startAt; i < Math.min(startAt + count, results.size()); i++) {
      String identifier = results.get(i).documentName;
      Document document = null;
      if (summarize) {
    	  document = getDocument(identifier, p1);
      }
      SearchResultItem item = new SearchResultItem();

      item.rank = i + 1;
      item.identifier = identifier;
      item.displayTitle = identifier;
      item.document = document;
      
      if (document != null && document.metadata.containsKey("title")) {
        item.displayTitle = document.metadata.get("title");
      }

      if (item.displayTitle != null) {
        item.displayTitle = generator.highlight(item.displayTitle, queryTerms);
      }

      if (document != null && document.metadata.containsKey("url")) {
        item.url = document.metadata.get("url");
      }

      if (summarize) {
        item.summary = getSummary(document, queryTerms);
      }

      if (document != null) {
    	  item.metadata = document.metadata;
      }
      item.score = results.get(i).score;
      result.items.add(item);
    }

    return result;
  }
}
