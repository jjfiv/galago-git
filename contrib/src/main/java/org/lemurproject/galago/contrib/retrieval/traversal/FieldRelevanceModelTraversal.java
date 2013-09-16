// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.contrib.retrieval.traversal;

import gnu.trove.map.hash.TObjectDoubleHashMap;
import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.traversal.Traversal;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author irmarc
 */
public class FieldRelevanceModelTraversal extends Traversal {

  ArrayList<String> queryTerms;
  Parameters p, availableFields;
  List<String> fields;
  String scorerType;
  Retrieval retrieval;
  
  public FieldRelevanceModelTraversal(Retrieval retrieval) {
    this.retrieval = retrieval;
    Parameters globals = retrieval.getGlobalParameters();
    p = globals.containsKey("fieldrm") ? globals.getMap("fieldrm") : new Parameters();
    queryTerms = new ArrayList<String>();
    try {
      availableFields = retrieval.getAvailableParts();
    } catch (Exception e) {
      throw new RuntimeException("Unable to get available parts");
    }
  }

  @Override
  public void beforeNode(Node object, Parameters qp) throws Exception {
    if (object.getOperator().equals("text") && object.getDefaultParameter() != null) {
      queryTerms.add(object.getDefaultParameter());
    }
  }

  @Override
  public Node afterNode(Node original, Parameters qp) throws Exception {
    if (original.getOperator().equals("fieldrm")) {
      if (!p.containsKey("fields")) {
        throw new IllegalArgumentException("Field Relevance Model expects fields to be specified.");
      }

      // Load and filter the fields once
      fields = new ArrayList<String>();
      for (String f : (List<String>) p.getList("fields")) {
        String partName = "field." + f;
        if (availableFields.containsKey(partName)) {
          fields.add(f);
        }
      }
      scorerType = retrieval.getGlobalParameters().get("scorer", "dirichlet");

      Node replacement;
      if (p.containsKey("documents")) {
        replacement = useRelevantDocuments();
      } else {
        replacement = estimateRelevance(original);
      }
      return replacement;
    } else {
      return original;
    }
  }

  private List<Document> getDocuments(List<String> names) throws Exception {
    String path = p.containsKey("corpus") ? p.getString("corpus")
            : p.getString("index") + File.separator + "corpus";
    Map<String, Document> docmap = retrieval.getDocuments(names, new DocumentComponents());
    List<Document> docs = new ArrayList<Document>(docmap.values());
    if (p.containsKey("parser")) {
      Class c = Class.forName(p.getString("parser"));
      Method m = c.getMethod("parse", List.class, Parameters.class);
      docs = (List<Document>) m.invoke(null, docs, p);
    }
    return docs;
  }

  private Node createTermFieldNodes(String term, TObjectDoubleHashMap<String> weights) {
    int i = 0;
    ArrayList<Node> termFields = new ArrayList<Node>();
    NodeParameters np = new NodeParameters();

    for (String field : fields) {
      String partName = "field." + field;
      NodeParameters par1 = new NodeParameters();
      par1.set("default", term);
      par1.set("part", partName);
      Node termCount = new Node("counts", par1, new ArrayList(), 0);
      double weight = weights.get(field);
      weight = (weight > 0.0) ? weight : FieldLanguageModel.smoothing;
      if (retrieval.getGlobalParameters().get("printWeights", false)) {
        if (weight > FieldLanguageModel.smoothing) {
          System.err.printf("%s\t%s\t%f\n", term, field, weight);
        }
      }
      np.set(Integer.toString(i), weight);
      Node termScore = new Node("feature", scorerType + "-raw");
      termScore.getNodeParameters().set("lengths", field);
      termScore.addChild(termCount);
      termFields.add(termScore);
      i++;
    }
    Node termFieldNodes = new Node("combine", np, termFields, 0);
    Node logScoreNode = new Node("feature", "log");
    logScoreNode.addChild(termFieldNodes);
    return logScoreNode;
  }

  private Node useRelevantDocuments() throws Exception {
    List<String> docnames = (List<String>) p.getList("documents");
    List<Document> docs = getDocuments(docnames);
    FieldLanguageModel flm = new FieldLanguageModel();
    for (Document d : docs) {
      if (d.terms != null && d.terms.size() > 0) {
        flm.addDocument(d);
      }
    }

    Node termNodes = new Node("combine", new NodeParameters(), new ArrayList<Node>(), 0);
    termNodes.getNodeParameters().set("norm", false);

    // Now put get the sub-model for each term
    TObjectDoubleHashMap<String> weights = new TObjectDoubleHashMap<String>();
    for (String term : queryTerms) {
      weights.clear();
      for (String field : fields) {
        weights.put(term, flm.getFieldProbGivenTerm(field, term));
      }
      termNodes.addChild(createTermFieldNodes(term, weights));
    }
    return termNodes;
  }

  private HashMap<String, TObjectDoubleHashMap<String>> getUnigramCollFieldScores() throws Exception {
    HashMap<String, TObjectDoubleHashMap<String>> outer = new HashMap<String, TObjectDoubleHashMap<String>>();
    for (String term : queryTerms) {
      TObjectDoubleHashMap<String> inner = new TObjectDoubleHashMap<String>();
      double normalizer = 0.0;

      // First get all the term/field unnormalized statistics (creating a normalizer as we go)
      for (String field : fields) {
        FieldStatistics field_cs = retrieval.getCollectionStatistics("#lengths:"+field+":part=lengths()");
        
        String partName = "field." + field;
        NodeParameters par1 = new NodeParameters();
        par1.set("default", term);
        par1.set("part", partName);
        Node termCount = new Node("counts", par1, new ArrayList(), 0);
        NodeStatistics ns = retrieval.getNodeStatistics(termCount);
        double fieldprob = (ns.nodeFrequency + 0.0) / field_cs.collectionLength; // P(t|F_j)
        inner.put(field, fieldprob);
        normalizer += fieldprob;
      }

      // Now normalize
      if (normalizer > 0.0) {
        for (String field : fields) {
          inner.put(field, inner.get(field) / normalizer);
        }
      }
      outer.put(term, inner);
    }
    return outer;
  }

  private HashMap<String, TObjectDoubleHashMap<String>> getBigramCollFieldScores() throws Exception {
    HashMap<String, TObjectDoubleHashMap<String>> outer = new HashMap<String, TObjectDoubleHashMap<String>>();
    for (int i = 0; i < queryTerms.size() - 1; i++) {
      String term1 = queryTerms.get(i);
      String term2 = queryTerms.get(i + 1);
      TObjectDoubleHashMap<String> inner = new TObjectDoubleHashMap<String>();
      double normalizer = 0.0;

      // First get all the term/field unnormalized statistics (creating a normalizer as we go)
      for (String field : fields) {
        FieldStatistics field_cs = retrieval.getCollectionStatistics("#lengths:"+field+":part=lengths()");

        String partName = "field." + field;

        NodeParameters par1 = new NodeParameters();
        par1.set("default", term1);
        par1.set("part", partName);
        Node term1Count = new Node("extents", par1, new ArrayList(), 0);

        NodeParameters par2 = new NodeParameters();
        par2.set("default", term2);
        par2.set("part", partName);
        Node term2Count = new Node("extents", par2, new ArrayList(), 0);

        Node odCount = new Node("od", new ArrayList<Node>());
        odCount.getNodeParameters().set("default", 1);
        odCount.addChild(term1Count);
        odCount.addChild(term2Count);

        NodeStatistics ns = retrieval.getNodeStatistics(odCount);
        double fieldprob = (ns.nodeFrequency + 0.0) / field_cs.collectionLength; // P(t|F_j)
        inner.put(field, fieldprob);
        normalizer += fieldprob;
      }

      // Now normalize
      if (normalizer > 0.0) {
        for (String field : fields) {
          inner.put(field, inner.get(field) / normalizer);
        }
      }
      // Have to map to a single term
      outer.put(term2, inner);
    }
    return outer;
  }

  private List<String> getGrams(int size) {
    ArrayList<String> grams = new ArrayList<String>();
    for (int i = 0; i < queryTerms.size() - (size - 1); i++) {
      grams.add(Utility.join(queryTerms.subList(i, i + size).toArray(new String[0])));
    }
    return grams;
  }

  private HashMap<String, TObjectDoubleHashMap<String>> getNGramPRFScores(List<Document> initial, int size) {
    // Setup the language model
    FieldLanguageModel flm = new FieldLanguageModel(size);
    for (Document d : initial) {
      if (d.terms != null && d.terms.size() > 0) {
        flm.addDocument(d);
      }
    }
    HashMap<String, TObjectDoubleHashMap<String>> outer = new HashMap<String, TObjectDoubleHashMap<String>>();
    List<String> grams = getGrams(size);
    for (String gram : grams) {
      TObjectDoubleHashMap<String> inner = new TObjectDoubleHashMap<String>();
      double normalizer = 0.0;

      for (String field : fields) {
        double fieldprob = flm.getTermProbGivenField(gram, field);
        inner.put(field, fieldprob);
        normalizer += fieldprob;
      }

      // Now normalize
      for (String field : fields) {
        inner.put(field, inner.get(field) / normalizer);
      }
      // Have to map to a single term
      String key = (size == 1) ? gram : gram.split(" ")[size - 1];
      outer.put(key, inner);
    }
    return outer;
  }

  private HashMap<String, TObjectDoubleHashMap<String>> getUnigramPRFScores(List<Document> initial) {
    return getNGramPRFScores(initial, 1);
  }

  private HashMap<String, TObjectDoubleHashMap<String>> getBigramPRFScores(List<Document> initial) {
    return getNGramPRFScores(initial, 2);
  }

  private Node estimateRelevance(Node original) throws Exception {
    // Need to perform an initial run
    Node combineNode = new Node("combine", new NodeParameters(), original.getInternalNodes(), original.getPosition());

    // Only get as many as we need
    Parameters localParameters = retrieval.getGlobalParameters().clone();
    int fbDocs = (int) retrieval.getGlobalParameters().get("fbDocs", 10);
    localParameters.set("requested", fbDocs);

    // transform and run
    Node transformedCombineNode = retrieval.transformQuery(combineNode, localParameters);
    List<ScoredDocument> initialResults = retrieval.executeQuery(transformedCombineNode, localParameters).scoredDocuments;
    
    // Gather content
    ArrayList<String> names = new ArrayList<String>();
    for (ScoredDocument sd : initialResults) {
      names.add(sd.documentName);
    }
    List<Document> docs = getDocuments(names);

    // Now the maps from the different information sources
    // Each of these is a term -> field -> score double-mapping
    HashMap<String, TObjectDoubleHashMap<String>> uCFSs = null;
    double ucfw = p.get("ucfw", 1.0);
    if (ucfw != 0.0) {
      uCFSs = getUnigramCollFieldScores();
    }

    HashMap<String, TObjectDoubleHashMap<String>> bCFSs = null;
    double bcfw = p.get("bcfw", 1.0);
    if (bcfw != 0.0) {
      bCFSs = getBigramCollFieldScores();
    }

    HashMap<String, TObjectDoubleHashMap<String>> uPRFSs = null;
    double uprfw = p.get("uprfw", 1.0);
    if (uprfw != 0.0) {
      uPRFSs = getUnigramPRFScores(docs);
    }

    HashMap<String, TObjectDoubleHashMap<String>> bPRFSs = null;
    double bprfw = p.get("bprfw", 1.0);
    if (bprfw != 0.0) {
      bPRFSs = getBigramPRFScores(docs);
    }

    // We can now construct term-field weights using our supplied lambdas
    // and scores
    Node termNodes = new Node("combine", new NodeParameters(), new ArrayList<Node>(), 0);
    termNodes.getNodeParameters().set("norm", false);
    TObjectDoubleHashMap<String> weights = new TObjectDoubleHashMap<String>();
    for (String term : queryTerms) {
      weights.clear();
      for (String field : fields) {
        double sum = 0.0;
        if (uCFSs != null) {
          sum += (ucfw * uCFSs.get(term).get(field));
        }

        if (bCFSs != null) {
          TObjectDoubleHashMap<String> termMap = bCFSs.get(term);
          if (termMap != null) {
            sum += (bcfw * termMap.get(field));
          }
        }

        if (uPRFSs != null) {
          sum += (uprfw * uPRFSs.get(term).get(field));
        }

        if (bPRFSs != null) {
          TObjectDoubleHashMap<String> termMap = bPRFSs.get(term);
          if (termMap != null) {
            sum += (bprfw * termMap.get(field));
          }
        }
        weights.put(field, sum);
      }
      termNodes.addChild(createTermFieldNodes(term, weights));
    }
    return termNodes;
  }
}
