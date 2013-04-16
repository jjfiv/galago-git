// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.scoring;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.TagTokenizer;
import org.lemurproject.galago.core.parse.stem.Porter2Stemmer;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.core.retrieval.GroupRetrieval;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.util.MathUtils;
import org.lemurproject.galago.core.util.TextPartAssigner;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Implements the basic unigram Relevance Model, as described in "Relevance
 * Based Language Models" by Lavrenko and Croft in SIGIR 2001.
 *
 * @author irmarc
 */
public class RelevanceModel implements ExpansionModel {

  public static class Gram implements WeightedTerm {

    public String term;
    public double score;

    public Gram(String t) {
      term = t;
      score = 0.0;
    }

    public String getTerm() {
      return term;
    }

    public double getWeight() {
      return score;
    }

    // The secondary sort is to have defined behavior for statistically tied samples.
    public int compareTo(WeightedTerm other) {
      Gram that = (Gram) other;
      int result = this.score > that.score ? -1 : (this.score < that.score ? 1 : 0);
      if (result != 0) {
        return result;
      }
      result = (this.term.compareTo(that.term));
      return result;
    }

    public String toString() {
      return "<" + term + "," + score + ">";
    }
  }
  Parameters parameters;
  Retrieval retrieval;
  TagTokenizer tokenizer = null;
  Stemmer stemmer;
  String group;

  public RelevanceModel(Parameters parameters, Retrieval r) {
    this.parameters = parameters;
    this.retrieval = r;
    this.group = parameters.get("group", this.retrieval.getGlobalParameters().get("group", (String) null));
  }

  /*
   * This should be run while we're waiting for the results. It either creates the
   * required data structures for the model, or resets them to be used for another query.
   *
   */
  public void initialize() throws Exception {
    if (tokenizer == null) {
      tokenizer = new TagTokenizer();
    }
    if (stemmer == null) {
      if (parameters.isString("rmStemmer")) {
        String rmstemmer = parameters.getString("rmStemmer");
        stemmer = (Stemmer) Class.forName(rmstemmer).getConstructor().newInstance();
      } else {
        stemmer = new Porter2Stemmer();
      }
    }
  }

  /*
   * Run this when the Relevance Model is no longer needed.
   */
  @Override
  public void cleanup() throws Exception {
    tokenizer = null;
    stemmer = null;
  }

  @Override
  public ArrayList<WeightedTerm> generateGrams(List<ScoredDocument> initialResults) throws IOException {
    // convert documentScores to posterior probs
    HashMap<Integer, Double> scores = logstoposteriors(initialResults);

    // get term frequencies in documents
    HashMap<String, HashMap<Integer, Integer>> counts = countGrams(initialResults);

    // compute term weights
    ArrayList<WeightedTerm> scored = scoreGrams(counts, scores);

    // sort by weight
    Collections.sort(scored);

    return scored;
  }

  @Override
  public Node generateExpansionQuery(List<ScoredDocument> initialResults, int fbTerms,
          Set<String> queryTerms, Set<String> stopwords) throws IOException {

    List<WeightedTerm> scored = generateGrams(initialResults);

    ArrayList<Node> newChildren = new ArrayList<Node>();
    NodeParameters expParams = new NodeParameters();
    int expanded = 0;

    // stem query terms
    Set<String> queryTermStemmed = stemTerms(queryTerms);

    // Time to construct the modified query - start with the expansion since we always have it
    for (int i = 0; i < scored.size() && expanded < fbTerms; i++) {
      Gram g = (Gram) scored.get(i);

      // if the expansion gram is a stopword or an existing query term -- do not use it.
      if (queryTermStemmed.contains(stemmer.stem(g.term)) || (stopwords.contains(g.term))) {
        continue;
      }

      Node inner = TextPartAssigner.assignPart(new Node("extents", g.term),
              this.retrieval.getGlobalParameters(),
              this.retrieval.getAvailableParts());
      ArrayList<Node> innerChild = new ArrayList<Node>();
      // by default - normalize using the entire document.
      innerChild.add(StructuredQuery.parse("#lengths:document:part=lengths()"));
      innerChild.add(inner);
      String scorerType = parameters.get("scorer", "dirichlet");
      newChildren.add(new Node("feature", scorerType, innerChild));
      expParams.set(Integer.toString(expanded), g.score);
      expanded++;
    }
    Node expansionNode = new Node("combine", expParams, newChildren, 0);
    return expansionNode;
  }

  // Implementation here is identical to the Relevance Model unigram normaliztion in Indri.
  // See RelevanceModel.cpp for details
  protected HashMap<Integer, Double> logstoposteriors(List<ScoredDocument> results) {
    HashMap<Integer, Double> scores = new HashMap<Integer, Double>();
    if (results.isEmpty()) {
      return scores;
    }

    double[] values = new double[results.size()];
    for (int i = 0; i < results.size(); i++) {
      values[i] = results.get(i).score;
    }

    // compute the denominator
    double logSumExp = MathUtils.logSumExp(values);

    for (ScoredDocument sd : results) {
      double logPosterior = sd.score - logSumExp;
      scores.put(sd.document, Math.exp(logPosterior));
    }

    return scores;
  }

  protected HashMap<String, HashMap<Integer, Integer>> countGrams(List<ScoredDocument> results) throws IOException {
    HashMap<String, HashMap<Integer, Integer>> counts = new HashMap<String, HashMap<Integer, Integer>>();
    HashMap<Integer, Integer> termCounts;
    Document doc;
    for (ScoredDocument sd : results) {

      if (group != null && retrieval instanceof GroupRetrieval) {
        String name = ((GroupRetrieval) retrieval).getDocumentName(sd.document, group);
        doc = ((GroupRetrieval) retrieval).getDocument(name, Parameters.parse("{\"text\":true}"), group);
      } else {
        doc = retrieval.getDocument(retrieval.getDocumentName(sd.document), Parameters.parse("{\"text\":true}"));
      }

      tokenizer.tokenize(doc);
      for (String term : doc.terms) {
        if (!counts.containsKey(term)) {
          counts.put(term, new HashMap<Integer, Integer>());
        }
        termCounts = counts.get(term);
        if (termCounts.containsKey(sd.document)) {
          termCounts.put(sd.document, termCounts.get(sd.document) + 1);
        } else {
          termCounts.put(sd.document, 1);
        }
      }
    }
    return counts;
  }

  protected ArrayList<WeightedTerm> scoreGrams(HashMap<String, HashMap<Integer, Integer>> counts,
          HashMap<Integer, Double> scores) throws IOException {
    ArrayList<WeightedTerm> grams = new ArrayList<WeightedTerm>();
    HashMap<Integer, Integer> termCounts;
    HashMap<Integer, Integer> lengthCache = new HashMap<Integer, Integer>();

    for (String term : counts.keySet()) {
      Gram g = new Gram(term);
      termCounts = counts.get(term);
      for (Integer docID : termCounts.keySet()) {
        if (!lengthCache.containsKey(docID)) {
          int docLen;
          if (group != null && retrieval instanceof GroupRetrieval) {
            docLen = ((GroupRetrieval) retrieval).getDocumentLength(docID, group);
          } else {
            docLen = retrieval.getDocumentLength(docID);
          }

          lengthCache.put(docID, docLen);
        }
        int length = lengthCache.get(docID);
        g.score += scores.get(docID) * termCounts.get(docID) / length;
      }
      // 1 / fbDocs from the RelevanceModel source code
      g.score *= (1.0 / scores.size());
      grams.add(g);
    }

    return grams;
  }

  private Set<String> stemTerms(Set<String> terms) {
    HashSet<String> stems = new HashSet(terms.size());
    for (String t : terms) {
      String s = stemmer.stem(t);
      // stemmers should ensure that terms do not stem to nothing.
      stems.add(s);
    }
    return stems;
  }
}
