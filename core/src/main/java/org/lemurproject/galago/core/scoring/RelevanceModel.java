// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.scoring;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.TagTokenizer;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.MathUtils;
import org.lemurproject.galago.core.util.TextPartAssigner;
import org.lemurproject.galago.tupleflow.Parameters;
import org.tartarus.snowball.ext.englishStemmer;

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
  englishStemmer stemmer = null;

  public RelevanceModel(Parameters parameters, Retrieval r) {
    this.parameters = parameters;
    this.retrieval = r;
  }

  /*
   * This should be run while we're waiting for the results. It either creates the
   * required data structures for the model, or resets them to be used for another query.
   *
   */
  public void initialize() throws Exception {
    // Stemming?
    if (parameters.get("stemming", true) && stemmer == null) {
      stemmer = new englishStemmer();
    }

    if (tokenizer == null) {
      tokenizer = new TagTokenizer();
    }
  }

  /*
   * Run this when the Relevance Model is no longer needed.
   */
  public void cleanup() throws Exception {
    tokenizer = null;
  }

  public ArrayList<WeightedTerm> generateGrams(List<ScoredDocument> initialResults) throws IOException {
    HashMap<Integer, Double> scores = logstoposteriors(initialResults);
    HashMap<String, HashMap<Integer, Integer>> counts = countGrams(initialResults);
    ArrayList<WeightedTerm> scored = scoreGrams(counts, scores);
    Collections.sort(scored);
    return scored;
  }

  public Node generateExpansionQuery(List<ScoredDocument> initialResults, int fbTerms,
          Set<String> exclusionTerms) throws IOException {
    List<WeightedTerm> scored = generateGrams(initialResults);
    ArrayList<Node> newChildren = new ArrayList<Node>();
    NodeParameters expParams = new NodeParameters();
    int expanded = 0;

    // Time to construct the modified query - start with the expansion since we always have it
    for (int i = 0; i < scored.size() && expanded < fbTerms; i++) {
      Gram g = (Gram) scored.get(i);
      if (exclusionTerms.contains(g.term)) {
        continue;
      }
      Node inner = TextPartAssigner.assignPart(new Node("extents", g.term),
              this.retrieval.getGlobalParameters(),
              this.retrieval.getAvailableParts());
      ArrayList<Node> innerChild = new ArrayList<Node>();
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
    String term;
    for (ScoredDocument sd : results) {
      doc = retrieval.getDocument(retrieval.getDocumentName(sd.document), new Parameters());
      tokenizer.tokenize(doc);
      for (String s : doc.terms) {
        if (stemmer == null) {
          term = s;
        } else {
          stemmer.setCurrent(s);
          stemmer.stem();
          term = stemmer.getCurrent();
        }
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
          lengthCache.put(docID, retrieval.getDocumentLength(docID));
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
}
