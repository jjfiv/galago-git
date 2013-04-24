// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.contrib.relevancemodels;

import info.bliki.wiki.tags.SourceTag;
import org.lemurproject.galago.core.eval.stat.NaturalOrderComparator;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.TagTokenizer;
import org.lemurproject.galago.core.parse.stem.Porter2Stemmer;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.core.retrieval.GroupRetrieval;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.ScoredPassage;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.scoring.ExpansionModel;
import org.lemurproject.galago.core.scoring.WeightedTerm;
import org.lemurproject.galago.core.util.MathUtils;
import org.lemurproject.galago.core.util.TextPartAssigner;
import org.lemurproject.galago.tupleflow.Parameters;

import java.io.IOException;
import java.util.*;

/**
 * Implements a unigram Relevance Model which can be extended to filter
 * documents and terms.
 * <p/>
 * @author dietz
 */
abstract public class AbstractDedupeRelevanceModel implements ExpansionModel {

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
  protected Parameters parameters;
  protected Retrieval retrieval;
  protected TagTokenizer tokenizer = null;
  protected Stemmer stemmer;
  protected String group;

  public AbstractDedupeRelevanceModel(Parameters parameters, Retrieval r) {
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
    HashMap<ScoredDocument, Double> scores = logstoposteriors(initialResults);

    // get term frequencies in documents
    HashMap<String, HashMap<ScoredDocument, Integer>> counts = countGrams(initialResults);

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
  protected HashMap<ScoredDocument, Double> logstoposteriors(List<ScoredDocument> results) {
    HashMap<ScoredDocument, Double> scores = new HashMap<ScoredDocument, Double>();
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
      scores.put(sd, Math.exp(logPosterior));
    }

    return scores;
  }

  protected HashMap<String, HashMap<ScoredDocument, Integer>> countGrams(List<ScoredDocument> results) throws IOException {
    HashMap<String, HashMap<ScoredDocument, Integer>> counts = new HashMap<String, HashMap<ScoredDocument, Integer>>();
    HashMap<ScoredDocument, Integer> termCounts;
    Document doc;

    docFilterReset();
    for (ScoredDocument sd : results) {

      if (group != null && retrieval instanceof GroupRetrieval) {
        doc = ((GroupRetrieval) retrieval).getDocument(sd.documentName, Parameters.parse("{\"text\":true}"), group);
      } else {
        doc = retrieval.getDocument(sd.documentName, Parameters.parse("{\"text\":true}"));
      }

      tokenizer.tokenize(doc);
      List<String> docterms;
      if (sd instanceof ScoredPassage) {
        ScoredPassage sp = (ScoredPassage) sd;
        docterms = doc.terms.subList(sp.begin, sp.end);
      } else {
        docterms = doc.terms;
      }

      if (!docFilter(sd, docterms)) {
        docterms = termFilter(sd, docterms);

        for (String term : docterms) {
          if (!counts.containsKey(term)) {
            counts.put(term, new HashMap<ScoredDocument, Integer>());
          }
          termCounts = counts.get(term);
          if (termCounts.containsKey(sd)) {
            termCounts.put(sd, termCounts.get(sd) + 1);
          } else {
            termCounts.put(sd, 1);
          }
        }
      }
    }

    return counts;
  }

  protected abstract List<String> termFilter(ScoredDocument sd, List<String> docterms);

  protected abstract boolean docFilter(ScoredDocument sd, List<String> docterms);

  protected abstract void docFilterReset();

  protected ArrayList<WeightedTerm> scoreGrams(HashMap<String, HashMap<ScoredDocument, Integer>> counts,
          HashMap<ScoredDocument, Double> scores) throws IOException {
    ArrayList<WeightedTerm> grams = new ArrayList<WeightedTerm>();
    HashMap<ScoredDocument, Integer> termCounts;
    HashMap<ScoredDocument, Integer> lengthCache = new HashMap<ScoredDocument, Integer>();

    for (String term : counts.keySet()) {
      Gram g = new Gram(term);
      termCounts = counts.get(term);

      for (ScoredDocument sd : termCounts.keySet()) {
        if (!lengthCache.containsKey(sd)) {
          int docLen;
          if (sd instanceof ScoredPassage) {
            ScoredPassage sp = (ScoredPassage) sd;
            docLen = sp.end - sp.begin;
          } else if (group != null && retrieval instanceof GroupRetrieval) {
            docLen = ((GroupRetrieval) retrieval).getDocumentLength(sd.documentName, group);
          } else {
            docLen = retrieval.getDocumentLength(sd.documentName);
          }

          lengthCache.put(sd, docLen);
        }
        int length = lengthCache.get(sd);
        g.score += scores.get(sd) * termCounts.get(sd) / length;
      }
      // 1 / fbDocs from the RelevanceModel source code
      g.score *= (1.0 / scores.size());
      grams.add(g);
    }

    Collections.sort(grams);
    for (WeightedTerm g : grams) {
      System.out.println("grams = " + g);
    }
    return grams;
  }

  private Set<String> stemTerms(Set<String> terms) {
    HashSet<String> stems = new HashSet<String>(terms.size());
    for (String t : terms) {
      String s = stemmer.stem(t);
      // stemmers should ensure that terms do not stem to nothing.
      stems.add(s);
    }
    return stems;
  }
}
