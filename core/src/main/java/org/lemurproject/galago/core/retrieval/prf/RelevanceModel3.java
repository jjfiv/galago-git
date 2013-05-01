/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.prf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.TagTokenizer;
import org.lemurproject.galago.core.parse.stem.Porter2Stemmer;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.core.retrieval.GroupRetrieval;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.util.MathUtils;
import org.lemurproject.galago.core.util.WordLists;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class RelevanceModel3 implements ExpansionModel {

  private static final Logger logger = Logger.getLogger("RM3");
  private final Retrieval retrieval;
  private double defaultFbOrigWeight;
  private int defaultFbDocs;
  private int defaultFbTerms;
  private Set<String> exclusionTerms;
  private Stemmer stemmer;
  private TagTokenizer tokenizer;

  public RelevanceModel3(Retrieval r) throws Exception {
    this.retrieval = r;

    defaultFbOrigWeight = r.getGlobalParameters().get("fbOrigWeight", 0.85);
    defaultFbDocs = (int) Math.round(r.getGlobalParameters().get("fbDocs", 10.0));
    defaultFbTerms = (int) Math.round(r.getGlobalParameters().get("fbTerm", 5.0));

    exclusionTerms = WordLists.getWordList(r.getGlobalParameters().get("rmstopwords", "rmstop"));

    tokenizer = new TagTokenizer();
    if (r.getGlobalParameters().isString("rmStemmer")) {
      String rmstemmer = r.getGlobalParameters().getString("rmStemmer");
      stemmer = (Stemmer) Class.forName(rmstemmer).getConstructor().newInstance();
    } else {
      stemmer = new Porter2Stemmer();
    }
  }

  @Override
  public Node expand(Node root, Parameters queryParameters) throws Exception {

    double fbOrigWeight = queryParameters.get("fbOrigWeight", defaultFbOrigWeight);
    int fbDocs = (int) Math.round(queryParameters.get("fbDocs", (double) defaultFbDocs));
    int fbTerms = (int) Math.round(queryParameters.get("fbTerm", (double) defaultFbTerms));

    fbOrigWeight = root.getNodeParameters().get("fbOrigWeight", fbOrigWeight);
    fbDocs = (int) Math.round(root.getNodeParameters().get("fbDocs", (double) fbDocs));
    fbTerms = (int) Math.round(root.getNodeParameters().get("fbTerm", (double) fbTerms));

    if (fbOrigWeight == 1.0 || fbDocs <= 0 || fbTerms <= 0) {
      logger.info("fbOrigWeight, fbDocs, or fbTerms is invalid (<= 0)");
      return root;
    }

    // transform query to ensure it will run
    Parameters fbParams = new Parameters();
    fbParams.set("requested", fbDocs);
    // first pass is asserted to be document level
    fbParams.set("passageQuery", false);
    fbParams.set("extentQuery", false);
    fbParams.setBackoff(queryParameters);

    Node transformed = retrieval.transformQuery(root.clone(), fbParams);

    // get some initial results
    List<ScoredDocument> initialResults = collectInitialResults(transformed, fbParams);

    // extract grams from results
    Set<String> stemmedQueryTerms = stemTerms(StructuredQuery.findQueryTerms(transformed));
    Set<String> exclusions = (fbParams.isString("rmstopwords")) ? WordLists.getWordList(fbParams.getString("rmstopwords")) : exclusionTerms;
    List<WeightedTerm> weightedTerms = extractGrams(initialResults, fbParams, stemmedQueryTerms, exclusions);

    // select some terms to form exp query node
    Node expNode = generateExpansionQuery(weightedTerms, fbTerms);

    Node rm3 = new Node("combine");
    rm3.addChild(root);
    rm3.addChild(expNode);

    rm3.getNodeParameters().set("0", fbOrigWeight);
    rm3.getNodeParameters().set("1", 1.0 - fbOrigWeight);

    return rm3;
  }

  public List<ScoredDocument> collectInitialResults(Node transformed, Parameters fbParams) throws Exception {
    ScoredDocument[] res = retrieval.runQuery(transformed, fbParams);
    return Arrays.asList(res);
  }

  public List<WeightedTerm> extractGrams(List<ScoredDocument> initialResults, Parameters fbParams, Set<String> queryTerms, Set<String> exclusionTerms) throws IOException {
    // convert documentScores to posterior probs
    Map<ScoredDocument, Double> scores = logstoposteriors(initialResults);

    // get term frequencies in documents
    Map<String, Map<ScoredDocument, Integer>> counts = countGrams(initialResults, fbParams, queryTerms, exclusionTerms);

    // compute term weights
    List<WeightedTerm> scored = scoreGrams(counts, scores);

    // sort by weight
    Collections.sort(scored);

    return scored;
  }

  public Node generateExpansionQuery(List<WeightedTerm> weightedTerms, int fbTerms) throws IOException {
    Node expNode = new Node("combine");
    for (int i = 0; i < Math.min(weightedTerms.size(), fbTerms); i++) {
      Node expChild = new Node("text", weightedTerms.get(i).getTerm());
      expNode.addChild(expChild);
      expNode.getNodeParameters().set("" + i, weightedTerms.get(i).getWeight());
    }
    return expNode;
  }

  // Implementation here is identical to the Relevance Model unigram normaliztion in Indri.
  // See RelevanceModel.cpp for details
  protected Map<ScoredDocument, Double> logstoposteriors(List<ScoredDocument> results) {
    Map<ScoredDocument, Double> scores = new HashMap<ScoredDocument, Double>();
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

  protected Map<String, Map<ScoredDocument, Integer>> countGrams(List<ScoredDocument> results, Parameters fbParams, Set<String> stemmedQueryTerms, Set<String> exclusionTerms) throws IOException {
    Map<String, Map<ScoredDocument, Integer>> counts = new HashMap<String, Map<ScoredDocument, Integer>>();
    Map<ScoredDocument, Integer> termCounts;
    Document doc;

    Parameters corpusParams = new Parameters();
    corpusParams.set("text", true);
    corpusParams.set("tags", false);
    corpusParams.set("terms", false);
    corpusParams.set("metadata", false);

    String group = fbParams.get("group", (String) null);

    for (ScoredDocument sd : results) {
      if (group != null && retrieval instanceof GroupRetrieval) {
        doc = ((GroupRetrieval) retrieval).getDocument(sd.documentName, corpusParams, group);
      } else {
        doc = retrieval.getDocument(sd.documentName, corpusParams);
      }

      if (doc == null) {
        logger.info("Failed to retrieve document: " + sd.documentName + " -- RM skipping document.");
        continue;
      }

      tokenizer.tokenize(doc);
      List<String> docterms;
      docterms = doc.terms;

      sd.annotation = new AnnotatedNode();
      sd.annotation.extraInfo = "" + docterms.size();

      for (String term : docterms) {
        // perform stopword and query term filtering here //
        if (stemmedQueryTerms.contains(stemmer.stem(term)) || exclusionTerms.contains(term)) {
          continue;
        }

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
    return counts;
  }

  protected List<WeightedTerm> scoreGrams(Map<String, Map<ScoredDocument, Integer>> counts, Map<ScoredDocument, Double> scores) throws IOException {
    List<WeightedTerm> grams = new ArrayList<WeightedTerm>();
    Map<ScoredDocument, Integer> termCounts;

    for (String term : counts.keySet()) {
      Gram g = new Gram(term);
      termCounts = counts.get(term);
      for (ScoredDocument sd : termCounts.keySet()) {
        // we forced this into the scored document earlier
        int length = Integer.parseInt(sd.annotation.extraInfo);
        g.score += scores.get(sd) * termCounts.get(sd) / length;
      }
      // 1 / fbDocs from the RelevanceModel source code
      g.score *= (1.0 / scores.size());
      grams.add(g);
    }

    return grams;
  }

  private Set<String> stemTerms(Set<String> terms) {
    Set<String> stems = new HashSet<String>(terms.size());
    for (String t : terms) {
      String s = stemmer.stem(t);
      // stemmers should ensure that terms do not stem to nothing.
      stems.add(s);
    }
    return stems;
  }

  // implementation of weighted term (term, score) pairs
  public static class Gram implements WeightedTerm {

    public String term;
    public double score;

    public Gram(String t) {
      term = t;
      score = 0.0;
    }

    @Override
    public String getTerm() {
      return term;
    }

    @Override
    public double getWeight() {
      return score;
    }

    // The secondary sort is to have defined behavior for statistically tied samples.
    @Override
    public int compareTo(WeightedTerm other) {
      Gram that = (Gram) other;
      int result = this.score > that.score ? -1 : (this.score < that.score ? 1 : 0);
      if (result != 0) {
        return result;
      }
      result = (this.term.compareTo(that.term));
      return result;
    }

    @Override
    public String toString() {
      return "<" + term + "," + score + ">";
    }
  }
}
