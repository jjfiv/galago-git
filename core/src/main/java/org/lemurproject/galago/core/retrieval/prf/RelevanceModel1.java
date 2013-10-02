/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.prf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.tokenize.Tokenizer;
import org.lemurproject.galago.core.parse.stem.KrovetzStemmer;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.core.retrieval.GroupRetrieval;
import org.lemurproject.galago.core.retrieval.Results;
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
public class RelevanceModel1 implements ExpansionModel {

  private static final Logger logger = Logger.getLogger("RM1");
  private final Retrieval retrieval;
  private int defaultFbDocs;
  private int defaultFbTerms;
  private Set<String> exclusionTerms;
  private Set<String> inclusionTerms;
  private Stemmer stemmer;
  private Tokenizer tokenizer;

  public RelevanceModel1(Retrieval r) throws Exception {
    this.retrieval = r;
    defaultFbDocs = (int) Math.round(r.getGlobalParameters().get("fbDocs", 10.0));
    defaultFbTerms = (int) Math.round(r.getGlobalParameters().get("fbTerm", 5.0));
    exclusionTerms = WordLists.getWordList(r.getGlobalParameters().get("rmstopwords", "rmstop"));
    inclusionTerms = null;
    Parameters gblParms = r.getGlobalParameters();
    
    if (gblParms.isString("rmwhitelist")){
        inclusionTerms = WordLists.getWordList(r.getGlobalParameters().getString("rmwhitelist"));
    }
    tokenizer = Tokenizer.instance(gblParms);
    if (r.getGlobalParameters().isString("rmStemmer")) {
      String rmstemmer = r.getGlobalParameters().getString("rmStemmer");
      stemmer = (Stemmer) Class.forName(rmstemmer).getConstructor().newInstance();
    } else {
      stemmer = new KrovetzStemmer();
    }
  }

  @Override
  public Node expand(Node root, Parameters queryParameters) throws Exception {

    int fbDocs = (int) Math.round(root.getNodeParameters().get("fbDocs", queryParameters.get("fbDocs", (double) defaultFbDocs)));
    int fbTerms = (int) Math.round(root.getNodeParameters().get("fbTerm", queryParameters.get("fbTerm", (double) defaultFbTerms)));

    if (fbDocs <= 0 || fbTerms <= 0) {
      logger.info("fbDocs, or fbTerms is invalid, no expansion possible. (<= 0)");
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
    if (initialResults.isEmpty()) {
        return root;
    }
    // extract grams from results
    Set<String> stemmedQueryTerms = stemTerms(StructuredQuery.findQueryTerms(transformed));
    Set<String> exclusions = (fbParams.isString("rmstopwords")) ? WordLists.getWordList(fbParams.getString("rmstopwords")) : exclusionTerms;
    Set<String> inclusions = null;
    if (fbParams.isString("rmwhitelist")){
        inclusions = WordLists.getWordList(fbParams.getString("rmwhitelist"));
    } else {
        inclusions = inclusionTerms;
    }
    
    List<WeightedTerm> weightedTerms = extractGrams(initialResults, fbParams, stemmedQueryTerms, exclusions, inclusions);

    // select some terms to form exp query node
    Node expNode = generateExpansionQuery(weightedTerms, fbTerms);

    return expNode;
  }

  public List<ScoredDocument> collectInitialResults(Node transformed, Parameters fbParams) throws Exception {
      Results results = retrieval.executeQuery(transformed, fbParams);
      List<ScoredDocument> res = results.scoredDocuments;
      return res;
  }

  public List<WeightedTerm> extractGrams(List<ScoredDocument> initialResults, Parameters fbParams, Set<String> queryTerms, Set<String> exclusionTerms, Set<String> inclusionTerms) throws IOException {
    // convert documentScores to posterior probs
    Map<ScoredDocument, Double> scores = logstoposteriors(initialResults);

    // get term frequencies in documents
    Map<String, Map<ScoredDocument, Integer>> counts = countGrams(initialResults, fbParams, queryTerms, exclusionTerms, inclusionTerms);

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

  protected Map<String, Map<ScoredDocument, Integer>> countGrams(List<ScoredDocument> results, Parameters fbParams, Set<String> stemmedQueryTerms, Set<String> exclusionTerms, Set<String> inclusionTerms) throws IOException {
    Map<String, Map<ScoredDocument, Integer>> counts = new HashMap<String, Map<ScoredDocument, Integer>>();
    Map<ScoredDocument, Integer> termCounts;
    Document doc;

    DocumentComponents corpusParams = new DocumentComponents(true, false, false);

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

      // only need terms, so just tokenize here.
      tokenizer.tokenize(doc);
      List<String> docterms;
      docterms = doc.terms;

      sd.annotation = new AnnotatedNode();
      sd.annotation.extraInfo = "" + docterms.size();

      for (String term : docterms) {
        // perform stopword and query term filtering here //
        if (inclusionTerms != null && !inclusionTerms.contains(term)) {
            continue; // not on the whitelist
        }
        if (stemmedQueryTerms.contains(stemmer.stem(term)) || exclusionTerms.contains(term)) {
          continue; // on the blacklist
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
