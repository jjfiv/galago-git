package org.lemurproject.galago.core.tools.apps;

import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.prf.RelevanceModel1;
import org.lemurproject.galago.core.retrieval.prf.WeightedTerm;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.core.util.WordLists;
import org.lemurproject.galago.utility.Parameters;

import java.io.PrintStream;
import java.util.List;
import java.util.Set;

/**
 * @author jfoley
 */
public class GetRMTermsFn extends AppFunction {
  @Override
  public String getName() {
    return "get-rm-terms";
  }

  @Override
  public String getHelpString() {
    return "get-rm-terms\n" +
    "\t--query=?\n" +
    "\t--numTerms=?\n" +
    "\t--index=?\n\n";
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    int numTerms = p.get("numTerms", 10);
    Node query = StructuredQuery.parse(p.getString("query"));
    Retrieval ret = RetrievalFactory.create(p);
    Stemmer stemmer = RelevanceModel1.getStemmer(p, ret);

    Node xquery = ret.transformQuery(query, p);
    List<ScoredDocument> initialResults = ret.executeQuery(xquery, p).scoredDocuments;

    System.err.println("Found "+initialResults.size()+" results for "+query);

    Set<String> stemmedQueryTerms = RelevanceModel1.stemTerms(stemmer, StructuredQuery.findQueryTerms(xquery));
    Set<String> exclusions = WordLists.getWordList(p.get("rmstopwords", "rmstop"));
    Set<String> inclusions = null; // no whitelist

    List<WeightedTerm> weightedTerms = RelevanceModel1.extractGrams(ret, initialResults, stemmer, p, stemmedQueryTerms, exclusions, inclusions);

    for(int i=0; i<weightedTerms.size() && i<numTerms; i++) {
      WeightedTerm wt = weightedTerms.get(i);
      System.out.printf("%s\t%f\n",wt.getTerm(), wt.getWeight());
    }

  }
}
