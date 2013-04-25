package org.lemurproject.galago.contrib.relevancemodels;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Deduplication strategy based on x% unigram overlap.
 * <p/>
 * Call {@link #docFilter} to test and add new documents
 * <p/>
 * Call {@link #reset()} to reset the document cache
 *<p/>
 * @author dietz
 */
public class UnigramDeDuplicator {
  protected ArrayList<HashMap<String, Integer>> acceptedTermCountSet = new ArrayList<HashMap<String, Integer>>();
  protected double dedupeScoreThresh;

  public UnigramDeDuplicator(double dedupeScoreThresh) {
    this.dedupeScoreThresh = dedupeScoreThresh;
  }

  public boolean docFilter(List<String> docterms) {
    // convert to term -> count map
    HashMap<String, Integer> testTermCounts = new HashMap<String, Integer>();
    for(String term:docterms){
      if(testTermCounts.containsKey(term)) {
        testTermCounts.put(term, testTermCounts.get(term)+1);
      } else {
        testTermCounts.put(term, 1);
      }
    }

    int doctermLength = docterms.size();
    boolean reject = false;
    double dupeScore = 0.0;
    for(HashMap<String, Integer> acceptedTermCounts: acceptedTermCountSet){
      int hits = 0;
      int acceptedTermSetLength = 0;

      for(int c :acceptedTermCounts.values())  acceptedTermSetLength+=c;


      for(String term:testTermCounts.keySet()){
        if(acceptedTermCounts.containsKey(term)){
          int minCount = Math.min(acceptedTermCounts.get(term), testTermCounts.get(term));
          hits += minCount;
        }
      }

      double score = 1. * hits / Math.min(acceptedTermSetLength, doctermLength);
      dupeScore = Math.max(dupeScore, score);

      if(score > dedupeScoreThresh) {
        reject = true;
        break;
      }
    }

    if(!reject){
      acceptedTermCountSet.add(testTermCounts);
    }

    return reject;
  }

  public void reset() {
    acceptedTermCountSet.clear();
  }
}
