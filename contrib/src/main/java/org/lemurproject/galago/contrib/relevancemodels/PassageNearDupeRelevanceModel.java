package org.lemurproject.galago.contrib.relevancemodels;

import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.tupleflow.Parameters;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * Implements a document deduplication and term whitelisting.
 * <p/>
 * Document deduplication is based on 95% unigram overlap with a previously accepted document.
 * <p/>
 * If a whitelist is given, the relevance model is only built on terms in the whitelist.
 *<p/>
 * This relevance model also supports learning a relevance model on passage retrieval.
 * <p/>
 *
 *
 * <h2>Parameters</h2>
 * <ul>
 *  <li>dedupeScoreThresh: threshold for unigram overlap to be counted as a duplicate. Default 0.95.
 *  <li>docdedupe: true/false to switch document deduplication on/off
 *  <li>termWhitelistFile: filename of file containing the whitelist for terms (whitespace separated)
 *  </ul>
 *
 * @author dietz
 * */
public class PassageNearDupeRelevanceModel extends AbstractDedupeRelevanceModel {
  protected UnigramDeDuplicator unigramDeDuplicator = null;
  private final boolean docDedupe;
  protected boolean useTermFilter;
  private final HashSet<String> whitelist;

  public PassageNearDupeRelevanceModel(Parameters parameters, Retrieval r) {
    super(parameters, r);
    double dedupeScoreThresh = parameters.get("dedupeScoreThresh", r.getGlobalParameters().get("dedupeScoreThresh", 0.95));
    docDedupe = parameters.get("docdedupe", r.getGlobalParameters().get("docdedupe",false));
    String termWhitelistFile = parameters.get("termWhitelistFile",r.getGlobalParameters().get("termWhitelistFile",""));

    whitelist = new HashSet<String>();
    if(termWhitelistFile.length()>0 && new File(termWhitelistFile).exists()){
      try {
        BufferedReader reader = new BufferedReader(new FileReader(new File(termWhitelistFile)));
        String line = null;
        while((line = reader.readLine()) != null){
          Collections.addAll(whitelist, line.split("\\s+"));
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      useTermFilter = !whitelist.isEmpty();

    }
    if(docDedupe) {
      unigramDeDuplicator = new UnigramDeDuplicator(dedupeScoreThresh);
    }

  }

    @Override
  protected List<String> termFilter(ScoredDocument sd, List<String> docterms) {
      if(useTermFilter) {
        ArrayList<String> filteredTerms = new ArrayList<String>();
        for(String term:docterms){
          if(whitelist.contains(term)) filteredTerms.add(term);
        }
        return filteredTerms;

      } else return docterms;
    }


  @Override
  protected boolean docFilter(ScoredDocument sd, List<String> docterms) {
    if(docDedupe)   return unigramDeDuplicator.docFilter(docterms);
    else return false;
  }

  @Override
  protected void docFilterReset() {
    if(unigramDeDuplicator != null) unigramDeDuplicator.reset();
  }
}
