/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.processing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class TwoPassDocumentModel extends ProcessingModel {

  private LocalRetrieval retrieval;
  private ProcessingModel firstPassDefault;
  private ProcessingModel firstPassMaxScore;
  private ProcessingModel secondPassDefault;
  private long topK = 10000;

  public TwoPassDocumentModel(LocalRetrieval lr) {
    retrieval = lr;
    firstPassDefault = new RankedDocumentModel(retrieval);
    firstPassMaxScore = new MaxScoreDocumentModel(retrieval);
    secondPassDefault = new WorkingSetDocumentModel(retrieval);
    topK = retrieval.getGlobalParameters().get("twoPassK", 10000);
  }

  @Override
  public ScoredDocument[] execute(Node queryTree, Parameters queryParams) throws Exception {
    Parameters firstPassParams = queryParams.clone();
    firstPassParams.set("requested", Math.max(topK, queryParams.get("requested", 1000)));
    // ensure the firstpass query is not mistaken for a delta ready query
    if (firstPassParams.containsKey("deltaReady")) {
      firstPassParams.remove("deltaReady");
    }
    
    // it would be nice to automatically generate these firstPassQueries,
    //   but there could be some complication in constructing a fast approximation
    //   of the query.
    Node firstPassQuery = StructuredQuery.parse(queryParams.getString("firstPassQuery"));
    firstPassQuery = retrieval.transformQuery(firstPassQuery, firstPassParams);

    ScoredDocument[] results;
    if (firstPassParams.get("deltaReady", false)) {
      results = firstPassMaxScore.execute(firstPassQuery, firstPassParams);
    } else {
      results = firstPassDefault.execute(firstPassQuery, firstPassParams);
    }

    List<Long> workingSet = resultsToWorkingSet(results);
    queryParams.set("working", workingSet);
    results = secondPassDefault.execute(queryTree, queryParams);

    return results;
  }

  private List<Long> resultsToWorkingSet(ScoredDocument[] results) {
    ArrayList<Long> ws = new ArrayList();
    for (int i = 0; i < results.length; i++) {
      ws.add((long) results[i].document);
    }
    Collections.sort(ws);
    return ws;
  }
}
