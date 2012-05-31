/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.learning.letor;

import java.io.PrintStream;
import java.util.List;
import org.lemurproject.galago.core.eval.QuerySetJudgments;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.tools.App.AppFunction;
import org.lemurproject.galago.core.tools.BatchSearch;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Using an index generates an output file for learning to rank - feature set is
 * based on letor
 *
 * @author sjh
 */
public class Letor extends AppFunction {

  @Override
  public String getHelpString() {
    return "galago generate-letor <parameters>\n\n";
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    Retrieval r = RetrievalFactory.instance(p);
    List<Parameters> queries = BatchSearch.collectQueries(p);
    QuerySetJudgments qrels = new QuerySetJudgments(p.getString("qrels"));

    for (Parameters q : queries) {
    }
  }
}
