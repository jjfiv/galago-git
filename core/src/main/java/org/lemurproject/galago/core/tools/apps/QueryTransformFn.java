package org.lemurproject.galago.core.tools.apps;

import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.tools.AppFunction;

import java.io.PrintStream;
import java.util.List;

/**
 * @author jfoley
 */
public class QueryTransformFn extends AppFunction {
  @Override
  public String getName() {
    return "query-transform";
  }

  @Override
  public String getHelpString() {
    return "galago query-transform --q=<query> --index=<index> \n\n"
        + "  Returns the parsed and transformed queries.\n";
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    if (!p.containsKey("index") || !p.containsKey("q")) {
      output.println(this.getHelpString());
      return;
    }

    Retrieval r = RetrievalFactory.create(p);

    for (String query : (List<String>) p.getAsList("q")) {
      System.out.println(query);
      Node parsed = StructuredQuery.parse(query);
      System.out.println(parsed);
      Node transformed = r.transformQuery(parsed, Parameters.create());
      System.out.println(transformed);
    }
    r.close();
  }
}
