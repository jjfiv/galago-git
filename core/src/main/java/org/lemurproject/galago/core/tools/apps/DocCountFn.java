/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.PrintStream;
import java.util.List;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author sjh
 */
public class DocCountFn extends AppFunction {

  @Override
  public String getName() {
    return "doccount";
  }

  @Override
  public String getHelpString() {
    return "galago doccount --x=<countable-query> --index=<index> \n\n"
            + "  Returns the number of documents that contain the countable-query.\n"
            + "  More than one index and expression can be specified.\n"
            + "  Examples of countable-expressions: terms, ordered windows and unordered windows.\n";
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    if (!p.containsKey("index") || !p.containsKey("x")) {
      output.println(this.getHelpString());
      return;
    }

    Retrieval r = RetrievalFactory.instance(p);

    long count;
    for (String query : (List<String>) p.getList("x")) {
      Node parsed = StructuredQuery.parse(query);
      parsed.getNodeParameters().set("queryType", "count");
      Node transformed = r.transformQuery(parsed, Parameters.instance());

      if (p.get("printTransformation", false)) {
        System.err.println(query);
        System.err.println(parsed);
        System.err.println(transformed);
      }

      count = r.getNodeStatistics(transformed).nodeDocumentCount;
      output.println(count + "\t" + query);
    }
    r.close();
  }
}
