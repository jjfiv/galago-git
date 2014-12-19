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
public class TransformQueryFn extends AppFunction {

  @Override
  public String getName() {
    return "transform";
  }

  @Override
  public String getHelpString() {
    return "galago transform --index=./index --query=\"#sdm(query terms)\" --format=<mode>\n"
            + "\n"
            + "\tformat : [machine, pretty, simple]\n"
            + "\n";
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    Retrieval r = RetrievalFactory.create(p);
    List<Parameters> queries = BatchSearch.collectQueries(p);

    String printMode = p.get("format", "pretty");

    for (Parameters q : queries) {
      String id = q.getString("number");
      String txt = q.getString("text");
      Node raw = StructuredQuery.parse(txt);
      Node trans = r.transformQuery(raw, q);

      if (p.get("id", true)) {
        output.print(id + "\t");
      }

      if (printMode.startsWith("m")) {
        output.println(trans.toString());
      } else if (printMode.startsWith("p")) {
        output.println(trans.toPrettyString());
      } else if (printMode.startsWith("s")) {
        output.println(trans.toSimplePrettyString());
      } else {
        output.println("format: " + printMode + " unknown -- exiting.");
        return;
      }
    }
  }
}
