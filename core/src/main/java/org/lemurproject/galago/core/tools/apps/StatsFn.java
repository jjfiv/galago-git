/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.PrintStream;
import java.util.List;
import java.util.Set;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class StatsFn extends AppFunction {

  @Override
  public String getName() {
    return "stats";
  }

  @Override
  public String getHelpString() {
    return "galago stats --index=/path/to/index [options]\n\n"
            + "\t--part+[partName]\n"
            + "\t--field+[fieldName]\n"
            + "\t--node+[countableNode]\n"
            + "\n\n"
            + "If no options are specified, output will be the part statistics"
            + "for the default postings part.\n";
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    if (!p.containsKey("index")) {
      output.print(getHelpString());
      return;
    }

    Retrieval r = RetrievalFactory.instance(p);

    // TODO: this should be in the retrieval interface...
    if (!p.containsKey("part")
            && !p.containsKey("field")
            && !p.containsKey("node")) {
      Set<String> available = r.getAvailableParts().getKeys();
      if (available.contains("postings.porter")) {
        p.set("part", "postings.porter");
      } else if (available.contains("postings.krovetz")) {
        p.set("part", "postings.krovetz");
      } else if (available.contains("postings")) {
        p.set("part", "postings");
      } else {
        output.print(getHelpString());
        output.println("Could not determine default part.");
        return;
      }
    }

    int statItemCount = 0;

    output.println("{");
    for (String part : (List<String>) p.getAsList("part")) {
      if (statItemCount > 0) {
        output.println("  ,");
      }
      statItemCount += 1;

      output.println("  \"" + part + "\" : ");
      
      try {
        output.println(r.getIndexPartStatistics(part).toParameters().toPrettyString("    "));
      } catch (IllegalArgumentException e) {
        System.err.println(e.toString());
      }
    }
    for (String field : (List<String>) p.getAsList("field")) {
      if (statItemCount > 0) {
        output.println("  ,");
      }
      statItemCount += 1;

      output.println("  \"" + field + "\" : ");

      Node n = StructuredQuery.parse(field);
      // It would be nice to make the traversals deal with this corrently.
      //n = r.transformQuery(n, new Parameters());

      // however, currently - I'm only willing to fix one type of lengths node:
      if(n.getOperator().equals("text")){
        n.setOperator("lengths");
        n.getNodeParameters().set("part", "lengths");
      }

      try {
        output.println(r.getCollectionStatistics(n).toParameters().toPrettyString("    "));
      } catch (IllegalArgumentException e) {
        System.err.println(e.toString());
      }
    }
    for (String node : (List<String>) p.getAsList("node")) {
      if (statItemCount > 0) {
        output.println("  ,");
      }
      statItemCount += 1;

      Node n = StructuredQuery.parse(node);
      n.getNodeParameters().set("queryType", "count");
      n = r.transformQuery(n, new Parameters());

      output.println("  \"" + node + "\" : ");
      try {
        output.println(r.getNodeStatistics(n).toParameters().toPrettyString("    "));
      } catch (IllegalArgumentException e) {
        System.err.println(e.toString());
      }
    }
    output.println("}");
  }
}
