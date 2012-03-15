// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.tools.App.AppFunction;
import org.lemurproject.galago.core.util.CallTable;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class BatchSearch extends AppFunction {

  public static void main(String[] args) throws Exception {
    (new BatchSearch()).run(new Parameters(args), System.out);
  }

  @Override
  public String getHelpString() {
    return "galago batch-search <args>\n\n"
            + "  Runs a batch of queries against an index and produces TREC-formatted\n"
            + "  output.  The output can be used with retrieval evaluation tools like\n"
            + "  galago eval (org.lemurproject.galago.core.eval).\n\n"
            + "  Sample invocation:\n"
            + "     galago batch-search --index=/tmp/myindex --count=200 /tmp/queries\n\n"
            + "  Args:\n"
            + "     --index=path_to_your_index\n"
            + "     --count : Number of results to return for each query, default=1000\n"
            + "     /path/to/parameter/file : Input file in xml parameters format (see below).\n\n"
            + "  Query file format:\n"
            + "    The query file is an XML file containing a set of queries.  Each query\n"
            + "    has text tag, which contains the text of the query, and a number tag, \n"
            + "    which uniquely identifies the query in the output.\n\n"
            + "  Example query file:\n"
            + "  {\n"
            + "     \"queries\" : [\n"
            + "       {\n"
            + "         \"number\" : \"CACM-408\", \n"
            + "         \"text\" : \"#combine(my query)\"\n"
            + "       },\n"
            + "       {\n"
            + "         \"number\" : \"WIKI-410\", \n"
            + "         \"text\" : \"#combine(another query)\" \n"
            + "       }\n"
            + "    ]\n"
            + "  }\n";
  }

  @Override
  public void run(Parameters parameters, PrintStream out) throws Exception {
    if (!(parameters.containsKey("query")
            || parameters.containsKey("queries"))) {
      out.println(this.getHelpString());
      return;
    }

    List<Parameters> queries;
    if (parameters.containsKey("query")) {
      queries = (List<Parameters>) parameters.getList("query");
    } else {
      queries = (List<Parameters>) parameters.getList("queries");
    }

    // open index
    Retrieval retrieval = RetrievalFactory.instance(parameters);

    // record results requested
    int requested = (int) parameters.get("requested", 1000);
    long starttime = System.currentTimeMillis();
    long sumtime = 0;

    // for each query, run it, get the results, print in TREC format
    if (parameters.containsKey("seed")) {
      long seed = parameters.getLong("seed");
      Random r = new Random(seed);
      Collections.shuffle(queries, r);
    }

    for (Parameters query : queries) {
      String queryText = query.getString("text");
      Parameters p = new Parameters();
      p.set("requested", requested);

      // This is slow - need to improve it, but later
      if (parameters.containsKey("working")) {
        p.set("working", parameters.getList("working"));
      }
      Node root = StructuredQuery.parse(queryText);
      Node transformed = retrieval.transformQuery(root, p);

      if (parameters.get("printTransformation", false)) {
        System.err.println("Text:" + queryText);
        System.err.println("Parsed Node:" + root.toString());
        System.err.println("Transformed Node:" + transformed.toString());
      }

      long querystarttime = System.currentTimeMillis();
      ScoredDocument[] results = retrieval.runQuery(transformed, p);
      long queryendtime = System.currentTimeMillis();
      sumtime += queryendtime - querystarttime;

      if (results != null) {
        for (int i = 0; i < results.length; i++) {
          double score = results[i].score;
          int rank = i + 1;

          out.format("%s Q0 %s %d %s galago\n", query.getString("number"), results[i].documentName, rank,
                  formatScore(score));
        }
      }
      if (parameters.get("print_calls", false)) {
        CallTable.print(System.out, query.getString("number"));
      }
      CallTable.reset();
    }

    long endtime = System.currentTimeMillis();

    if (parameters.get("time", false)) {
      System.err.println("TotalTime: " + (endtime - starttime));
      System.err.println("AvgTime: " + ((endtime - starttime) / queries.size()));
      System.err.println("AvgQueryTime: " + (sumtime / queries.size()));
    }
  }

  private static String formatScore(double score) {
    double difference = Math.abs(score - (int) score);

    if (difference < 0.00001) {
      return Integer.toString((int) score);
    }
    return String.format("%10.8f", score);
  }
  
}
