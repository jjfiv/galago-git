// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.tools.App.AppFunction;
import org.lemurproject.galago.core.util.CallTable;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Parameters.Type;

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
            + "     galago batch-search --index=/tmp/myindex --requested=200 /tmp/queries\n\n"
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

    // collect queries
    List<Parameters> queries = collectQueries(parameters);

    // open index
    Retrieval retrieval = RetrievalFactory.instance(parameters);

    // prepare output
    ScoredDocument[] results = null;

    // Look for a query range - these should be list indexes, not qids
    //  e.g.:
    //    --range=5-10
    if (parameters.containsKey("range")) {
      String[] parts = parameters.getString("range").split("-");
      int firstQuery = Integer.parseInt(parts[0]);
      int lastQuery = Integer.parseInt(parts[1]);
      // restrict the list of queries to these offsets
      queries = queries.subList(firstQuery, lastQuery);
    }

    // repeats dictate the number of times to run this set of queries
    //  - they are run as k iterations of the query batch
    int repeats = (int) parameters.get("repeats", 1);

    // record results requested
    int requested = (int) parameters.get("requested", 1000);

    // prepare timing variables
    boolean timing = false;
    Map<String, List<Long>> times = new HashMap();
    ArrayList<Long> batchTimes = new ArrayList();
    if (parameters.isString("time")) {
      timing = true;
    } else if (parameters.isBoolean("time")) {
      timing = parameters.getBoolean("time");
    }

    // for each query, run it, get the results, print in TREC format
    Random r = null;
    if (parameters.containsKey("seed")) {
      long seed = parameters.getLong("seed");
      r = new Random(seed);
    }

    for (int rep = 0; rep < repeats; rep++) {
      long batchStartTime = System.currentTimeMillis();
      if (r != null) {
        Collections.shuffle(queries, r);
      }

      for (Parameters queryParameters : queries) {
        String queryText = queryParameters.getString("text");

        queryParameters.set("requested", requested);

        // This is slow - need to improve it, but later
        if (parameters.containsKey("working")) {
          queryParameters.set("working", parameters.getList("working"));
        }

        if (rep == 0) {
          CallTable.turnOn();
        } else {
          CallTable.turnOff();
        }

        long queryStartTime = System.currentTimeMillis();


        if (parameters.get("printTransformation", false)) {
          // print the original query
          System.err.println("Text:" + queryText);
        }
        Node root = StructuredQuery.parse(queryText);
        if (parameters.get("printTransformation", false)) {
          // print the parsed query
          System.err.println("Parsed Node:" + root.toString());
        }
        Node transformed = retrieval.transformQuery(root, queryParameters);
        if (parameters.get("printTransformation", false)) {
          // print the transformed query
          System.err.println("Transformed Node:" + transformed.toString());
        }

        // These must be done after parsing and transformation b/c they're
        // overrides or post-processing
        String[] overrides = {"processingModel", "longModel", "shortModel"};
        for (String o : overrides) {
          if (parameters.containsKey(o)) {
            queryParameters.set(o, parameters.getString(o));
          }
        }

        if (parameters.containsKey("deltaReady")) {
          queryParameters.set("deltaReady", parameters.getBoolean("deltaReady"));
        }

        results = retrieval.runQuery(transformed, queryParameters);
        long queryEndTime = System.currentTimeMillis();

        // record query proc time
        if (timing) {
          if (!times.containsKey(queryParameters.getString("number"))) {
            times.put(queryParameters.getString("number"), new ArrayList());
          }
          times.get(queryParameters.getString("number")).add(queryEndTime - queryStartTime);
        }

        // only print results for the first repetition
        if (rep == 0) {
          if (results != null) {
            for (int i = 0; i < results.length; i++) {
              ScoredDocument doc = results[i];
              double score = doc.score;
              int rank = i + 1;

              out.format("%s Q0 %s %d %s galago\n", queryParameters.getString("number"), doc.documentName, rank, formatScore(score));
            }
          }

          if (parameters.get("print_calls", false)) {
            CallTable.print(System.err, queryParameters.getString("number"));
          }
          CallTable.reset();

        }
      }
      long batchEndTime = System.currentTimeMillis();

      batchTimes.add(batchEndTime - batchStartTime);
    }

    if (timing) {
      PrintStream timeStream = System.err;
      if (parameters.isString("time")) {
        timeStream = new PrintStream(new FileOutputStream(parameters.getString("time")));
      }

      for (Parameters query : queries) {
        String id = query.getString("number");
        StringBuilder sb = new StringBuilder();
        sb.append(id);
        long runtotal = 0;
        for (int i = 0; i < times.get(id).size(); i++) {
          long time = times.get(id).get(i);
          runtotal += time;
          sb.append("\t").append(time);
        }
        sb.append("\t").append(((float) runtotal / (float) times.get(id).size()));
        timeStream.println(sb.toString());
      }

      StringBuilder sb = new StringBuilder();
      sb.append("all");
      long runtotal = 0;
      for (int i = 0; i < batchTimes.size(); i++) {
        long time = batchTimes.get(i);
        runtotal += time;
        sb.append("\t").append(time);
      }
      sb.append("\t").append(((float) runtotal / (float) batchTimes.size()));
      timeStream.println(sb.toString());

      timeStream.close();
    }
  }

  private static String formatScore(double score) {
    double difference = Math.abs(score - (int) score);

    if (difference < 0.00001) {
      return Integer.toString((int) score);
    }
    return String.format("%10.8f", score);
  }

  /**
   * this function extracts a list of queries from a parameter object.
   *  - there are several methods of inputting queries:
   *  (query/queries) -> String/List(String)/List(Map)
   * 
   * if List(Map):
   *  [{"number":"id", "text":"query text"}, ...]
   */
  public static List<Parameters> collectQueries(Parameters parameters) throws IOException {
    List<Parameters> queries = new ArrayList();
    int unnumbered = 0;
    if (parameters.isString("query") || parameters.isList("query", Type.STRING)) {
      String id;
      for (String q : (List<String>) parameters.getAsList("query")) {
        id = "unk-" + unnumbered;
        unnumbered++;
        queries.add(Parameters.parse(String.format("{\"number\":\"%s\", \"text\":\"%s\"}", id, q)));
      }
    }
    if (parameters.isString("queries") || parameters.isList("queries", Type.STRING)) {
      String id;
      for (String q : (List<String>) parameters.getAsList("query")) {
        id = "unk-" + unnumbered;
        unnumbered++;
        queries.add(Parameters.parse(String.format("{\"number\":\"%s\", \"text\":\"%s\"}", id, q)));
      }
    }
    if (parameters.isList("query", Type.MAP)) {
      queries.addAll(parameters.getList("query"));
    }
    if (parameters.isList("queries", Type.MAP)) {
      queries.addAll(parameters.getList("queries"));
    }
    return queries;
  }
}
