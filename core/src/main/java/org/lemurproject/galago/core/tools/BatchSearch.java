// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools;

import gnu.trove.map.hash.TObjectLongHashMap;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.lemurproject.galago.core.retrieval.EstimatedDocument;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.tools.App.AppFunction;
import org.lemurproject.galago.tupleflow.CallTable;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

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
    ScoredDocument[] results = null;
    TObjectLongHashMap times = new TObjectLongHashMap();
    long querystarttime, queryendtime;
    long endtime;

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

    // Look for a range
    int[] queryrange = new int[2];
    if (parameters.containsKey("range")) {
      String[] parts = parameters.getString("range").split("-");
      queryrange[0] = Integer.parseInt(parts[0]);
      queryrange[1] = Integer.parseInt(parts[1]);
    } else {
      queryrange[0] = 0;
      queryrange[1] = queries.size();
    }

    // And repeats
    int repeats = (int) parameters.get("repeats", 1);

    // open index
    Retrieval retrieval = RetrievalFactory.instance(parameters);

    // record results requested
    int requested = (int) parameters.get("requested", 1000);

    // check for delayed execution
    if (parameters.containsKey("processingModel")) {
      String[] parts = parameters.getString("processingModel").split("\\.");
      if (parts[parts.length - 1].contains("Delayed")) {
        parameters.set("delayed", true);
        //parameters.set("shareNodes", true);
      }
    }

    // for each query, run it, get the results, print in TREC format
    if (parameters.containsKey("seed")) {
      long seed = parameters.getLong("seed");
      Random r = new Random(seed);
      Collections.shuffle(queries, r);
    }

    long starttime = System.currentTimeMillis();

    for (int idx = queryrange[0]; idx < queryrange[1]; idx++) {
      Parameters query = queries.get(idx);
      String queryText = query.getString("text");
      Parameters p = new Parameters();
      p.set("requested", requested);

      // This is slow - need to improve it, but later
      if (parameters.containsKey("working")) {
        p.set("working", parameters.getList("working"));
      }

      for (int rep = 0; rep < repeats; rep++) {
        if (rep < (repeats - 1)) {
          CallTable.turnOff();
        } else {
          CallTable.turnOn();
        }

        querystarttime = System.currentTimeMillis();
        Node root = StructuredQuery.parse(queryText);
        Node transformed = retrieval.transformQuery(root, p);

        // These must be done after parsing and transformation b/c they're
        // overrides or post-processing
        String[] overrides = {"processingModel", "longModel", "shortModel"};
        for (String o : overrides) {
          if (parameters.containsKey(o)) {
            p.set(o, parameters.getString(o));
          }
        }

        if (parameters.containsKey("deltaReady")) {
          p.set("deltaReady", parameters.getBoolean("deltaReady"));
        }

        if (parameters.get("printTransformation", false)) {
          System.err.println("Text:" + queryText);
          System.err.println("Parsed Node:" + root.toString());
          System.err.println("Transformed Node:" + transformed.toString());
        }

        results = retrieval.runQuery(transformed, p);
        queryendtime = System.currentTimeMillis();
        times.put(query.getString("number") + '.' + (rep + 1), queryendtime - querystarttime);
      }

      if (results != null) {
        for (int i = 0; i < results.length; i++) {
          ScoredDocument doc = results[i];
          double score = doc.score;
          int rank = i + 1;

          if (EstimatedDocument.class.isAssignableFrom(doc.getClass())) {
            EstimatedDocument edoc = (EstimatedDocument) doc;
            out.format("%s Q0 %s %d %s galago %s %s [%s] %d\n", query.getString("number"), doc.documentName, rank,
                    formatScore(score), formatScore(edoc.min), formatScore(edoc.max),
                    (edoc.counts != null) ? Utility.join(edoc.counts) : "null", edoc.length);
          } else {
            out.format("%s Q0 %s %d %s galago\n", query.getString("number"), doc.documentName, rank,
                    formatScore(score));
          }
        }
      }
      if (parameters.get("print_calls", false)) {
        CallTable.print(System.out, query.getString("number"));
      }
      CallTable.reset();
    }

    endtime = System.currentTimeMillis();
    long runtotal = 0;
    if (parameters.get("time", false)) {
      for (int i = queryrange[0]; i < queryrange[1]; i++) {
        Parameters query = queries.get(i);
        String id = query.getString("number");
        for (int rep = 1; rep <= repeats; rep++) {
          String label = id + "." + rep;
          long timeInMS = times.get(label);
          System.err.printf("RUN-ONE\t%s\t%d\n", label, timeInMS);
          if (rep == repeats) {
            runtotal += timeInMS;
          }
        }
      }

      long timeInMS = (endtime - starttime);
      System.err.printf("RUN-TOT\truns\t%d\n", runtotal);
      System.err.printf("RUN-TOT\ttotal\t%d\n", timeInMS);
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
