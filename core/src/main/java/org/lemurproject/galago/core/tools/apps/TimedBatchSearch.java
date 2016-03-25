// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools.apps;

import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.queries.JSONQueryFormat;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.utility.tools.Arguments;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.*;
import java.util.logging.Logger;

/**
 *
 * @author trevor, sjh
 */
public class TimedBatchSearch extends AppFunction {

  public static final Logger logger = Logger.getLogger("BatchSearch");

  public static void main(String[] args) throws Exception {
    (new BatchSearch()).run(Arguments.parse(args), System.out);
  }

  public String getName() {
    return "timed-batch-search";
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

    // ensure we can print to a file instead of the commandline
    if (parameters.isString("outputFile")) {
      boolean append = parameters.get("appendFile", false);
      out = new PrintStream(new BufferedOutputStream(
              new FileOutputStream(parameters.getString("outputFile"), append)));
    }

    // check if we can print to a file instead of the commandline (else use stderr, same as verbose)
    PrintStream times = System.err;
    if (parameters.isString("timesFile")) {
      boolean append = parameters.get("appendTimes", false);
      times = new PrintStream(new BufferedOutputStream(
              new FileOutputStream(parameters.getString("timesFile"), append)));
    }

    // get queries
    List<Parameters> queries;
    String queryFormat = parameters.get("queryFormat", "json").toLowerCase();
    switch (queryFormat)
    {
      case "json":
        queries = JSONQueryFormat.collectQueries(parameters);
        break;
      case "tsv":
        queries = JSONQueryFormat.collectTSVQueries(parameters);
        break;
      default: throw new IllegalArgumentException("Unknown queryFormat: "+queryFormat+" try one of JSON, TSV");
    }

    // open index
    Retrieval retrieval = RetrievalFactory.create(parameters);

    // record results requested
    int requested = (int) parameters.get("requested", 1000);

    // And repeats
    int repeats = (int) parameters.get("repeats", 5);

    // timing variables
    long batchstarttime, batchendtime;
    long querystarttime, querymidtime, queryendtime;
    long[] batchTimes = new long[repeats];
    // keys should be in sorted order for ordered printing
    Map<String, long[]> queryTimes = new TreeMap<>();
    Map<String, long[]> queryExecTimes = new TreeMap<>();


    Random rnd = null;
    if (parameters.containsKey("seed")) {
      long seed = parameters.getLong("seed");
      rnd = new Random(seed);
    }

    for (int rep = 0; rep < repeats; rep++) {

      // randomize the query order:
      if (rnd != null) {
        Collections.shuffle(queries, rnd);
      }

      batchstarttime = System.currentTimeMillis();

      // for each query, run it, get the results, print in TREC format
      for (Parameters query : queries) {

        querystarttime = System.currentTimeMillis();

        String queryText = query.getString("text");
        String queryNumber = query.getString("number");


        query.setBackoff(parameters);
        query.set("requested", requested);

        // option to fold query cases -- note that some parameters may require upper case
        if (query.get("casefold", false)) {
          queryText = queryText.toLowerCase();
        }

        if (parameters.get("verbose", false)) {
          logger.info("RUNNING: " + queryNumber + " : " + queryText);
        }

        // parse and transform query into runnable form
        Node root = StructuredQuery.parse(queryText);
        // --operatorWrap=sdm will now #sdm(...text... here)
        if(parameters.isString("operatorWrap")) {
          if(root.getOperator().equals("root")) {
            root.setOperator(parameters.getString("operatorWrap"));
          } else {
            Node oldRoot = root;
            root = new Node(parameters.getString("operatorWrap"));
            root.add(oldRoot);
          }
        }
        Node transformed = retrieval.transformQuery(root, query);

        querymidtime = System.currentTimeMillis();

        if (parameters.get("verbose", false)) {
          logger.info("Transformed Query:\n" + transformed.toPrettyString());
        }

        // run query
        List<ScoredDocument> results = retrieval.executeQuery(transformed, query).scoredDocuments;

        queryendtime = System.currentTimeMillis();

        if (!queryTimes.containsKey(queryNumber)) {
          queryTimes.put(queryNumber, new long[repeats]);
          queryExecTimes.put(queryNumber, new long[repeats]);
        }
        queryTimes.get(queryNumber)[rep] = (queryendtime - querystarttime);
        queryExecTimes.get(queryNumber)[rep] = (queryendtime - querymidtime);

        // if we have some results -- print in to output stream
        if (rep == 0 && !results.isEmpty()) {
          for (ScoredDocument sd : results) {
            if(parameters.get("trec", false)){
              out.println(sd.toTRECformat(queryNumber));
            } else {
              out.println(sd.toString(queryNumber));
            }
          }
        }
      }

      batchendtime = System.currentTimeMillis();
      batchTimes[rep] = (batchendtime - batchstarttime);

    }

    if (parameters.isString("outputFile")) {
      out.close();
    }

    printTimingData(times, batchTimes, queryTimes, queryExecTimes);


    if (parameters.isString("timesFile")) {
      times.close();
    }
  }

  private void printTimingData(PrintStream times, long[] batchTimes, Map<String, long[]> queryTimes, Map<String, long[]> queryExecTimes) {
    // print per query results
    // QID [full times] [exec times] avgFull avgExec

    for (String queryNumber : queryTimes.keySet()) {
      times.print(queryNumber);
      double fullSum = 0;
      double execSum = 0;
      long[] fulls = queryTimes.get(queryNumber);
      long[] execs = queryExecTimes.get(queryNumber);
      for (int rep = 0; rep < batchTimes.length; rep++) {
        times.format("\t%d", fulls[rep]);
        fullSum += fulls[rep];
      }
      for (int rep = 0; rep < batchTimes.length; rep++) {
        times.format("\t%d", execs[rep]);
        execSum += execs[rep];
      }
      times.format("\t%g", fullSum / batchTimes.length);
      times.format("\t%g\n", execSum / batchTimes.length);
    }

    // BATCH [full times] [avgQ times] avgFull avgQtime

    times.print("BATCH");
    double batchSum = 0;
    for (int rep = 0; rep < batchTimes.length; rep++) {
      times.format("\t%d", batchTimes[rep]);
      batchSum += batchTimes[rep];
    }
    for (int rep = 0; rep < batchTimes.length; rep++) {
      times.format("\t%d", batchTimes[rep] / queryTimes.size());
    }
    times.format("\t%g", batchSum / batchTimes.length);
    times.format("\t%g\n", batchSum / batchTimes.length / queryTimes.size());
  }
}
