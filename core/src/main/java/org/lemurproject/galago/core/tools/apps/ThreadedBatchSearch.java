// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools.apps;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class ThreadedBatchSearch extends AppFunction {

  public static final Logger logger = Logger.getLogger("ThreadedBatchSearch");

  public static void main(String[] args) throws Exception {
    (new ThreadedBatchSearch()).run(new Parameters(args), System.out);
  }

  @Override
  public String getName() {
    return "threaded-batch-search";
  }

  @Override
  public String getHelpString() {
    return "galago threaded-batch-search <args>\n\n"
            + "  Runs a batch of queries against an index and produces TREC-formatted\n"
            + "  output.  The output can be used with retrieval evaluation tools like\n"
            + "  galago eval (org.lemurproject.galago.core.eval).\n\n"
            + "  Sample invocation:\n"
            + "     galago batch-search --index=/tmp/myindex --requested=200 /tmp/queries\n\n"
            + "  Args:\n"
            + "     --index=path_to_your_index\n"
            + "     --requested : Number of results to return for each query, default=1000\n"
            + "     /path/to/parameter/file : Input file in xml parameters format (see below).\n\n"
            + "  Query file format:\n"
            + "    The query file is an JSON file containing a set of queries.  Each query\n"
            + "    has text field, which contains the text of the query, and a number field, \n"
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

    // get queries
    List<Parameters> queries = BatchSearch.collectQueries(parameters);

    // open index
    Retrieval retrieval = RetrievalFactory.instance(parameters);

    // record results requested
    int requested = (int) parameters.get("requested", 1000);

    CountDownLatch latch = new CountDownLatch(queries.size());

    // exception list
    List<Exception> exceptions = new ArrayList();

    // prepare thread pool
    int threadCount = (int) parameters.get("threadCount", Runtime.getRuntime().availableProcessors());
    ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);

    // for each query, create a runner
    List<QueryRunner> runners = new ArrayList();
    for (Parameters query : queries) {

      query.setBackoff(parameters);
      query.set("requested", requested);

      QueryRunner runner = new QueryRunner(retrieval, query, out, exceptions, latch);
      runners.add(runner);
      threadPool.submit(runner);
    }

    while (true) {
      boolean done = latch.await(1000, TimeUnit.MILLISECONDS);
      if (done) {
        break;
      }
      synchronized (logger) {
        logger.info("Still running... " + latch.getCount() + " to go.");
      }
    }

    threadPool.shutdown();

    if (parameters.isString("outputFile")) {
      out.close();
    }
  }

  public class QueryRunner extends Thread {

    private final Parameters query;
    private final Retrieval ret;
    private final PrintStream out;
    private final List<Exception> exceptions;
    private final CountDownLatch latch;

    public QueryRunner(Retrieval ret, Parameters query, PrintStream out, List<Exception> exceptions, CountDownLatch latch) {
      this.ret = ret;
      this.query = query;
      this.out = out;
      this.exceptions = exceptions;
      this.latch = latch;

      // ensure we know what this thread is doing.
      setName(query.getString("number"));
    }

    @Override
    public void run() {
      String queryText = query.getString("text");
      String queryNumber = query.getString("number");

      try {

        // option to fold query cases
        if (query.get("casefold", false)) {
          queryText = queryText.toLowerCase();
        }

        if (query.get("verbose", false)) {
          synchronized (logger) {
            logger.info("RUNNING: " + queryNumber + " : " + queryText);
          }
        }

        // parse and transform query into runnable form
        Node root = StructuredQuery.parse(queryText);
        Node transformed = ret.transformQuery(root, query);

        if (query.get("verbose", false)) {
          synchronized (logger) {
            logger.info("Transformed Query:\n" + transformed.toPrettyString());
          }
        }

        // run query
        ScoredDocument[] results = ret.runQuery(transformed, query);

        // if we have some results -- print in to output stream
        if (results != null) {
          // lock on out to avoid overwriting issues
          synchronized (out) {
            for (int i = 0; i < results.length; i++) {
              if (query.get("trec", false)) {
                out.println(results[i].toTRECformat(queryNumber));
              } else {
                out.println(results[i].toString(queryNumber));
              }
            }
          }
        }
      } catch (Exception e) {
        synchronized (logger) {
          logger.info("FAILED to run query: " + queryNumber + " : " + queryText);
        }
        synchronized (exceptions) {
          exceptions.add(e);
        }
      }
      latch.countDown();
    }
  }
}
