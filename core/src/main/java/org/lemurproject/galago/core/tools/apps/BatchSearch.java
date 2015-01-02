// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools.apps;

import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.utility.queries.JSONQueryFormat;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.utility.Parameters;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.logging.Logger;

/**
 *
 * @author sjh
 */
public class BatchSearch extends AppFunction {

  public static final Logger logger = Logger.getLogger("BatchSearch");

  public static void main(String[] args) throws Exception {
    (new BatchSearch()).run(Parameters.parseArgs(args), System.out);
  }

  @Override
  public String getName() {
    return "batch-search";
  }

  @Override
  public String getHelpString() {
    return "galago simple-batch-search <args>\n\n"
            + "  Runs a batch of queries against an index and produces TREC-formatted\n"
            + "  output.  The output can be used with retrieval evaluation tools like\n"
            + "  galago eval (org.lemurproject.galago.core.eval).\n\n"
            + "  Sample invocation:\n"
            + "     galago batch-search --index=/tmp/myindex --requested=200 /tmp/queries.json \n\n"
            + "  Args:\n"
            + "     --index=path_to_your_index\n"
            + "     --requested : Number of results to return for each query, default=1000\n"
            + "     /path/to/query/file.json : Input file in xml parameters format (see below).\n\n"
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
    List<ScoredDocument> results;

    if (!(parameters.containsKey("query")
            || parameters.containsKey("queries"))) {
      out.println(this.getHelpString());
      return;
    }

    // ensure we can print to a file instead of the commandline
    if (parameters.isString("outputFile")) {
      boolean append = parameters.get("appendFile", false);
      out = new PrintStream(new BufferedOutputStream(
              new FileOutputStream(parameters.getString("outputFile"), append)), true, "UTF-8");
    }

    // get queries
    List<Parameters> queries = JSONQueryFormat.collectQueries(parameters);

    // open index
    Retrieval retrieval = RetrievalFactory.create(parameters);

    // record results requested
    int requested = (int) parameters.get("requested", 1000);

    // for each query, run it, get the results, print in TREC format

    for (Parameters query : queries) {
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
      Node transformed = retrieval.transformQuery(root, query);

      if (parameters.get("verbose", false)) {
        logger.info("Transformed Query:\n" + transformed.toPrettyString());
      }

      // run query
      results = retrieval.executeQuery(transformed, query).scoredDocuments;
      

      // if we have some results -- print in to output stream
      if (!results.isEmpty()) {
        for (ScoredDocument sd : results) {
          if (query.get("trec", false)) {
            out.println(sd.toTRECformat(queryNumber));
          } else {
            out.println(sd.toString(queryNumber));
          }
        }
      }
    }

    if (parameters.isString("outputFile")) {
      out.close();
    }
  }

}
