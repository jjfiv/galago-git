/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.tools;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
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
public class TimedXCount extends AppFunction {

  @Override
  public String getName() {
    return "timed-xcount";
  }

  @Override
  public String getHelpString() {
    return "galago timed-xcount <params>";
  }

  @Override
  public void run(Parameters parameters, PrintStream out) throws Exception {
    if (!(parameters.containsKey("x"))) {
      out.println(this.getHelpString());
      return;
    }

    // ensure we can print to a file instead of the commandline
    if (parameters.isString("outputFile")) {
      out = new PrintStream(new BufferedOutputStream(
              new FileOutputStream(parameters.getString("outputFile"))));
    }

    // get queries
    List<Parameters> exprs = prepareExpressions(parameters.getString("inputs"), parameters.getString("op"), parameters.getString("part"));

    // open index
    Retrieval retrieval = RetrievalFactory.instance(parameters);

    // Repeats
    int repeats = (int) parameters.get("repeats", 5);

    // timing variables
    long querystarttime, queryendtime;


    Random rnd = null;
    if (parameters.containsKey("seed")) {
      long seed = parameters.getLong("seed");
      rnd = new Random(seed);
    }

    for (int rep = 0; rep < repeats; rep++) {

      // randomize the query order:
      if (rnd != null) {
        Collections.shuffle(exprs, rnd);
      }

      // for each query, run it, get the results, print in TREC format
      for (Parameters expr : exprs) {

        querystarttime = System.currentTimeMillis();

        String queryText = expr.getString("node");

        expr.setBackoff(parameters);

        // parse and transform query into runnable form
        Node root = StructuredQuery.parse(queryText);

        // run query
        NodeStatistics s = retrieval.getNodeStatistics(root);

        queryendtime = System.currentTimeMillis();

        if (!expr.containsKey("times")) {
          expr.set("times", new ArrayList<Long>());
        }
        expr.getList("times").add(queryendtime - querystarttime);
        if (!expr.containsKey("cf")) {
          expr.set("cf", s.nodeFrequency);
          expr.set("dc", s.nodeDocumentCount);
        }
      }
    }

    for (Parameters expr : exprs) {
      out.format("%s\t%d\t%d\t", expr.getString("l"), expr.getLong("cf"), expr.getLong("dc"));
      double sum = 0.0;
      for (Long t : (List<Long>) expr.getList("times")) {
        out.format("%d\t", t);
        sum += t;
      }
      out.format("%.3f", (sum / (double) repeats));
    }

    if (parameters.isString("outputFile")) {
      out.close();
    }
  }

  private List<Parameters> prepareExpressions(String file, String op, String part) throws IOException {
    List<Parameters> exprs = new ArrayList();
    BufferedReader r = new BufferedReader(new FileReader(file));
    String l;
    while ((l = r.readLine()) != null) {
      String[] terms = l.split(" ");

      Parameters expr = new Parameters();

      String node = op + "( ";
      for (String t : terms) {
        node += "#extents:" + t + ":part=" + part + "() ";
      }
      node += ")";

      expr.set("l", l);
      expr.set("node", node);

    }
    r.close();
    return exprs;
  }
}
