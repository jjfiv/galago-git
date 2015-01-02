/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.tools;

import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QuerySetJudgments;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.core.tools.apps.BatchSearch;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 *
 * @author sjh
 */
public class GenerateWorkingSetQueries extends AppFunction {

  @Override
  public String getName() {
    return "construct-working-set-queries";
  }

  @Override
  public String getHelpString() {
    return "galago construct-working-set-queries <<parameters>>\n\n"
            + "Generates a new json query file that contains a working set\n"
            + "for each input query.\n\n"
            + "Parameters: \n"
            + "\t--queries\t JSON query file\n"
            + "\t--index\t/path/to/index\n"
            + "Optional Parameters: \n"
            + "\t--topK=[10000]\tSize of generated working sets\n\n"
            // + "\t--randomK=[0]\tNumber of additional random documents to add to set.\n"
            + "\t--qrels\t/path/to/qrel/file\n"
            + "\t--output\t/path/to/output/file\n"
            + "\t--wsNumbers=[false]\tif the desired application uses a localretrieval, internal document numbers can be output,\n"
            + "\t\totherwise document names will be output for the working set.\n\n";

  }

  @Override
  public void run(Parameters parameters, PrintStream output) throws Exception {
    List<Parameters> queries = BatchSearch.collectQueries(parameters);

    if (!parameters.containsKey("index") || queries.isEmpty()) {
      output.println(getHelpString());
      return;
    }

    Retrieval retrieval = RetrievalFactory.create(parameters);

    List<Parameters> wsQueries = new ArrayList<Parameters>();
    int topK = (int) parameters.get("topK", 10000);
    //int randomK = (int) p.get("randomK", 0);

    boolean numbers = parameters.get("wsNumbers", false);
    if (numbers) {
      try {
        LocalRetrieval lr = (LocalRetrieval) retrieval;
      } catch (Exception e) {
        output.println("--wsNumbers=true requires a LocalRetrieval.\n"
                + "Got: " + retrieval.getClass() + "\n"
                + "Required: " + LocalRetrieval.class + "\n"
                + getHelpString());
        return;
      }
    }

    QuerySetJudgments qrels = null;
    if (parameters.isString("qrels")) {
      boolean binaryJudgments = parameters.get("binary", false);
      boolean positiveJudgments = parameters.get("postive", true);
      qrels = new QuerySetJudgments(parameters.getString("qrels"), binaryJudgments, positiveJudgments);
    }

    for (Parameters queryParams : queries) {
      Parameters wsQ = queryParams.clone();
      wsQueries.add(wsQ);

      HashSet<String> workingSetNames = new HashSet<String>();
      HashSet<Long> workingSetNumbers = new HashSet<Long>();

      // first run the query - collect topK
      queryParams.set("requested", topK);
      Node query = StructuredQuery.parse(queryParams.getString("text"));
      query = retrieval.transformQuery(query, queryParams);
      List<ScoredDocument> results = retrieval.executeQuery(query, queryParams).scoredDocuments;
      
      for (ScoredDocument r : results) {
        workingSetNames.add(r.documentName);
        if (numbers) {
          workingSetNumbers.add(r.document);
        }
      }

      // add all relevant documents
      if (qrels != null) {
        QueryJudgments qrel = qrels.get(queryParams.getString("number"));
        if (qrel != null) {
          for (String docName : qrel.getDocumentSet()) {
            if (qrel.isRelevant(docName)) {
              try {
                // check that this docName is in the collection
                long docId = ((LocalRetrieval) retrieval).getDocumentId(docName);

                workingSetNames.add(docName);
                if (numbers) {
                  workingSetNumbers.add(docId);
                }
              } catch (IOException e) {
                //ignore failed document names
              }
            }
          }
        }
      }

//      // add some random documents
//      if (randomK > 0) {
//        int maxDocument = 
//        for (int count = 0; count < randomK;) {
//          if (numbers) {
//            workingSet.add(((LocalRetrieval) retrieval).getDocumentId(docName));
//          } else {
//            workingSet.add(docName);
//          }
//        }
//      }

      List<String> wsNames = new ArrayList<String>(workingSetNames);
      Collections.sort(wsNames);
      wsQ.set("working", wsNames);

      if (numbers) {
        List<Long> wsNumbers = new ArrayList<Long>(workingSetNumbers);
        Collections.sort(wsNumbers);
        wsQ.set("workingIds", wsNumbers);
      }
    }

    Parameters wsParameters = Parameters.create();
    wsParameters.set("queries", wsQueries);

    if (parameters.isString("output")) {
      File o = new File(parameters.getString("output"));
      FSUtil.makeParentDirectories(o);
      Utility.copyStringToFile(wsParameters.toPrettyString(), o);
    } else {
      output.println(wsParameters.toPrettyString());
    }
  }
}
