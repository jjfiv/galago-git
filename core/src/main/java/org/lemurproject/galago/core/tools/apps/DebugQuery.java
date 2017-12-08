package org.lemurproject.galago.core.tools.apps;

import org.lemurproject.galago.core.retrieval.Results;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.tools.AppFunction;

import java.io.PrintStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * @author jfoley
 */

public class DebugQuery extends AppFunction {
  @Override
  public String getName() {
    return "debug-query";
  }

    
  @Override
  public String getHelpString() {
    return this.makeHelpStr(
        "index     ", " The index to search",
        "pretty    ", " [=true] by default, whether to format the output JSON.",
        "docid     ", " Default: missing, but the name of the document to explain",
        "query     ", " The query to issue to the index and explain.",
	"queryFile ", " A series of queries in the specified file.  Assumes one query per line.",
        "requested ", " The number of results to return");
  }

    
  @Override
  public void run(Parameters p, PrintStream output) throws Exception {

    List<String> queryList = new ArrayList<>();

    int requested = p.get("requested", 10);
    String queryFileName = null;
    boolean prettyPrint = p.get("pretty", true);
    String queryStr = null;
	  
    try (Retrieval retrieval = RetrievalFactory.create(p)) {
      Parameters retP = Parameters.create();
      retP.copyFrom(p);
      retP.set("requested", requested);
      retP.set("annotate", true);

      if (p.isString("docid")) {
        retP.set("working", new ArrayList<>(Collections.singletonList(p.getString("docid"))));
      }
      else {
        // This is a hack because default processing model for a lot of queries
	// is Maxscore/WAND and these don't support annotation.
        retP.set("processingModel", "rankeddocument");
      }

      //- Queries in a file
      if (p.containsKey ("queryFile")) {
        queryFileName = p.getString ("queryFile");

	System.out.println ("Query File: " + queryFileName);

        try {
          BufferedReader br = new BufferedReader (new FileReader (queryFileName));
	  
          while ((queryStr = br.readLine ()) != null) {
	    if (queryStr.trim ().length() == 0) {
              continue;
            }

	    queryList.add (queryStr.trim ());
	  }

          br.close();
	}
	catch (IOException ioe) {
          System.out.println ("Failure reading queries from file " + queryFileName);
          System.exit (1);
	}
      }
      //- Single query on the commandline
      else if (p.containsKey ("query")) {
          queryStr = p.getString ("query").trim ();

          if (queryStr.length () == 0) {
            System.out.println ("No usable query.  Quitting.");
            System.exit (1);
	  }
	
          queryList.add (queryStr);
      }
      else {
        System.out.println ("Must have either query or queryFile parameter.  Quitting.");
        System.exit (1);
      }

      for (String q : queryList) {
        Node query = StructuredQuery.parse (q);
        Node xq = retrieval.transformQuery(query, retP);

	System.out.println ("\n=======================================================");
	System.out.println ("Query            : " + q);
	System.out.println ("Transformed Query: " + xq);
	
        Results results = retrieval.executeQuery(xq, retP);

        List<Parameters> jsonResults = new ArrayList<>();

	for (ScoredDocument sdoc : results.scoredDocuments) {
          Parameters docP = Parameters.create();
          docP.put("id", sdoc.documentName);
          docP.put("score", sdoc.score);
          docP.put("rank", sdoc.rank);
          docP.put("annotatedNode", sdoc.annotation.toJSON());
          jsonResults.add(docP);
        }

        Parameters finalOutput = Parameters.create();
        finalOutput.put("docs", jsonResults);
        if (prettyPrint) {
          output.println(finalOutput.toPrettyString());
        }
	else {
          output.println(finalOutput);
        }
      }
    }
    catch (Exception ex) {
      System.out.println ("Exception: ");
      ex.printStackTrace ();
    }	

  }  //- end run
}  //- end class DebugQuery
