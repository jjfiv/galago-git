package org.lemurproject.galago.core.tools.apps;

import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.StreamCreator;
import org.lemurproject.galago.utility.tools.AppFunction;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Dump documents by id to one JSON object per line.
 * @author jfoley
 */
public class GetDocsJSONL extends AppFunction {

  @Override
  public String getName() {
    return "get-docs-jsonl";
  }

  @Override
  public String getHelpString() {
    return makeHelpStr(
        "index", "The index to target",
        "input", "The file containing document ids to pull; 1 per line."
        );
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {

    Retrieval index = RetrievalFactory.create(p);

    List<String> ids = new ArrayList<>(Utility.readStreamToStringSet(StreamCreator.openInputStream(p.getString("input"))));

    for (int i = 0; i < ids.size(); i+=100) {
      List<String> batch = ids.subList(i, Math.min(ids.size(), i+100));
      Map<String, Document> docs = index.getDocuments(batch, Document.DocumentComponents.JustText);
      for (Document document : docs.values()) {
        Parameters docP = Parameters.create();
        docP.put("id", document.name);
        docP.put("content", document.text);
        output.println(docP);
      }
    }
  }
}
