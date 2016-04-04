package org.lemurproject.galago.core.tools.apps;

import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.tokenize.Tokenizer;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.StreamCreator;
import org.lemurproject.galago.utility.StreamUtil;
import org.lemurproject.galago.utility.tools.AppFunction;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * @author jfoley
 */
public class TokenizeAndGrabStats extends AppFunction {

  @Override
  public String getName() {
    return "tokenize-and-grab-stats";
  }

  @Override
  public String getHelpString() {
    return "Given a document, and an index, grab stats for all the terms in that document. Return the output as useful JSON.\n"+makeHelpStr(
        "input", "The path to the text document.",
        "index", "The index to use. (Standard RetrievalFactory setup)."
    );
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    Retrieval index = RetrievalFactory.create(p);
    Tokenizer tokenizer = index.getTokenizer();

    String text = StreamUtil.copyStreamToString(StreamCreator.openInputStream(p.getString("input")));

    Document doc = tokenizer.tokenize(text);
    HashSet<String> uniq = new HashSet<>(doc.terms);

    List<Parameters> termInfos = new ArrayList<>();
    for (String query : uniq) {
      Parameters termStats = Parameters.create();
      NodeStatistics counts = index.getNodeStatistics(new Node("counts", query));
      termStats.set("term", query);
      termStats.set("cf", counts.nodeFrequency);
      termStats.set("maxTF", counts.maximumCount);
      termStats.set("df", counts.nodeDocumentCount);
      termInfos.add(termStats);
    }

    Parameters overall = Parameters.create();
    FieldStatistics lengths = index.getCollectionStatistics(new Node("lengths"));
    overall.put("clen", lengths.collectionLength);
    overall.put("terms", termInfos);

    if(p.get("pretty", true)) {
      output.println(overall.toPrettyString());
    } else {
      output.println(overall);
    }
  }
}
