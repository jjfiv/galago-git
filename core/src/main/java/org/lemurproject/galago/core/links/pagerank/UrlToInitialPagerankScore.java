/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.links.pagerank;

import java.io.File;
import java.io.IOException;
import org.lemurproject.galago.core.types.DocumentUrl;
import org.lemurproject.galago.core.types.PageRankScore;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.TypeReader;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.tupleflow.types.TupleflowLong;
import org.lemurproject.galago.utility.StreamUtil;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.DocumentUrl", order = {"+identifier"})
@OutputClass(className = "org.lemurproject.galago.core.types.PageRankScore", order = {"+docName"})
public class UrlToInitialPagerankScore extends StandardStep<DocumentUrl, PageRankScore> {

  long trueDocCount = 0;
  double defaultScore = 0.0;
  File docCountFile;

  public UrlToInitialPagerankScore(TupleFlowParameters params) throws IOException {
    Parameters p = params.getJSON();
    if (p.containsKey("defaultScore")) {
      defaultScore = p.getLong("defaultScore");
    } else {
      String stream = p.getString("docCountStream");
      TypeReader<TupleflowLong> docCounts = params.getTypeReader(stream);
      long dc = 0;
      TupleflowLong c = docCounts.read();
      while (c != null) {
        dc += c.value;
        c = docCounts.read();
      }

      if (dc > 0) {
        defaultScore = 1.0 / dc;
      } else {
        defaultScore = 0;
      }
    }

    docCountFile = new File(p.getString("docCountFolder"), "" + params.getInstanceId());
  }

  @Override
  public void process(DocumentUrl url) throws IOException {
    trueDocCount += 1;

    processor.process(new PageRankScore(url.identifier, defaultScore));
  }

  @Override
  public void close() throws IOException {

    // ensure the docCount is written to a file.
    StreamUtil.copyStringToFile("" + trueDocCount, docCountFile);

    processor.close();
  }
}
