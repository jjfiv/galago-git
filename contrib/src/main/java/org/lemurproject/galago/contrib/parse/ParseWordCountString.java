/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.parse;

import java.io.BufferedReader;
import java.io.IOException;
import org.lemurproject.galago.core.parse.DocumentStreamParser;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.WordCount;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.DocumentSplit")
@OutputClass(className = "org.lemurproject.galago.core.types.WordCount", order = {"+word"})
public class ParseWordCountString extends StandardStep<DocumentSplit, WordCount> {

    String delim;

    public ParseWordCountString(TupleFlowParameters p) {
        delim = p.getJSON().getString("delim");
    }

    @Override
    public void process(DocumentSplit doc) throws IOException {
        BufferedReader reader = DocumentStreamParser.getBufferedReader(doc);
        for (String str = reader.readLine();
                str != null;
                str = reader.readLine()) {

            String[] parts = str.split(delim);
            if (parts.length > 0) {
                String t = parts[0];
                long cf = 1;
                long dc = 1;
                long mxdf = 1;
                if (parts.length >= 2) {
                    cf = Long.parseLong(parts[1]);
                }
                if (parts.length >= 3) {
                    dc = Long.parseLong(parts[2]);
                }
                if (parts.length >= 4) {
                    mxdf = Long.parseLong(parts[4]);
                }

                WordCount wc = new WordCount(Utility.fromString(t), cf, dc, mxdf);
                processor.process(wc);

            }

        }
        reader.close();
    }
}
