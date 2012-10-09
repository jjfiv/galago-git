// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import org.lemurproject.galago.core.types.WordDateCount;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;

/** 
 * Uses the year as the "date". Doesn't try to parse it into an actual time, because:
 * 1) Java's date formatting implementation is horrible, stupid, and broken, and
 * 2) Most of the dates are before the CS-defined "epoch", hence will be negative
 *    anyhow.
 *
 * @author irmarc
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.types.WordDateCount")
public class DateTokenizer extends StandardStep<Document, WordDateCount> {

    @Override
    public void process(Document document) throws IOException {
	System.err.printf("About to convert '%s'\n", document.name);
	try {
	    int date = Integer.parseInt(document.name);
	    for (String token : document.terms) {
		processor.process(new WordDateCount(Utility.fromString(token),
						    date,
						    1));
	    }
	} catch (Exception e) {
	    System.err.printf("[SKIPPING] Unable to parse '%s' : %s\n", 
			      document.name,
			      e.getMessage());
	}
	
    }
}