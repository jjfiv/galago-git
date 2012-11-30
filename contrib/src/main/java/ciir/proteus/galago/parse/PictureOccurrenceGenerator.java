// BSD License (http://lemurproject.org/galago-license)
package ciir.proteus.galago.parse;

import java.io.IOException;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.core.types.PictureOccurrence;

@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.types.PictureOccurrence")
public class PictureOccurrenceGenerator extends StandardStep<Document, PictureOccurrence> {
    private Counter pictureCounter;
    
    public PictureOccurrenceGenerator(TupleFlowParameters parameters) {
	pictureCounter = parameters.getCounter("Pictures Generated");
    }

    public void process(Document document) throws IOException {
	PictureOccurrence po = new PictureOccurrence();
	po.id = Utility.fromString(document.name);
	po.ordinal = Integer.parseInt(document.metadata.get("ordinal"));
	po.top = Integer.parseInt(document.metadata.get("top"));
	po.bottom = Integer.parseInt(document.metadata.get("bottom"));
	po.left = Integer.parseInt(document.metadata.get("left"));
	po.right = Integer.parseInt(document.metadata.get("right"));
	processor.process(po);
	if (pictureCounter != null) {
	    pictureCounter.increment();
	}
    }
}