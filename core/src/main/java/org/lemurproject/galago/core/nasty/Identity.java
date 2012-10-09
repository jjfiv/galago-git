package org.lemurproject.galago.core.nasty;

import java.io.IOException;

import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;

@Verified
@InputClass(className = "org.lemurproject.galago.core.types.NumberedDocumentData", order={"+url"})
@OutputClass(className = "org.lemurproject.galago.core.types.NumberedDocumentData")
public class Identity extends StandardStep<NumberedDocumentData,NumberedDocumentData> {

	@Override
	public void process(NumberedDocumentData object) throws IOException {
		processor.process(object);	
	}

}
