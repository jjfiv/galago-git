package org.lemurproject.galago.contrib.index;

import org.lemurproject.galago.core.types.AdditionalDocumentText;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.Verified;

import java.io.*;

@Verified
@InputClass(className = "org.lemurproject.galago.core.types.AdditionalDocumentText")
public class AnchorTextWriter implements Processor<AdditionalDocumentText> {

	protected PrintWriter pw;
	
	public AnchorTextWriter(TupleFlowParameters p) throws FileNotFoundException, UnsupportedEncodingException {
    Parameters args = p.getJSON();
		pw = new PrintWriter(new File(args.getString("filename")), "UTF-8");
		pw.println("Anchors");
	}

	

	@Override
	public void process(AdditionalDocumentText object) throws IOException {
	//	System.out.println("Starting doc: " + object.identifier);
		pw.println(object.identifier+"\t"+object.text);
	}

	@Override
	public void close() throws IOException {
		pw.flush();
		pw.close();
	}

}
