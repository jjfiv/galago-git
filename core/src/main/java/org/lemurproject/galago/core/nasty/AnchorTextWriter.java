package org.lemurproject.galago.core.nasty;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Formatter;

import org.lemurproject.galago.core.types.AdditionalDocumentText;
import org.lemurproject.galago.core.types.ExtractedLink;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.Verified;

@Verified
@InputClass(className = "org.lemurproject.galago.core.types.AdditionalDocumentText")
public class AnchorTextWriter implements Processor<AdditionalDocumentText> {

	protected PrintWriter pw;
	
	private Formatter f = new Formatter();
	public AnchorTextWriter(TupleFlowParameters p) throws FileNotFoundException {
		pw = new PrintWriter(new File("/usr/aubury/scratch2/jdalton/anchors"));
		pw.println("Anchors");
	}

	

	@Override
	public void process(AdditionalDocumentText object) throws IOException {
	//	System.out.println("Starting doc: " + object.identifier);
		pw.println(String.format("%d\t%s", object.identifier, object.text.toString()));
	}

	@Override
	public void close() throws IOException {
		pw.flush();
		pw.close();
		
	}

}
