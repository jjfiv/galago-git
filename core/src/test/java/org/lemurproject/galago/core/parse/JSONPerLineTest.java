// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.parse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.lemurproject.galago.core.tokenize.Tokenizer;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author jfoley
 */
@RunWith(JUnit4.class)
public class JSONPerLineTest {	
	@Test
	public void simpleParse() throws IOException {
		ArrayList<Parameters> pdocs = new ArrayList<Parameters>();
		
		for(int i=0; i<10; i++) {
			Parameters doc = new Parameters();
			doc.set("id", i);
			StringBuilder text = new StringBuilder();
			text.append("foo");
			for(int j=0; j<i; j++) {
				text.append(" bar");
			}
			doc.set("text", text.toString());
			pdocs.add(doc);
		}
		
		StringBuilder inputText = new StringBuilder();
		for(Parameters p : pdocs) {
			inputText.append(p.toString()).append('\n').append('\n');
		}
		
		Parameters parseParams = new Parameters();
		parseParams.set("documentNameField", "id");
		
		Tokenizer tokenizer =	Tokenizer.instance(parseParams);
		
		ArrayList<Document> docs = new ArrayList<Document>();
		
		File tmp = null;
		try {
			tmp = FileUtility.createTemporary();
			Utility.copyStringToFile(inputText.toString(), tmp);

			DocumentSplit docsplit = new DocumentSplit();
			docsplit.fileName = tmp.getAbsolutePath();
			
			JSONPerLineParser jsonPerLineParser = new JSONPerLineParser(docsplit, parseParams);
			while(true) {
				Document d = jsonPerLineParser.nextDocument();
				if(d == null) {
					break;
				}
				tokenizer.process(d);
				docs.add(d);
			}
		} finally {
			if(tmp != null) {
				tmp.delete();
			}
		}
		
		assertEquals(docs.size(), pdocs.size());
		for(int i=0; i<docs.size(); i++) {
			Document di = docs.get(i);
			String idstr = Integer.toString(i);
			assertEquals(di.name, idstr);
			// System.out.println(Utility.join(di.terms, ", "));
			// id#, foo, bar*i
			assertEquals(i+2, di.terms.size());
			assertEquals(di.terms.get(0), idstr);
			assertEquals(di.terms.get(1), "foo");
			for(int j=2; j<di.terms.size(); j++) {
				assertEquals(di.terms.get(2), "bar");
			}
		}
		
	}
}
