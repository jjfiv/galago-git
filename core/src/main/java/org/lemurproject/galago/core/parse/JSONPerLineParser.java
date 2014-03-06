// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.BufferedReader;
import java.io.IOException;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Treats each line of an input file as its own document, and expects to find JSON on that line.
 * @author jfoley
 */
public class JSONPerLineParser extends DocumentStreamParser {
  private BufferedReader reader;
  private String documentNameField;

  public JSONPerLineParser(DocumentSplit split, Parameters p) throws IOException {
    super(split, p);
    this.documentNameField = p.getString("documentNameField");
    this.reader = getBufferedReader(split);
  }

  @Override
  public Document nextDocument() throws IOException {
		String next = "";
		// skip all blank lines
		while(next.trim().isEmpty()) {
			next = reader.readLine();
			if(next == null)
				return null;
		}
		
    Parameters json = Parameters.parseString(next);
    
    Document result =  new Document();
    result.name = json.getAsString(documentNameField);
    result.text = json.toXML();
    
    return result;
  }

  @Override
  public void close() throws IOException {
    if(reader != null) {
      reader.close();
      reader = null;
    }
  }
  
}
