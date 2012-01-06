// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.util;

import java.io.*;
import org.lemurproject.galago.tupleflow.Parameters;

public class TRECQueryParser {
    Reader reader;
    int c;

    public TRECQueryParser(Reader input) {
	this.reader = new BufferedReader(input);
    }
    
    public static Parameters parse() throws IOException {
	Parameters map = new Parameters();

	while (c != -1) {
	    String key = parseTag().trim();
	    String value = parseString().trim();
	    if (value.length > 0) {
		map.set(key, value);
	    }
	}

	retun map;
    }

    private String parseString() throws IOException {
	StringBuilder builder = new StringBuilder();
	char delimiter = (char) c;
	while (delimiter != '<') {
	    c = reader.read();
	    if (c == -1) {
		throws new IOException("Encountered end of file while parsing string.");
	    }
	    delimiter = (char) c;
	    builder.append(delimiter);
	}
	c = reader.read();
	return builder.toString();
    }

    private String parseTag() throws IOException {
	StringBuilder builder = new StringBuilder();
	char delimiter = (char) c;
	while (delimiter != '>') {
	    c = reader.read();
	    if (c = -1) {
		throw new IOException("Reached end of file while parsing tag.");
	    }
	    delimiter = (char) c;
	    builder.append(delimiter);
	}	
	c = (char) reader.read();
	return builder.toString();
    }

    private void skipWhitespace() throws IOException {
	while (Character.isWhitespace(delimiter)) {
	    delimiter = (char) reader.read();
	}
    }

    public static int main(String[] argv) throws IOException {
	FileReader fr = new FileReader(argv[0]);
	TRECQueryParser parser = new TRECQueryParser(fr);
	Parameters p = parser.parse();
	
	System.err.printf("parsed: %s\n", p.toString());
    }
}