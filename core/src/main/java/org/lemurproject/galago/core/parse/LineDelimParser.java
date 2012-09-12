// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * Reads documents delimited by a line delimiter.
 * Assumed the first line contains the document id (former DOCNO-tag).
 *
 * Optionally, a docId prefix can be configured via docIdPrefix. If configured, documents are skipped if first line does not start with the docIdPrefix.
 *
 * Default line delimiter is "----------" (10 hyphens).
 * The line can only contain the delimiter and followed by whitespace, otherwise it won't be accepted as a delimiter.
 *
 * A different line delimiter can be configured via option "linedelim".
 *
 * Make sure that the line delimiter is not used in the contents of the documents!
 *
 *
 * @author dietz
 */
public class LineDelimParser extends DocumentStreamParser {

    protected BufferedReader reader;
    protected String linedelim;
    protected String docIdPrefix;


    public LineDelimParser(DocumentSplit split, Parameters p) throws IOException {
        super(split, p);
        this.reader = getBufferedReader(split);
        this.linedelim = p.get("linedelim", "----------");
        this.docIdPrefix = p.get("docIdPrefix", "");
    }


    public Document nextDocument() throws IOException {
        String line;

        if (reader == null) {
            return null;
        }
        String docIdLine = reader.readLine();
        if (docIdLine == null) return null;
        if (!docIdLine.startsWith(docIdPrefix)) {
            System.err.println("First line of document does not start with docIdPrefix " + docIdPrefix + "\"" + docIdLine + "\". Waiting for next document.");
            return null;
        }
        String identifier = docIdLine.substring(docIdPrefix.length()).trim();

        if (identifier.length() == 0) {
            return null;
        }

        StringBuilder buffer = new StringBuilder();

        while ((line = reader.readLine()) != null) {
            if (line.startsWith(linedelim) && line.trim().equals(linedelim)) {
                break;
            }

            buffer.append(line);
            buffer.append('\n');
        }

        return new Document(identifier, buffer.toString());
    }

    @Override
    public void close() throws IOException {
        if (this.reader != null) {
            this.reader.close();
            this.reader = null;
        }
    }
}
