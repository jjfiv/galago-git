// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.utility.Parameters;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.String;

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
    protected String textBeginLine;
    protected String textEndLine;


    public LineDelimParser(DocumentSplit split, Parameters pp) throws IOException {
        super(split, pp);
        this.reader = getBufferedReader(split);

        Parameters p = null;
        for(Object parserP_ : pp.getList("externalParsers")){
            Parameters parserP = (Parameters) parserP_;
            if(parserP.get("filetype","").equalsIgnoreCase(split.fileType)){
                p = parserP;
            }
        }
        if(p == null) p = Parameters.create();
        this.linedelim = p.get("linedelim", "----------");
        this.docIdPrefix = p.get("docIdPrefix", "");
        this.textBeginLine = p.get("textBeginLine","");
        this.textEndLine = p.get("textEndLine","");

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
        boolean inText = false;
        if (textBeginLine.isEmpty()) inText = true;

        while ((line = reader.readLine()) != null) {

            String lineTrimmed = line.trim();
            if (line.startsWith(linedelim) && (lineTrimmed.equals(linedelim) || line.equals(linedelim))) {
                break;
            } else if(!inText && !textBeginLine.isEmpty() && line.startsWith(textBeginLine) && (lineTrimmed.equals(textBeginLine) || line.equals(textBeginLine))) {
                inText = true;
            } else if(inText && !textEndLine.isEmpty() && line.startsWith(textEndLine) && (lineTrimmed.equals(textEndLine) || line.equals(textEndLine))) {
                inText = false;
            } else {
                if(inText){
                    buffer.append(line);
                    buffer.append('\n');
                }
            }
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
