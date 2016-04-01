package org.lemurproject.galago.contrib.parse;

import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.DocumentStreamParser;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.utility.Parameters;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * Simple {@link DocumentStreamParser} to consume the Signal Media Dataset from the NewsIR'16 workshop.
 * Add --filetype=org.lemurproject.galago.contrib.parse.SignalMediaJSONParser to your command line.
 * Also add --tokenizer/field=title to your "galago build" command if you want to index the title field.
 * @author jfoley
 */
public class SignalMediaJSONParser extends DocumentStreamParser {
  private final BufferedReader reader;

  /**
   * This is the constructor expected by UniversalParser
   * It must be implemented in each implementing class
   *
   * @param split
   * @param p
   */
  public SignalMediaJSONParser(DocumentSplit split, Parameters p) throws IOException {
    super(split, p);
    this.reader = DocumentStreamParser.getBufferedReader(split);
  }

  @Override
  public Document nextDocument() throws IOException {
    String line = reader.readLine();
    if(line == null) return null;

    Document doc = new Document();
    Parameters jdoc = Parameters.parseString(line);
    doc.metadata.put("source", jdoc.getString("source"));
    doc.metadata.put("title", jdoc.getString("title"));
    doc.metadata.put("media-type", jdoc.getString("media-type")); // News or Blog
    doc.metadata.put("published", jdoc.getString("published"));

    doc.name = jdoc.getString("id");
    doc.text = "<title>"+jdoc.getString("title")+"</title>"+jdoc.getString("content");
    return doc;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
