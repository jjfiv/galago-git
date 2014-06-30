// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.contrib.parse;

import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.DocumentStreamParser;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.utility.Parameters;

import java.io.BufferedReader;
import java.io.File;
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
public class SingleFileParser extends DocumentStreamParser {

  protected BufferedReader reader;
  protected String identifier;

  public SingleFileParser(DocumentSplit split, Parameters pp) throws IOException {
    super(split, pp);
    this.reader = getBufferedReader(split);

    //        Parameters p = null;
    //        for(Object parserP_ : pp.getList("externalParsers")){
    //            Parameters parserP = (Parameters) parserP_;
    //            if(parserP.get("filetype","").equalsIgnoreCase(split.fileType)){
    //                p = parserP;
    //            }
    //        }
    //        if(p == null) p = new Parameters();
    try{
      identifier = split.fileName;
      int idx;
      if((idx=identifier.lastIndexOf(File.separator))>=0){
        identifier = identifier.substring(idx+1);
      }
      if(split.fileType != null && identifier.endsWith(split.fileType)) {
        identifier = identifier.substring(0, identifier.length()-split.fileType.length()-1);
      }
    } catch(Exception e) {
      e.printStackTrace();
      System.err.println("split.fileName="+split.fileName);
      System.err.println("split.fileType="+split.fileType);
    }
  }


  public Document nextDocument() throws IOException {
    String line;

    if (reader == null) {
      return null;
    }

    if (identifier.length() == 0) {
      return null;
    }

    StringBuilder buffer = new StringBuilder();

    int lines = 0;
    while ((line = reader.readLine()) != null) {
      buffer.append(line);
      buffer.append('\n');
      lines ++;
    }

    if(lines == 0) return null;

    //        System.out.println(identifier+ "\t\t"+lines+" lines.");
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
