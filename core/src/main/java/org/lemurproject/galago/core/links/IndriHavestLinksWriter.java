/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.links;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import org.lemurproject.galago.core.types.ExtractedLink;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verification;

/**
 *
 * @author sjh
 */
@InputClass(className = "org.lemurproject.galago.core.types.ExtractedLink", order = {"+filePath", "+fileLocation"})
public class IndriHavestLinksWriter implements Processor<ExtractedLink> {

  private final String filePrefix;
  private final String prefixReplacement;
  private BufferedWriter writer;
  private String currentFilePath;
  private String currentDocName;
  private List<ExtractedLink> currentLinks;

  public IndriHavestLinksWriter(TupleFlowParameters tp) {
    Parameters p = tp.getJSON();

    filePrefix = p.getString("filePrefix");
    prefixReplacement = p.getString("prefixReplacement");
    writer = null;
    currentFilePath = "";
    currentDocName = "";
    currentLinks = new ArrayList();
  }

  @Override
  public void process(ExtractedLink link) throws IOException {
    if (!link.filePath.equals(currentFilePath)) {
      resetWriter(link.filePath);
    }

    if (!link.srcName.equals(currentDocName)) {
      writeLinks();

      writer.write("DOCNO=" + link.srcName + "\n");
      writer.write(link.srcUrl + "\n");
      
      currentDocName = link.srcName;
    }

    currentLinks.add(link);
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writeLinks();
      writer.close();
    }
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!Verification.requireParameters(new String[]{"filePrefix", "prefixReplacement"}, parameters.getJSON(), handler)) {
      return;
    }
  }

  private void resetWriter(String filePath) throws IOException {
    if (writer != null) {
      writeLinks();
      writer.close();
    }

    writer = null;

    if (filePath != null) {
      String outputPath = filePath.replaceFirst(filePrefix, prefixReplacement);
      if (outputPath.equals(filePath)) {
        throw new IOException("Can not over write input data.");
      }
      Utility.makeParentDirectories(outputPath);
      writer = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(outputPath))));
      currentFilePath = filePath;
    }
  }

  private void writeLinks() throws IOException {
    writer.write("LINKS=" + currentLinks.size() + "\n");
    for (ExtractedLink el : currentLinks) {
      writer.write("LINKDOCNO=" + el.destName + "\n");
      writer.write("LINKFROM=" + el.destUrl + "\n");

      // ensure "text" is ok.
      el.anchorText = el.anchorText.replaceAll("\"", "\'");

      writer.write("TEXT=\"" + el.anchorText + "\"\n");
    }
    currentLinks.clear();
  }
}

/*
 * Sample output:
 * 
 * 
DOCNO=clueweb09-en0000-01-00001
http://0-derrewyn-0.deviantart.com/favourites/
LINKS=3
LINKDOCNO=clueweb09-en0000-01-00000
LINKFROM=http://0-derrewyn-0.deviantart.com
TEXT="Browse Favourites "
LINKDOCNO=clueweb09-en0000-01-00000
LINKFROM=http://0-derrewyn-0.deviantart.com
TEXT="Faves "
LINKDOCNO=clueweb09-en0000-01-00001
LINKFROM=http://0-derrewyn-0.deviantart.com/favourites/
TEXT="Profile "
DOCNO=clueweb09-en0000-01-00002
....
 */