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
import java.util.regex.Matcher;
import java.util.zip.GZIPOutputStream;
import org.lemurproject.galago.core.types.ExtractedLinkIndri;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Verification;

/**
 *
 * @author sjh
 */
@InputClass(className = "org.lemurproject.galago.core.types.ExtractedLinkIndri", order = {"+filePath", "+fileLocation"})
public class IndriHavestLinksWriter implements Processor<ExtractedLinkIndri> {

    private static final int MAX_INLINKS = 10000;
    private final String filePrefix;
    private final String prefixReplacement;
    private BufferedWriter writer;
    private String currentFilePath;
    private String currentDocName;
    private List<ExtractedLinkIndri> currentLinks;

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
    public void process(ExtractedLinkIndri link) throws IOException {
        if (!link.anchorText.isEmpty()) {
            // ony record links with anchor text
            if (!link.filePath.equals(currentFilePath)) {
                resetWriter(link.filePath);
            }

            if (!link.destName.equals(currentDocName)) {

                // check that there was a document to be linked to (first output to file check)
                if (!currentDocName.isEmpty()) {
                    writeLinks();
                }

                writer.write("DOCNO=" + link.destName + "\n");
                writer.write(link.destUrl + "\n");

                currentDocName = link.destName;
            }
            // limit to a maximum number of inlinks
            if (currentLinks.size() < MAX_INLINKS) {
                currentLinks.add(link);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writeLinks();
            writer.close();
        }
    }

    public static void verify(TupleFlowParameters parameters, ErrorStore store) {
        if (!Verification.requireParameters(new String[]{"filePrefix", "prefixReplacement"}, parameters.getJSON(), store)) {
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
            // MCZ 3/2014 - added quoteReplacement() for Windows compatibility
            String outputPath = filePath.replaceFirst(Matcher.quoteReplacement(filePrefix) ,  Matcher.quoteReplacement(prefixReplacement) );
            if (outputPath.equals(filePath)) {
                throw new IOException("Can not over write input data.");
            }
            FileUtility.makeParentDirectories(outputPath);

            // all output is compressed.
            writer = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(outputPath))));
            currentFilePath = filePath;
            currentDocName = "";
        }
    }

    private void writeLinks() throws IOException {
        writer.write("LINKS=" + currentLinks.size() + "\n");
        for (ExtractedLinkIndri el : currentLinks) {
            writer.write("LINKDOCNO=" + el.srcName + "\n");
            writer.write("LINKFROM=" + el.srcUrl + "\n");

            // ensure quoted "text" is ok.
            el.anchorText = el.anchorText.replaceAll("\"", "\'");
            // no empty links are recorded.
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
