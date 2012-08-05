// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.util.StreamReaderDelegate;
import org.lemurproject.galago.core.types.DocumentSplit;

/**
 * STATES:
 *
 * S0 = starting state. Reads in header information. Stay in this state until
 * you see the "text" open tag, then head to S1.
 *
 * S1 = "w" tags trigger a read from the "form" attribute, which is the 
 *          outputted string.
 *      "name" opening and closing tags are echoed to the output string.
 *      "text" and "TEI" closing tags are also echoed to the output.
 *
 * S1 is a terminal state.
 *
 */
class MBTEIBookParser extends MBTEIParserBase {
    Pattern wantedMetadata = Pattern.compile("title|creator|language|subject|date");
    String header;
    HashMap<String, String> metadata;
    String currentMetaTag = null;
    StringBuilder tagBuilder;

    public MBTEIBookParser(DocumentSplit split, InputStream is) {
	super(split, is);
	// set up to parse the header
	metadata = new HashMap<String, String>();
	S0();
    }

    @Override
    public void cleanup() {
	// Do nothing in this situation for now. It's a pain to
	// cleanup properly. Easier to dump the incomplete data.
	parsedDocument = null;
    }

    public void S0() {
	// Put the more specific matches first
	addStartElementAction(textTag, "moveToS1");
	addStartElementAction(wantedMetadata, "captureMetadata");
	addEndElementAction(wantedMetadata, "stopCaptureMetadata");

	// Collect everything else
	addStartElementAction(matchAll, "echo");
	addEndElementAction(matchAll, "echo");
	setCharactersAction("echo");
    }

    public void moveToS1(int ignored) {
	header = buffer.toString();
	buffer = new StringBuilder();
	contentLength = 0;
	// Matched on "text" opening, but that should be
	// echoed. Do it manually.
	echo(XMLStreamConstants.START_ELEMENT);

	// Move on to the new rules
	// First remove old matchers
	clearAllActions();

	// Now set up our normal processing matchers
	addStartElementAction(wordTag, "echoFormAttribute");
	addStartElementAction(nameTag, "transformNameTag");
	addEndElementAction(nameTag, "transformNameTag");
	addEndElementAction(textTag, "echo");
	addEndElementAction(teiTag, "emitFinalDocument");
    }

    public String lastSeenNameTag = null;
    public void transformNameTag(int event) {
	switch (event) {
	case XMLStreamConstants.START_ELEMENT:
	    String entityType = reader.getAttributeValue(null, "type").toLowerCase(); 
	    buffer.append("<").append(entityType).append(">");
	    lastSeenNameTag = entityType;
	    break;
	case XMLStreamConstants.END_ELEMENT:
	    if (lastSeenNameTag != null) {
		buffer.append("</").append(lastSeenNameTag).append(">");
	    } else {
		Logger.getLogger(getClass()
				 .toString())
		    .log(Level.WARNING,"No open name tag, but close called!");
	    }
	    lastSeenNameTag = null;
	    break;
	}
    }

    public void captureMetadata(int ignored) {
	echo(XMLStreamConstants.START_ELEMENT);
	currentMetaTag = reader.getLocalName();
	tagBuilder = new StringBuilder();
    } 

    public void stopCaptureMetadata(int ignored) {
	echo(XMLStreamConstants.END_ELEMENT);
	metadata.put(currentMetaTag, tagBuilder.toString().trim());
	currentMetaTag = null;
	tagBuilder = null;
    }

    public void echo(int event) {
	super.echo(event);
	if (currentMetaTag != null &&
	    event == XMLStreamConstants.CHARACTERS) {
	    tagBuilder.append(reader.getText()).append(" ");
	}
    }

    // This should only be called once but there isn't really a way 
    // to handle that gracefully other than removing all the handlers.
    public void emitFinalDocument(int ignored) {
	if (contentLength > 0) {
	    // Echo "</tei>" or whatever it is.
	    echo(XMLStreamConstants.END_ELEMENT);		
	    
	    StringBuilder documentContent = new StringBuilder(header);
	    documentContent.append(buffer.toString().trim());
	    String bookIdentifier = getArchiveIdentifier();
	    parsedDocument = new Document(bookIdentifier, 
					  documentContent.toString());
	    parsedDocument.metadata = metadata;
	}

	// Emit 1 document per file, or none at all.
	clearStartElementActions();
	clearEndElementActions();
	unsetCharactersAction();
	contentLength = 0;
    }
}