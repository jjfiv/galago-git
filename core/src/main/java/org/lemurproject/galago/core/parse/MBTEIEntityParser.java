// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.regex.Pattern;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.util.StreamReaderDelegate;
import org.lemurproject.galago.core.types.DocumentSplit;

// Fundamentally operates differently than the book and page parsers,
// so it is subclassed higher up the hierarchy
//
// Assumptions
// - Names cannot be nested
class MBTEIEntityParser extends MBTEIParserBase {
    class Context {
	public Context(LinkedList<String> previousText) {
	    nameBuffer = new StringBuilder();
	    postText = new StringBuilder();
	    StringBuilder preTextBuilder = new StringBuilder();
	    for (String token : previousText) {
		preTextBuilder.append(token).append(" ");
	    }
	    preText = preTextBuilder.toString().trim();
	    numTrailingWords = windowSize;
	}
	String name;
	String type;
	StringBuilder nameBuffer;
	String preText;
	StringBuilder postText;
	int numTrailingWords;
    }   

    // number of words before and after a name tag to associate
    public int windowSize = 100; 
    public LinkedList<String> slidingWindow;
    public LinkedList<Context> openContexts;
    Pattern dateTag = Pattern.compile("date");
    String restrict = null;

    public MBTEIEntityParser(DocumentSplit split, InputStream is) {
	super(split, is);
	openContexts = new LinkedList<Context>();
	slidingWindow = new LinkedList<String>();
	S0();
    } 

    // Override to check for "finished" documents (contexts)
    // before moving on. This handles a case where two contexts
    // finished at the same time, so after one is emitted, the
    // next one should be emitted immediately after that.
    @Override
    public Document nextDocument() throws IOException {
	if (openContexts.size() > 0) {
	    Context leadingContext = openContexts.peek();
	    if (leadingContext.numTrailingWords == 0) {
		// Will immediately cause the super call to return,
		// but interferes with the "flow" less.
		buildDocument();
	    }
	}
	return super.nextDocument();
    }

    public void S0() {
	addStartElementAction(textTag, "moveToS1");
    }

    public void moveToS1(int ignored) {
	echo(XMLStreamConstants.START_ELEMENT);
	clearStartElementActions();
	
	addStartElementAction(wordTag, "updateContexts");
	addStartElementAction(nameTag, "openNewContext");
	addEndElementAction(nameTag, "updateLastNamingContext");
	addEndElementAction(textTag, "removeActionsAndEmit");
    }

    public void removeActionsAndEmit(int event) {
	echo(event);
	clearAllActions();
	buildDocument();
    }

    public void openNewContext(int ignored) {
	Context freshContext = new Context(slidingWindow);
	freshContext.type = reader.getAttributeValue(null, "type").toLowerCase();
	openContexts.addLast(freshContext);
    }

    public void updateLastNamingContext(int ignored) {
	Context latestContext = openContexts.peekLast();
	latestContext.name = latestContext.nameBuffer.toString().trim();
	latestContext.nameBuffer = null;
	
	// We now apply the restriction if it exists, meaning if 
	// we don't care about this entity, don't track terms for it.
	if (restrict != null) {
	    if (!restrict.equals(latestContext.type)) {
		openContexts.pollLast();
	    }
	}    
    }

    public void updateContexts(int ignored) {
	String formValue = reader.getAttributeValue(null, "form");
	String scrubbed = scrub(formValue);
	slidingWindow.addLast(scrubbed);
	while (slidingWindow.size() > windowSize) {
	    slidingWindow.poll();
	}
	
	for (Context c : openContexts) {
	    if (c.nameBuffer != null) {
		c.nameBuffer.append(scrubbed).append(" ");
	    } else {
		c.postText.append(scrubbed).append(" ");
		--c.numTrailingWords;
	    }
	}

	// Finally check for a finished context
	if (openContexts.size() > 0 && 
	    openContexts.peek().numTrailingWords == 0) {
	    buildDocument();
	}
    }

    public void buildDocument() {
	if (openContexts.size() > 0) {
	    Context closingContext = openContexts.poll();
	    StringBuilder documentText = new StringBuilder(closingContext.preText);
	    documentText.append(" ").append(closingContext.postText.toString().trim());
	    parsedDocument = new Document(closingContext.name,
					  documentText.toString());
	} else {
	    parsedDocument = null;
	}
    }

    public void cleanup() {
	if (openContexts.size() > 0) {
	    // Restrict out any contexts that don't match.
	    if (restrict != null) {
		LinkedList<Context> temp = new LinkedList<Context>();
		for (Context c : openContexts) {
		    if (restrict.equals(c.type)) {
			temp.add(c);
		    }
		}
		openContexts = temp;
	    }

	    for (Context c : openContexts) {
		c.numTrailingWords = 0;
	    }
	    // immediately put a document up
	    buildDocument();	    
	}
    }
}
