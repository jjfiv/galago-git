// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.util.StreamReaderDelegate;
import org.lemurproject.galago.core.types.DocumentSplit;

class MBTEIPageEntityLinker extends MBTEIParserBase {

    Document bookLinks;
    Document pageLinks;
    int bookPosition;
    int pagePosition;
    String pageNumber;
    LinkedList<Document> finishedDocumentQueue;
    Pattern pageBreakTag = Pattern.compile("pb");

    public MBTEIPageEntityLinker(DocumentSplit split, InputStream is) {
	super(split, is);
	S0();
	bookLinks = new Document();
	bookLinks.name = "COLLECTION-LINK";
	bookLinks.metadata.put("id", getArchiveIdentifier());
	bookLinks.metadata.put("type", "collection");
	bookLinks.metadata.put("pos", "0");
	bookLinks.tags = new LinkedList<Tag>();
	pagePosition = bookPosition = 0;
	pageLinks = null;
	finishedDocumentQueue = new LinkedList<Document>();
    }

    @Override
    protected Document getParsedDocument() {
	if (finishedDocumentQueue.size() > 0) {
	    return finishedDocumentQueue.poll();
	} else {
	    return null;
	}
    }
    
    @Override
    protected boolean documentReady() {
	return finishedDocumentQueue.size() > 0;
    }

    @Override
    public void cleanup() {
	if (pageLinks != null) {
	    emitLinks(pageLinks);	    
	}
	// At this point we should emit links for the entire book.
	emitLinks(bookLinks);
    }

    public void S0() {
	// Don't need any metadata but need to know when to start
	// normal processing
	addStartElementAction(textTag, "moveToS1");
    }

    public void moveToS1(int ignored) {
	clearStartElementActions();

	addStartElementAction(nameTag, "mapTag");
	addStartElementAction(wordTag, "increment");
	addStartElementAction(pageBreakTag, "emitPageLinks");
    }

    public void mapTag(int ignored) {
	String innerType = reader.getAttributeValue(null, "type");
	String identifier = reader.getAttributeValue(null, "name");
	int length = identifier.split(" ").length;
	HashMap<String, String> attributes = new HashMap<String, String>();
	attributes.put("id", identifier);
	attributes.put("type", innerType);
	attributes.put("pos", Integer.toString(pagePosition));
	Tag pageLink = new Tag(String.format("%s-LINK", innerType.toUpperCase()),
			       attributes, 
			       pagePosition, 
			       pagePosition+length);
	pageLinks.tags.add(pageLink);

	Tag bookLink = new Tag(String.format("%s-LINK", innerType.toUpperCase()),
			       null, 
			       bookPosition, 
			       bookPosition+length);
	// Make a deep copy of the attributes and then replace entries.
	// It's wasteful, but it makes the mapping between documents and tags 
	// and links less painful.
	bookLink.attributes = new HashMap<String, String>();
	bookLink.attributes.putAll(attributes);
	bookLink.attributes.put("pos", Integer.toString(bookPosition));
	bookLinks.tags.add(bookLink);
    }

    public void increment(int ignored) {
	++pagePosition;
	++bookPosition;
    }

    public void emitPageLinks(int ignored) {
	if (pageLinks != null) {
	    emitLinks(pageLinks);
	}
	pageNumber = reader.getAttributeValue(null, "n");
	pagePosition = 0;
	String pageId = String.format("%s_%s", 
				      getArchiveIdentifier(), 
				      pageNumber);
	pageLinks = new Document();
	pageLinks.name = "PAGE-LINK";
	pageLinks.metadata.put("id", pageId);
	pageLinks.metadata.put("type", "page");
	pageLinks.metadata.put("pos", "0");
	pageLinks.tags = new LinkedList<Tag>();
    }

    // Although we have links going one way (i.e. page --> person),
    // we need to generate all of the person --> page links.
    protected void emitLinks(Document forwardLinks) {
	if (forwardLinks.tags.size() == 0) {
	    return;
	}
	finishedDocumentQueue.add(forwardLinks);
	for (Tag forwardTag : forwardLinks.tags) {
	    Document reverseLink = new Document();
	    reverseLink.name = forwardTag.name;
	    reverseLink.metadata.put("id", forwardTag.attributes.get("id"));
	    reverseLink.metadata.put("type", forwardTag.attributes.get("type"));
	    reverseLink.metadata.put("pos", forwardTag.attributes.get("pos"));
	    reverseLink.tags = new ArrayList<Tag>(1);
	    Tag reverseTag = new Tag(forwardLinks.name,
				     forwardLinks.metadata,
				     0,
				     0);
	    reverseLink.tags.add(reverseTag);
	    finishedDocumentQueue.add(reverseLink);
	}
    }
}
