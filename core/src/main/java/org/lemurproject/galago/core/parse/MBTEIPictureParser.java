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

class MBTEIPictureParser extends MBTEIParserBase {
    Pattern blockTag = Pattern.compile("block");
    Pattern pageTag = Pattern.compile("page");
    int page = 0;
    int index = 0;
   
    public MBTEIPictureParser(DocumentSplit split, InputStream is) {
	super(split, is);
	S0();
    }

    public void S0() {
	addStartElementAction(blockTag, "checkBlock");
	addStartElementAction(pageTag, "markNewPage");
    } 

    public void markNewPage(int ignored) {
	++page;
	index = 0;
    }

    public void checkBlock(int ignored) {
	String blockType = reader.getAttributeValue(null, "blockType");
	if (blockType.equalsIgnoreCase("picture")) {
	    parsedDocument = new Document();
	    parsedDocument.name = String.format("%s_%d", 
						getArchiveIdentifier(),
						page);
	    parsedDocument.metadata.put("ordinal", Integer.toString(index));
	    parsedDocument.metadata.put("top", 
					reader.getAttributeValue(null, "t"));
	    parsedDocument.metadata.put("bottom", 
					reader.getAttributeValue(null, "b"));
	    parsedDocument.metadata.put("left", 
					reader.getAttributeValue(null, "l"));
	    parsedDocument.metadata.put("right", 
					reader.getAttributeValue(null, "r"));
	    ++index;
	}
    }
    

    public void cleanup() {
	// Nothing to do.
    }
}