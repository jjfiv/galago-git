// BSD License (http://lemurproject.org/galago-license)
package ciir.proteus.galago.parse;

import java.util.regex.Pattern;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;

public class MBTEIPictureParser extends MBTEIParserBase {
    Pattern blockTag = Pattern.compile("block");
    Pattern pageTag = Pattern.compile("page");
    int page = 0;
    int index = 0;
   
    public MBTEIPictureParser(DocumentSplit split, Parameters p) {
	super(split, p);
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