// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.parse;

import java.util.ArrayList;
import org.lemurproject.galago.core.types.ExtractedLink;

/**
 *
 * @author trevor
 */
public class DocumentLinkData {
    public int identifier;
    public String url;
    public int textLength;

    public ArrayList<ExtractedLink> links;
    
    public DocumentLinkData() {
        links = new ArrayList<ExtractedLink>();
        identifier = -1;
        url = "";
        textLength = 0;
    }
}
