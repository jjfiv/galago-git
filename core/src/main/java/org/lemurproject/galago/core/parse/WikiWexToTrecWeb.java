package org.lemurproject.galago.core.parse;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Wikipedia WEX parser
 * 
 * @author jdalton
 *
 */
public class WikiWexToTrecWeb {

    BufferedReader reader;

    int m_curDoc = 0;
    private PrintWriter m_outputWriter;

    /** Creates a new create of TrecTextParser */
    public WikiWexToTrecWeb(File m_outputFile, BufferedReader reader) throws FileNotFoundException, IOException {
        this.reader = reader;
        m_outputWriter = new PrintWriter(m_outputFile);
    }

    public void convert() throws IOException {
        // entire document exists on a single line -
        String line;
        String[] data;
        // data is split by tabs
        while ((line = reader.readLine()) != null) {
            data = line.split("\t");
            if (data.length > 7) {
                String wikiId = data[0];
                String wikiTitle = data[1];
                String lastModified = data[2];
                String wikiXml = data[3];
                String rawWikiText = data[4];
                String freebaseNames = data[5];
                String freebaseTypes = data[6];
                String categories = data[7];

                String redirects = "";
                if (data.length > 8) {
                    redirects = data[8];
                }

                String kbName = "";
                if (data.length > 10) {
                    kbName = data[10];
                }

                String kbId = "";
                if (data.length > 11) {
                    kbId = data[11];
                }

                String kbType = "";
                if (data.length > 12) {
                    kbType = data[12];
                }


                StringBuilder documentText = new StringBuilder();
                createField("title", wikiTitle, documentText, true, true);
                createField("timestamp", lastModified, documentText, true, false);
                createField("fbname", freebaseNames, documentText, true, true);
                createField("fbtype", freebaseTypes, documentText, true, false);
                createField("category", categories, documentText, true, false);
                createField("redirect", redirects, documentText, true, true);

                int inlinkCount=-1;
                if (data.length > 9) {
                    String anchors = data[9];
                    inlinkCount = handleAnchors("anchor", anchors, documentText, true);
                }

                boolean parseXml = false;
                if (parseXml) {
                    try {
                        String text = xmlToText(wikiXml);
                        String cleanText = cleanText(text);
                        createField("text", cleanText, documentText, false, true);
                    } catch (Exception e) {
                        System.err.println("Error parsing xml." + e.toString());
                    }
                } else {

                    String cleanText = cleanText(rawWikiText);
                    createField("text", cleanText, documentText, false, true);

                }

                // constructing the galago document -- OR make the string and write it out to a file.

                //				//documentText.append("</body>");

                m_outputWriter.println("\n<DOC>");
                m_outputWriter.print("<DOCNO>");
                m_outputWriter.print(wikiId);
                String url = "http://en.wikipedia.org/wiki/" + wikiTitle.replace(" ","_");
                m_outputWriter.print("</DOCNO>\n<DOCHDR>\n" + url + "\n</DOCHDR>\n");
                m_outputWriter.println(documentText);
                m_outputWriter.println("\n</DOC>\n");

            } else {
                int minLen = Math.min(100, line.length());
                Logger.getLogger(getClass().toString()).log(Level.WARNING,
                        "Error Line: " + line.substring(0, minLen) + " num fields: " + data.length);
            }
        }
        m_outputWriter.close();
    }

    private String cleanText(String rawText) {
        String newText = rawText.replace("\\n"," ");
        newText = newText.replace("#tag:ref", "");

        boolean containsBrackets = newText.indexOf("[[",0) > -1 ? true : false;

        // clean it up
        if (containsBrackets) {
            int curIndex = 0;
            StringBuilder sb = new StringBuilder();
            while (curIndex < newText.length()) {
                int startBracket = newText.indexOf("[[",curIndex);
                if (startBracket == -1) {
                    break;
                }
                sb.append(newText.subSequence(curIndex, startBracket));
                int endBracket = newText.indexOf("]]", startBracket);
                if (endBracket == -1) {
                    endBracket = newText.indexOf("]", startBracket);
                    if (endBracket == -1) {
                        System.out.println("No ending markup tag backets in String: " + newText);
                        break;
                    } else {
                        curIndex = endBracket+1;
                    }
                } else {
                    curIndex = endBracket+2;
                }

            }
            sb.append(newText.subSequence(curIndex, newText.length()));
            newText = sb.toString();
        }
        return newText;
    }

    private int handleAnchors(String fieldName, String anchors, StringBuilder sb, boolean tokenize) {
        if (anchors == null || anchors.length() == 0) {
            return 0;
        }

        int inlinkCount = 0;
        String[] anchorList = anchors.split(",");
        for (String anchorPair : anchorList) {
            Pattern anchorPattern = Pattern.compile("(.+):(\\d+)");
            Matcher m = anchorPattern.matcher(anchorPair);
            if (m.find()) {
                int anchorFieldsCnt = m.groupCount();
                if (anchorFieldsCnt != 2) {
                    throw new IllegalArgumentException("Invalid number of fields in anchor text." + anchorPair);
                }
                String text = m.group(1);
                String cntString = m.group(2);

                int count = Integer.parseInt(cntString);
                inlinkCount +=count;
                for (int i=0; i < count; i++) {
                    start(fieldName, sb, tokenize);
                    sb.append(text.trim());
                    end(fieldName, sb);  

                }
            }
        }
        return inlinkCount;
    }

    private String createField(String fieldName, String fieldValue, StringBuilder sb, boolean compound, boolean tokenize) {
        if (fieldValue == null || fieldValue.length() == 0) {
            return "";
        }
        if (compound) {
            String[] vals = split(fieldValue);
            for (String val : vals) {
                start(fieldName, sb, tokenize);
                sb.append(val.trim());
                end(fieldName, sb);
            }
        } else {
            start(fieldName, sb, tokenize);
            sb.append(fieldValue.trim());
            end(fieldName, sb);
        }
        //System.out.println(fieldValue);
        return sb.toString();
    }

    private void start(String tag, StringBuilder sb, boolean tokenize) {
        sb.append("<");
        sb.append(tag);
       
        sb.append(">");
    }

    private void end(String tag, StringBuilder sb) {
        sb.append("</");
        sb.append(tag);
        sb.append("> \n");
    }

    private String[] split(String compoundField) {
        return compoundField.split(",");
    }
    private String xmlToText(String wikiXml) throws Exception {
        StringBuilder sb = new StringBuilder();
        DocumentBuilderFactory dbf =
                DocumentBuilderFactory.newInstance();

        dbf.setNamespaceAware(false);
        dbf.setValidating(false);

        DocumentBuilder builder = dbf.newDocumentBuilder();

        wikiXml = wikiXml.replaceAll("\\\n", "\n");
        //System.out.println(wikiXml);
        InputSource is = new InputSource();
        is.setCharacterStream(new StringReader(wikiXml));
        org.w3c.dom.Document doc = builder.parse(is);

        NodeList nodes = doc.getChildNodes();
        for (int i=0; i<nodes.getLength(); i++) {
            Node childNode = nodes.item(i);
            String text = getText(childNode);
            sb.append(text);
            //System.out.println(text);
        }
        return sb.toString();
    }

    /**
     * Return the text that a node contains. This routine:<ul>
     * <li>Ignores comments and processing instructions.
     * <li>Concatenates TEXT nodes, CDATA nodes, and the results of
     *     recursively processing EntityRef nodes.
     * <li>Ignores any element nodes in the sublist.
     *     (Other possible options are to recurse into element 
     *      sublists or throw an exception.)
     * </ul>
     * @param    node  a  DOM node
     * @return   a String representing its contents
     */
    public String getText(Node node) {
        StringBuffer result = new StringBuffer();
        if (! node.hasChildNodes()) return "";

        NodeList list = node.getChildNodes();
        for (int i=0; i < list.getLength(); i++) {
            Node subnode = list.item(i);
            //System.out.println(subnode.getNodeValue() + "; " + subnode.getTextContent() + "\n");
            if (subnode.getNodeType() == Node.TEXT_NODE) {
                result.append(subnode.getNodeValue() + " ");
            }
            else if (subnode.getNodeType() ==
                    Node.CDATA_SECTION_NODE) 
            {
                result.append(subnode.getNodeValue());
            }
            else if (subnode.getNodeType() ==
                    Node.ENTITY_REFERENCE_NODE) 
            {
                // Recurse into the subtree for text
                // (and ignore comments)
                result.append(getText(subnode));
            }
            else if (subnode.getNodeType() ==
                    Node.ELEMENT_NODE) 
            {
                // Recurse into the subtree for text
                // (and ignore comments)
                result.append(getText(subnode));
            }
            //System.out.println(subnode.getNodeType());
        }
        return result.toString();
    }

    public String cleanTitle(String title){
        return title.replaceAll(" ", "_");
    }
}
