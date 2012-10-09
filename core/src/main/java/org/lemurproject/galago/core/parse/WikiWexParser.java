package org.lemurproject.galago.core.parse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * Wikipedia WEX parser
 * 
 * @author jdalton
 *
 */
public class WikiWexParser implements DocumentStreamParser {

	BufferedReader reader;

	/** Creates a new instance of TrecTextParser */
	public WikiWexParser(BufferedReader reader) throws FileNotFoundException, IOException {
		this.reader = reader;
	}

	public Document nextDocument() throws IOException {
		// entire document exists on a single line -
		String line;
		String[] data;
		// data is split by tabs
		while ((line = reader.readLine()) != null) {
			data = line.split("\t");
			if (!filter(data)) {
				String wikiId = data[0];
				String wikiTitle = data[1];
				String lastModified = data[2];
				String wikiXml = data[3];
				String rawWikiText = data[4];
				
				String freebaseNames = "";
				if (data.length > 5) {
				 freebaseNames = data[5];
				}
				
				String freebaseTypes = "";
				if(data.length > 6) {
				    freebaseTypes = data[6];
				}
				String categories = "";
				if (data.length > 7) {
				    categories = data[7];
				}
				
				String redirects = "";
				if (data.length > 8) {
					redirects = data[8];
				}
				
				String kbName = "";
				if (data.length > 12) {
				    kbName = data[12];
				}
				
				String kbId = "";
                if (data.length > 13) {
                    kbId = data[13];
                }
                
                String kbType = "";
                if (data.length > 14) {
                    kbType = data[14];
                }

				
				StringBuilder documentText = new StringBuilder();
                createField("title", wikiTitle, documentText, true, true);
                createField("title-exact", wikiTitle, documentText, true, false);
                createField("timestamp", lastModified, documentText, true, false);
                createField("fbname-exact", freebaseNames, documentText, true, false);
				createField("fbname", freebaseNames, documentText, true, true);
				createField("fbtype", freebaseTypes, documentText, true, false);
				createField("category", categories, documentText, true, false);
				createField("redirect", redirects, documentText, true, true);
                createField("redirect-exact", redirects, documentText, true, false);
                createField("kb_class", kbType, documentText, true, false);
                createField("kb_name", kbName, documentText, true, true);
                createField("kb_name-exact", kbName, documentText, true, false);


				int inlinkCount=-1;
				if (data.length > 9) {
					String anchors = data[9];
					inlinkCount = parseInlinks("anchor", anchors, documentText, true);
					parseInlinks("anchor-exact", anchors, documentText, false);

				}
				
				String sourceInlinks = "";
                if (data.length > 10) {
                    String sources = data[10];
                    Iterable<String> sourceData = parseSources(sources);
                    StringBuilder sb = new StringBuilder();
                    for (String source : sourceData) {
                        sb.append(source);
                        sb.append(" ");
                    }
                    sourceInlinks = sb.toString();
                }
                
                int externalInlinkCount = -1;
                if (data.length > 11) {
                    String stanfLinks = data[11];
                    externalInlinkCount = parseStanfordLinkData("stanf_anchor", stanfLinks, documentText, true);
                    parseStanfordLinkData("stanf_anchor-exact", stanfLinks, documentText, false);

                }
                
                String contextLinks = "";
                if (data.length > 15) {
                    String rawContext = data[15];
                    contextLinks = parseContext("context_links", rawContext);
                }
				
				boolean parseXml = false;
				if (parseXml) {
					try {
						String text = xmlToText(wikiXml);
	                    String cleanText = cleanText(text);
						createField("text", cleanText, documentText, false, true);
					} catch (Exception e) {
						System.err.println("Error parsing xml." + e.toString());
						return null;
					}
				} else {
                    
                    String cleanText = cleanText(rawWikiText);
                    createField("text", cleanText, documentText, false, true);
					
				}
				//documentText.append("</body>");
				Document res = new Document(cleanTitle(wikiTitle), documentText.toString());
				//System.out.println(documentText.toString()+"\n\n");
				//Logger.getLogger(getClass().toString()).log(Level.WARNING, "processing doc: " + wikiTitle);
				res.metadata.put("wikiId", wikiId);
				res.metadata.put("title", wikiTitle);
				res.metadata.put("lastModified", lastModified);
                res.metadata.put("inlink", inlinkCount+"");
                res.metadata.put("kbName", kbName);
                res.metadata.put("kbId", kbId);
                res.metadata.put("kbType", kbType);
                res.metadata.put("fbname", freebaseNames);
                res.metadata.put("category", categories);
                res.metadata.put("fbtype", freebaseTypes);
                res.metadata.put("srcInlinks", sourceInlinks);
                res.metadata.put("xml", wikiXml);
                res.metadata.put("externalLinkCount", externalInlinkCount+"");
                res.metadata.put("contextLinks", contextLinks);
                
				return res;
			} else {
				int minLen = Math.min(100, line.length());
				Logger.getLogger(getClass().toString()).log(Level.WARNING,
			              "Error Line: " + line.substring(0, minLen) + " num fields: " + data.length);
			}
		}
		return null;
	}

    private boolean filter(String[] data) {
        boolean filter = false;
        if (data.length < 5) {
            filter = true;
        }
        
        String wikiTitle = data[1];
        if (wikiTitle.startsWith("File:") || wikiTitle.startsWith("Category:") || wikiTitle.startsWith("Template:") || wikiTitle.startsWith("List of") || wikiTitle.startsWith("Lists of")) {
            return true;
        }
        
        return filter;
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
	
	
	private int parseInlinks(String fieldName, String anchors, StringBuilder sb, boolean tokenize) {
	    if (anchors == null || anchors.length() == 0) {
            return 0;
        } else {
            
            // we have link data, parse it.
            int inlinkCount = 0;
            
            int curIdx = 0;
            final String startText = "<TXT>";
            final String endText = "</TXT>";

            final String startCnt = "<CNT>";
            final String endCnt = "</CNT>";
            
            while ((curIdx = anchors.indexOf(startText, curIdx)) > -1) {
                int endTxtIdx = anchors.indexOf(endText, curIdx);
                String anchorText = anchors.substring(curIdx + startText.length(), endTxtIdx);
                
                int startCntIdx = anchors.indexOf(startCnt, curIdx);
                int endCntIdx = anchors.indexOf(endCnt, startCntIdx);
                String countString = anchors.substring(startCntIdx + startCnt.length(), endCntIdx);
                int count = Integer.parseInt(countString);
                
                inlinkCount +=count;
                for (int i=0; i < count; i++) {
                    start(fieldName, sb, tokenize);
                    sb.append(anchorText.trim());
                    end(fieldName, sb);  
                    
                }
                curIdx = endCntIdx;
            }
            
            return inlinkCount;
        }
	}
	
	   private String parseContext(String fieldName, String context) {
	        if (context == null || context.length() == 0) {
	            return "";
	        } else {
	            	            
	            int curIdx = 0;
	            final String startText = "<CTX>";
	            final String endText = "</CTX>";
	            
	            HashMap<String, Integer> cooccurringLinks = new HashMap<String, Integer>();
	            
	            while ((curIdx = context.indexOf(startText, curIdx)) > -1) {
	                int endTxtIdx = context.indexOf(endText, curIdx);
	                String contextLinks = context.substring(curIdx + startText.length(), endTxtIdx);
	                String[] curContext = contextLinks.split("\\s+");
	                
	                for (String link : curContext) {
	                    Integer curCount = cooccurringLinks.get(link);
	                    if (curCount == null) {
	                        curCount = 0;
	                    }
	                    curCount += 1;
	                    cooccurringLinks.put(link, curCount);
	                }
	                curIdx = endTxtIdx;
	            }
	            
	            StringBuilder sb = new StringBuilder();
	            Set<String> keys = cooccurringLinks.keySet();
	            for (String key: keys) {
	                sb.append(key + "\t" + cooccurringLinks.get(key).toString() + "\n");
	            }
	            
	            
	            return sb.toString();
	        }
	    }
	
	
	private int parseStanfordLinkData(String fieldName, String anchors, StringBuilder sb, boolean tokenize) {
        if (anchors == null || anchors.length() == 0) {
            return 0;
        } else {
            
            // we have link data, parse it.
            int inlinkCount = 0;
            
            int curIdx = 0;
            final String startText = "<ATEXT>";
            final String endText = "</ATEXT>";

            final String startScores = "<SCORES>";
            final String endScores = "</SCORES>";
            
            while ((curIdx = anchors.indexOf(startText, curIdx)) > -1) {
                int endTxtIdx = anchors.indexOf(endText, curIdx);
                String anchorText = anchors.substring(curIdx + startText.length(), endTxtIdx);
                
                anchorText = cleanAnchors(anchorText);
                int startCntIdx = anchors.indexOf(startScores, curIdx);
                int endCntIdx = anchors.indexOf(endScores, startCntIdx);
                String scoreString = anchors.substring(startCntIdx + startScores.length(), endCntIdx);
                
                int count = 1;
                if (!scoreString.equals("null")) {
                    String[] scores = scoreString.split("\\s+");
                    for (String score : scores) {
                        
                        String startExternal="W:";
                        int externalIdx = score.indexOf(startExternal);
                        if ( externalIdx > -1) {
                           int dividerIdx = score.indexOf('/');
                           String numerator = score.substring(externalIdx + startExternal.length(), dividerIdx);
                           String denominator = score.substring(dividerIdx+1);
                           count = Integer.parseInt(numerator);
                           inlinkCount = Integer.parseInt(denominator);                           
                        }
                    }    
                }
                if (!filterAnchorText(anchorText, count)) {
                    for (int i=0; i < count; i++) {
                        start(fieldName, sb, tokenize);
                        sb.append(anchorText.trim());
                        end(fieldName, sb);  

                    }
                }
                curIdx = endCntIdx;
            }
            
            return inlinkCount;
        }
    }
	
	private String cleanAnchors(String text) {
	    String[] termsToReplace = {"wikipedia the free encyclopedia", "wikipedia, the free encyclopedia", "http://en.wikipedia.org/wiki/", "wikipedia"};
	   	    
	    String clean = text; 
	    for (String dirtyStrings : termsToReplace ) {
	        String lower = clean.toLowerCase();
	        int dirtyIdx = lower.indexOf(dirtyStrings);
	        if (dirtyIdx > -1) {
	            clean = clean.substring(0, dirtyIdx);
	            int endIdx = dirtyIdx + dirtyStrings.length();
	            if (endIdx < clean.length()) {
	                clean += clean.substring(endIdx);
	            }
	        }
	    }
	    return clean;
	}
	
	private boolean filterAnchorText(String text, int count) {
	    String[] wikiStopwords = {"more....", "wikipedia article", "source: wikipedia","here", "wiki", "wikipedia", "wikipedia the free encyclopedia", "en.wikipedia.org", "full article at wikipedia.org"};
	    Set<String> wikiStopwordSet = new HashSet<String>(Arrays.asList(wikiStopwords));
	    if (text.length() == 0 || (text.startsWith("[") && text.endsWith("]")) || wikiStopwordSet.contains(text.toLowerCase())) {
	        return true;
	    } else {
	        return false;
	    }
	}
	
	   private Iterable<String> parseSources(String sources) {
	        if (sources == null || sources.length() == 0) {
	            return new ArrayList<String>();
	        } else {
	            
	           Set<String> sourceList = new HashSet<String>();
	            // we have link data, parse it.
	            
	            int curIdx = 0;
	            final String startText = "<SRC_LINK>";
	            final String endText = "</SRC_LINK>";
	            
	            while ((curIdx = sources.indexOf(startText, curIdx)) > -1) {
	                int endTxtIdx = sources.indexOf(endText, curIdx);
	                String source = sources.substring(curIdx + startText.length(), endTxtIdx);
	                sourceList.add(source);
	                curIdx = endTxtIdx;
	            }   
	            return sourceList;
	        }
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
		
		if (!tokenize) {
		    sb.append(" tokenizeTagContent=\"false\"");
		}
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

	public static void main(String[] args) throws FileNotFoundException, IOException {
	    
//	    String test = "1234\tEurope\t2011-123-123\t<?xml version=1.0>stuff</xml>\tVarious groups of Europeans"+
//" and Chinese also integrated with the native population during that period.#tag:ref" +
//	            "[[File:Visayas regions.PNG|right|thumb|A map of the Visayas colour-coded according to the constituent regions."
//+"The major islands, from west to east, are Panay, Negros, Cebu, Bohol, Leyte, and Samar.]]Administratively, Bollywood films.\\n\\nOn 10"+
//	            "\tEurope\t/location/location\tcategory:place\tEuropean\t\t\t\t\t";
		File testFile = new File("/usr/aubury/scratch2/jdalton/aristotle.wex");
//	      WikiWexParser parser = new WikiWexParser(new BufferedReader(new StringReader(test)));

		WikiWexParser parser = new WikiWexParser(new BufferedReader(new FileReader(testFile)));
		Document doc = null;
		
		while ((doc = parser.nextDocument()) != null) {
		   
		    TagTokenizer tt = new TagTokenizer();
		    tt.addField("title");
		    tt.addField("category");
		    tt.addField("anchor");
		    tt.addField("anchor-exact");
            tt.addField("fbnames");
            tt.addField("stanf_anchor");

		    System.out.println(doc.toString());
		    tt.process(doc);
//		    System.out.println("PARSED TERMS");
//		    List<String> tokens = doc.terms;
//		    for (int i = 0; i < tokens.size(); i++) {
//	            System.out.println("Token: " + tokens.get(i));
//	        }
		    
		    System.out.println(doc.toString());
		}
		
		
	}

	@Override
	  public void close() throws IOException {
	    this.reader.close();
	  }
}
