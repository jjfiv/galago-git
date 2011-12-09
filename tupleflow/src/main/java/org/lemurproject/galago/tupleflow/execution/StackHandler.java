// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 *
 * @author trevor
 */
public class StackHandler extends DefaultHandler {
    int levels = 0;
    StackHandler handler;

    public void startHandler(String uri, String localName, String qName, Attributes attributes) throws SAXException {
    }

    public void endHandler(String uri, String localName, String qName) throws SAXException {
    }

    public void endChild(StackHandler handler, String uri, String localName, String qName) throws SAXException {
    }

    public void unhandledStartElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
    }

    public void unhandledEndElement(String uri, String localName, String qName) throws SAXException {
    }

    public void unhandledCharacters(char[] buffer, int offset, int length) throws SAXException {
    }

    public void addHandler(StackHandler hand, String uri, String localName, String qName, Attributes attributes) throws SAXException {
        levels = 1;
        this.handler = hand;
        handler.startHandler(uri, localName, qName, attributes);
    }

    public void addHandler(StackHandler hand) throws SAXException {
        levels = 1;
        this.handler = hand;
        handler.startHandler(null, null, null, null);
    }

    public void characters(char[] buffer, int offset, int length) throws SAXException {
        if (handler != null) {
            handler.characters(buffer, offset, length);
        } else {
            unhandledCharacters(buffer, offset, length);
        }
    }

    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        if (handler != null) {
            levels++;
            handler.startElement(uri, localName, qName, attributes);
        } else {
            unhandledStartElement(uri, localName, qName, attributes);
        }
    }

    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (handler != null) {
            levels--;
            handler.endElement(uri, localName, qName);

            if (levels == 0) {
                handler.endHandler(uri, localName, qName);
                endChild(handler, uri, localName, qName);
                handler = null;
            }
        } else {
            unhandledEndElement(uri, localName, qName);
        }
    }
}
