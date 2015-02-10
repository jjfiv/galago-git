package org.lemurproject.galago.core.parse.tagtok;

/**
 * @author trevor.
 * @see org.lemurproject.galago.core.parse.TagTokenizer
 */
public class TagTokenizerUtil {

	public static int NOT_FOUND = Integer.MIN_VALUE;

	public static int indexOfNonSpace(String text, int start) {
		if (start < 0) {
			return NOT_FOUND;
		}
		for (int i = start; i < text.length(); i++) {
			char c = text.charAt(i);
			if (!Character.isSpaceChar(c)) {
				return i;
			}
		}

		return NOT_FOUND;
	}

	/** Find the index of an '=' character between start and end in a string */
	public static int indexOfEquals(String text, int start, int end) {
    if (start < 0) {
			return NOT_FOUND;
    }
    for (int i = start; i < end; i++) {
      char c = text.charAt(i);
      if (c == '=') {
        return i;
      }
    }

		return NOT_FOUND;
  }

	public static int indexOfEndAttribute(String text, int start, int tagEnd) {
    if (start < 0) {
      return NOT_FOUND;
      // attribute ends at the first non-quoted space, or the first '>'.
    }
    boolean inQuote = false;
    boolean lastEscape = false;

    for (int i = start; i <= tagEnd; i++) {
      char c = text.charAt(i);

      if ((c == '\"' || c == '\'') && !lastEscape) {
        inQuote = !inQuote;
        if (!inQuote) {
          return i;
        }
      } else if (!inQuote && (Character.isSpaceChar(c) || c == '>')) {
        return i;
      } else if (c == '\\' && !lastEscape) {
        lastEscape = true;
      } else {
        lastEscape = false;
      }
    }

		return NOT_FOUND;
  }

}
