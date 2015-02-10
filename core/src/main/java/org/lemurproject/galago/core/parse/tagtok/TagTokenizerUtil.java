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

	/**
	 * Scans through the token, removing apostrophes and converting
	 * uppercase to lowercase letters.
	 */
	public static String normalizeSimple(String token) {
		char[] chars = token.toCharArray();
		int j = 0;

		for (int i = 0; i < chars.length; i++) {
			char c = chars[i];
			boolean isAsciiUppercase = (c >= 'A' && c <= 'Z');
			boolean isApostrophe = (c == '\'');

			if (isAsciiUppercase) {
				chars[j] = (char) (chars[i] + 'a' - 'A');
			} else if (isApostrophe) {
				// it's an apostrophe, skip it
				j--;
			} else {
				chars[j] = chars[i];
			}

			j++;
		}

		token = new String(chars, 0, j);
		return token;
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

	public static String normalizeComplex(String token) {
    token = normalizeSimple(token);
    token = token.toLowerCase();

    return token;
  }

	/**
   * This method scans the token, looking for uppercase characters and
   * special characters.  If the token contains only numbers and lowercase
   * letters, it needs no further processing, and it returns Clean.
   * If it also contains uppercase letters or apostrophes, it returns
   * NeedsSimpleFix.  If it contains special characters (especially Unicode
   * characters), it returns NeedsComplexFix.  Finally, if any periods are
   * present, this returns NeedsAcronymProcessing.
   */
  public static StringStatus checkTokenStatus(final String token) {
    StringStatus status = StringStatus.Clean;
    char[] chars = token.toCharArray();

    for (char c : chars) {
      boolean isAsciiLowercase = (c >= 'a' && c <= 'z');
      boolean isAsciiNumber = (c >= '0' && c <= '9');

      if (isAsciiLowercase || isAsciiNumber) {
        continue;
      }
      boolean isAsciiUppercase = (c >= 'A' && c <= 'Z');
      boolean isPeriod = (c == '.');
      boolean isApostrophe = (c == '\'');

      if ((isAsciiUppercase || isApostrophe) && status == StringStatus.Clean) {
        status = StringStatus.NeedsSimpleFix;
      } else if (!isPeriod) {
        status = StringStatus.NeedsComplexFix;
      } else {
        status = StringStatus.NeedsAcronymProcessing;
        break;
      }
    }

    return status;
  }
}
