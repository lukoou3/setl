package com.lk.setl.util;

import java.util.regex.Pattern;

public class StringUtils {

    /**
     * Given a hex string this will return the byte array corresponding to the string .
     *
     * @param hex the hex String array
     * @return a byte array that is a hex string representation of the given string. The size of the
     *     byte array is therefore hex.length/2
     */
    public static byte[] hexStringToByte(final String hex) {
        final byte[] bts = new byte[hex.length() / 2];
        for (int i = 0; i < bts.length; i++) {
            bts[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
        }
        return bts;
    }

    public static String escapeLikeRegex(String pattern, char escapeChar) {
        StringBuilder out = new StringBuilder();
        int length = pattern.length();

        for (int i = 0; i < length; i++) {
            char c = pattern.charAt(i);
            if (c == escapeChar && i + 1 < length) {
                i++;
                c = pattern.charAt(i);
                if (c == '_' || c == '%') {
                    out.append(Pattern.quote(Character.toString(c)));
                } else if (c == escapeChar) {
                    out.append(Pattern.quote(Character.toString(c)));
                } else {
                    throw new IllegalArgumentException(String.format("the escape character is not allowed to precede '%s'", Character.toString(c)));
                }
            } else if (c == escapeChar) {
                throw new IllegalArgumentException("it is not allowed to end with the escape character");
            } else if (c == '_') {
                out.append(".");
            } else if (c == '%') {
                out.append(".*");
            } else {
                out.append(Pattern.quote(Character.toString(c)));
            }
        }

        return "(?s)" + out.toString(); // (?s) enables dotall mode, causing "." to match new lines
    }

    public static String substringSQL(String string, int pos, int len){
        if(string == null){
            return null;
        }

        int length = string.length();
        if (pos > length) {
            return "";
        }

        int start = 0;
        int end;
        if (pos > 0) {
            start = pos - 1;
        } else if (pos < 0) {
            start = length + pos;
        }
        if ((length - start) < len) {
            end = length;
        } else {
            end = start + len;
        }
        start = Math.max(start, 0); // underflow
        if (start >= end) {
            return "";
        }

        return  string.substring(start, end);
    }

    public static String trim(String srcStr, boolean leading, boolean trailing, String trimStr) {
        trimStr = trimStr == null || trimStr.isEmpty() ? " " : trimStr;
        int begin = 0, end = srcStr.length();
        if (leading) {
            while (begin < end && trimStr.indexOf(srcStr.charAt(begin)) >= 0) {
                begin++;
            }
        }
        if (trailing) {
            while (end > begin && trimStr.indexOf(srcStr.charAt(end - 1)) >= 0) {
                end--;
            }
        }
        // substring() returns self if start == 0 && end == length()
        return srcStr.substring(begin, end);
    }
}
