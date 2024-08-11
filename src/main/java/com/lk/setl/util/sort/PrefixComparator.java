package com.lk.setl.util.sort;

/**
 * Compares 8-byte key prefixes in prefix sort. Subclasses may implement type-specific
 * comparisons, such as lexicographic comparison for strings.
 */
public abstract class PrefixComparator {
  public abstract int compare(long prefix1, long prefix2);
}
