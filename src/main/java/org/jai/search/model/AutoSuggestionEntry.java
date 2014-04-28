package org.jai.search.model;

import org.apache.commons.lang.builder.ToStringBuilder;

public class AutoSuggestionEntry
{
    private String term;

    private int count;

    public AutoSuggestionEntry(final String term, final int count)
    {
        this.term = term;
        this.count = count;
    }

    public String getTerm()
    {
        return term;
    }

    public void setTerm(final String term)
    {
        this.term = term;
    }

    public int getCount()
    {
        return count;
    }

    public void setCount(final int count)
    {
        this.count = count;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this).append(term).append(count).toString();
    }
}
