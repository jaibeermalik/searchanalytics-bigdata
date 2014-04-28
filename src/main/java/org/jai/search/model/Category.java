package org.jai.search.model;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

public class Category
{
    private final String name;

    private final String type;

    private final Category parentCategory;

    public Category(final String catName, final Category parent, final String catType)
    {
        this.name = catName;
        this.parentCategory = parent;
        this.type = catType;
    }

    public String getName()
    {
        return name;
    }

    public Category getParentCategory()
    {
        return parentCategory;
    }

    public String getType()
    {
        return type;
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder().append(name).hashCode();
    }

    @Override
    public boolean equals(final Object obj)
    {
        final Category other = (Category) obj;
        return new EqualsBuilder().append(name, other.getName()).isEquals();
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this).append(name).append(type).toString();
    }
}
