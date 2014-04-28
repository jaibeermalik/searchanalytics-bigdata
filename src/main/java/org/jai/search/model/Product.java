package org.jai.search.model;

import org.apache.commons.lang.builder.ToStringBuilder;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Product
{
    private Long id;

    private String title;

    private String description;

    private BigDecimal price;

    private boolean soldOut;

    private Date availableOn;

    private float boostFactor = 1.0f;

    private List<String> keywords = new ArrayList<String>();

    private final List<Category> categories = new ArrayList<Category>();

    private final List<ProductProperty> productProperties = new ArrayList<ProductProperty>();

    private List<Specification> specifications = new ArrayList<Specification>();

    public Long getId()
    {
        return id;
    }

    public void setId(final Long id)
    {
        this.id = id;
    }

    public String getTitle()
    {
        return title;
    }

    public void setTitle(final String title)
    {
        this.title = title;
    }

    public String getDescription()
    {
        return description;
    }

    public void setDescription(final String description)
    {
        this.description = description;
    }

    public List<String> getKeywords()
    {
        return keywords;
    }

    public void setKeywords(final List<String> keywords)
    {
        this.keywords = keywords;
    }

    public BigDecimal getPrice()
    {
        return price;
    }

    public void setPrice(final BigDecimal price)
    {
        this.price = price;
    }

    public boolean isSoldOut()
    {
        return soldOut;
    }

    public void setSoldOut(final boolean soldOut)
    {
        this.soldOut = soldOut;
    }

    public Date getAvailableOn()
    {
        return availableOn;
    }

    public void setAvailableOn(final Date availableOn)
    {
        this.availableOn = availableOn;
    }

    public void addKeyword(final String keyword)
    {
        keywords.add(keyword);
    }

    public float getBoostFactor()
    {
        return boostFactor;
    }

    public void setBoostFactor(final float boostFactor)
    {
        this.boostFactor = boostFactor;
    }

    public void addCategory(final Category category)
    {
        categories.add(category);
    }

    public List<Category> getCategories()
    {
        return categories;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this).append(id).append(title).append(description).append(price).append(soldOut).append(availableOn)
                .append(keywords).append(boostFactor).append(categories).toString();
    }

    public List<ProductProperty> getProductProperties()
    {
        return productProperties;
    }

    public void addProductProperty(final ProductProperty productProperty)
    {
        productProperties.add(productProperty);
    }

    public List<Specification> getSpecifications()
    {
        return specifications;
    }

    public void setSpecifications(final List<Specification> specifications)
    {
        this.specifications = specifications;
    }

    public void addSpecification(final Specification specification)
    {
        specifications.add(specification);
    }

    public boolean categoryNameExists(final String catName)
    {
        for (final Category category : categories)
        {
            if (category.getName().equals(catName))
            {
                return true;
            }
        }
        return false;
    }
}
