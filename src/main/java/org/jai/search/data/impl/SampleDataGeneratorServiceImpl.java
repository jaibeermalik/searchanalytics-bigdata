package org.jai.search.data.impl;

import org.jai.search.data.SampleDataGeneratorService;
import org.jai.search.model.Category;
import org.jai.search.model.Product;
import org.jai.search.model.ProductGroup;
import org.jai.search.model.ProductProperty;
import org.jai.search.model.SearchFacetName;
import org.jai.search.model.Specification;

import org.elasticsearch.common.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
public class SampleDataGeneratorServiceImpl implements SampleDataGeneratorService
{
    private static final Logger logger = LoggerFactory.getLogger(SampleDataGeneratorServiceImpl.class);

    private static Set<Category> hierarchicalCategories = new HashSet<Category>();

    private static Set<ProductProperty> productProperties = new HashSet<ProductProperty>();

    private static List<ProductGroup> productGroups = new ArrayList<ProductGroup>();

    private static List<Product> products = new ArrayList<Product>();
    static
    {
        hierarchicalCategories = generateHierarchicalCategoryData();
        productProperties = getProductProperties();
        products = generateSampleData();
        productGroups = getProductGroupSampleData();
    }

    @Override
    public List<ProductGroup> generateProductGroupSampleData()
    {
        return productGroups;
    }

    private static List<Product> generateSampleData()
    {
        final List<Product> products = new ArrayList<Product>();
        for (int i = 1; i <= 50; i++)
        {
            final Product product = new Product();
            product.setId(Long.valueOf(i));
            product.setTitle("Title " + i);
            product.setDescription("Description" + i);
            product.setAvailableOn(new DateTime().plusDays(i).toDate());
            product.addKeyword("Keyword " + i);
            product.setPrice(BigDecimal.valueOf(i));
            product.setSoldOut(i % 2 == 0 ? true : false);
            product.setBoostFactor(i / 10000f);
            if (i < 5)
            {
                product.addCategory(findCategory(hierarchicalCategories, MACBOOK_AIR));
                product.addCategory(findCategory(hierarchicalCategories, APPLE));
                product.addCategory(findCategory(hierarchicalCategories, RED));
                product.addCategory(findCategory(hierarchicalCategories, AGE_18_PLUS_YEARS));
                product.addProductProperty(findProductProperty(productProperties, PRODUCTPROPERTY_SIZE_21_INCH, PRODUCTPROPERTY_COLOR_BROWN));
                product.addProductProperty(findProductProperty(productProperties, PRODUCTPROPERTY_SIZE_17_INCH,
                        PRODUCTPROPERTY_COLOR_PURPLE));
                product.addSpecification(new Specification(RESOLUTON_3200_1800, MEMORY_8_GB));
                product.addSpecification(new Specification(RESOLUTON_1920_1200, MEMORY_6_GB));
            }
            else if (i >= 5 && i <= 10)
            {
                product.addCategory(findCategory(hierarchicalCategories, MACBOOK_PRO));
                product.addCategory(findCategory(hierarchicalCategories, APPLE));
                product.addCategory(findCategory(hierarchicalCategories, BLUE));
                product.addProductProperty(findProductProperty(productProperties, PRODUCTPROPERTY_SIZE_15_INCH,
                        PRODUCTPROPERTY_COLOR_YELLOW));
                product.addProductProperty(findProductProperty(productProperties, PRODUCTPROPERTY_SIZE_17_INCH,
                        PRODUCTPROPERTY_COLOR_PURPLE));
                product.addSpecification(new Specification(RESOLUTON_1920_1080, MEMORY_6_GB));
                product.addSpecification(new Specification(RESOLUTON_1920_1200, MEMORY_6_GB));
            }
            else if (i > 10 && i <= 20)
            {
                product.addCategory(findCategory(hierarchicalCategories, HP));
                product.addCategory(findCategory(hierarchicalCategories, AGE_12_18_YEARS));
                product.addProductProperty(findProductProperty(productProperties, PRODUCTPROPERTY_SIZE_12_INCH, PRODUCTPROPERTY_COLOR_BLACK));
                product.addSpecification(new Specification(RESOLUTON_1920_1080, MEMORY_4_GB));
                product.addSpecification(new Specification(RESOLUTON_1920_1080, MEMORY_2_GB));
            }
            else
            {
                product.addCategory(findCategory(hierarchicalCategories, DELL));
                product.addCategory(findCategory(hierarchicalCategories, GREEN));
                product.addCategory(findCategory(hierarchicalCategories, AGE_0_12_YEARS));
                product.addProductProperty(findProductProperty(productProperties, PRODUCTPROPERTY_SIZE_13_INCH, PRODUCTPROPERTY_COLOR_GREY));
                product.addSpecification(new Specification(RESOLUTON_1024_758, MEMORY_2_GB));
                product.addSpecification(new Specification(RESOLUTON_1024_600, MEMORY_6144_MB));
            }
            products.add(product);
        }
        return products;
    }

    private static Set<ProductProperty> getProductProperties()
    {
        final Set<ProductProperty> productProperties = new HashSet<ProductProperty>();
        final String[] sizes = new String[] { PRODUCTPROPERTY_SIZE_12_INCH, PRODUCTPROPERTY_SIZE_13_INCH, PRODUCTPROPERTY_SIZE_15_INCH,
                PRODUCTPROPERTY_SIZE_17_INCH, PRODUCTPROPERTY_SIZE_21_INCH };
        final String[] colors = new String[] { PRODUCTPROPERTY_COLOR_BLACK, PRODUCTPROPERTY_COLOR_GREY, PRODUCTPROPERTY_COLOR_YELLOW,
                PRODUCTPROPERTY_COLOR_PURPLE, PRODUCTPROPERTY_COLOR_BROWN };
        for (int i = 1; i <= 5; i++)
        {
            final ProductProperty productProperty = new ProductProperty();
            productProperty.setId(Long.valueOf(i));
            // if (i <= 5)
            // {
            productProperty.setSize(sizes[i - 1]);
            productProperty.setColor(colors[i - 1]);
            // }
            // else
            // {
            // productProperty.setSize(sizes[j++]);
            // productProperty.setColor(colors[9 - i]);
            // }
            productProperties.add(productProperty);
        }
        logger.debug(productProperties.toString());
        return productProperties;
    }

    private static ProductProperty findProductProperty(final Set<ProductProperty> productProperties, final String size, final String color)
    {
        for (final ProductProperty productProperty : productProperties)
        {
            if (size.equals(productProperty.getSize()) && color.equals(productProperty.getColor()))
            {
                return productProperty;
            }
        }
        return null;
    }

    @Override
    public ProductProperty findProductProperty(final String size, final String color)
    {
        return findProductProperty(productProperties, size, color);
    }

    @Override
    public Product generateProductSampleDataFor(final Long productId)
    {
        for (final Product product : products)
        {
            if (product.getId().equals(productId))
            {
                return product;
            }
        }
        return null;
    }

    @Override
    public ProductProperty generateProductPropertySampleDataFor(final Long productPropertyId)
    {
        for (final ProductProperty productProperty : productProperties)
        {
            if (productProperty.getId().equals(productPropertyId))
            {
                return productProperty;
            }
        }
        return null;
    }

    @Override
    public ProductGroup generateProductGroupSampleDataFor(final Long productGroupId)
    {
        for (final ProductGroup productGroup : productGroups)
        {
            if (productGroup.getId().equals(productGroupId))
            {
                return productGroup;
            }
        }
        return null;
    }

    @Override
    public List<Product> generateProductsSampleData()
    {
        return products;
    }

    @Override
    public Set<ProductProperty> generateProductPropertySampleData()
    {
        return productProperties;
    }

    private static Set<Category> generateHierarchicalCategoryData()
    {
        final Category computerCategory = new Category(COMPUTER, null, SearchFacetName.SEARCH_FACET_TYPE_PRODUCT_TYPE.getCode());
        final Category laptops = new Category(LAPTOPS, computerCategory, SearchFacetName.SEARCH_FACET_TYPE_PRODUCT_TYPE.getCode());
        final Category macbookLaptops = new Category(MACBOOK, laptops, SearchFacetName.SEARCH_FACET_TYPE_PRODUCT_TYPE.getCode());
        final Category macbookProLaptops = new Category(MACBOOK_PRO, macbookLaptops,
                SearchFacetName.SEARCH_FACET_TYPE_PRODUCT_TYPE.getCode());
        final Category macbookAirLaptops = new Category(MACBOOK_AIR, macbookLaptops,
                SearchFacetName.SEARCH_FACET_TYPE_PRODUCT_TYPE.getCode());
        final Category chrmoebookLaptops = new Category(CHROMEBOOK, laptops, SearchFacetName.SEARCH_FACET_TYPE_PRODUCT_TYPE.getCode());
        final Category netbookLaptops = new Category(NETBOOK, laptops, SearchFacetName.SEARCH_FACET_TYPE_PRODUCT_TYPE.getCode());
        final Category brands = new Category(BRANDS, null, SearchFacetName.SEARCH_FACET_TYPE_BRAND.getCode());
        final Category appleBrand = new Category(APPLE, brands, SearchFacetName.SEARCH_FACET_TYPE_BRAND.getCode());
        final Category hpBrand = new Category(HP, brands, SearchFacetName.SEARCH_FACET_TYPE_BRAND.getCode());
        final Category dellBrand = new Category(DELL, brands, SearchFacetName.SEARCH_FACET_TYPE_BRAND.getCode());
        final Category age = new Category(AGE, null, SearchFacetName.SEARCH_FACET_TYPE_AGE.getCode());
        final Category kidAge = new Category(AGE_0_12_YEARS, age, SearchFacetName.SEARCH_FACET_TYPE_AGE.getCode());
        final Category teenAge = new Category(AGE_12_18_YEARS, age, SearchFacetName.SEARCH_FACET_TYPE_AGE.getCode());
        final Category adultAge = new Category(AGE_18_PLUS_YEARS, age, SearchFacetName.SEARCH_FACET_TYPE_AGE.getCode());
        final Category colors = new Category(COLORS, null, SearchFacetName.SEARCH_FACET_TYPE_COLOR.getCode());
        final Category redColor = new Category(RED, colors, SearchFacetName.SEARCH_FACET_TYPE_COLOR.getCode());
        final Category greenColor = new Category(GREEN, colors, SearchFacetName.SEARCH_FACET_TYPE_COLOR.getCode());
        final Category blueColor = new Category(BLUE, colors, SearchFacetName.SEARCH_FACET_TYPE_COLOR.getCode());
        final Set<Category> categories = new HashSet<Category>();
        categories.add(computerCategory);
        categories.add(laptops);
        categories.add(macbookLaptops);
        categories.add(macbookProLaptops);
        categories.add(macbookAirLaptops);
        categories.add(chrmoebookLaptops);
        categories.add(netbookLaptops);
        categories.add(brands);
        categories.add(appleBrand);
        categories.add(hpBrand);
        categories.add(dellBrand);
        categories.add(age);
        categories.add(kidAge);
        categories.add(teenAge);
        categories.add(adultAge);
        categories.add(colors);
        categories.add(redColor);
        categories.add(greenColor);
        categories.add(blueColor);
        return categories;
    }

    private static List<ProductGroup> getProductGroupSampleData()
    {
        final List<ProductGroup> productGroups = new ArrayList<ProductGroup>();
        int count = 0;
        for (int i = 1; i <= 10; i++)
        {
            final ProductGroup productGroup = new ProductGroup();
            productGroup.setId(Long.valueOf(i));
            productGroup.setGroupTitle("groupTitle" + i);
            productGroup.setGroupDescription("groupDescription" + i);
            while (count < i * 5)
            {
                final Product product = products.get(count);
                productGroup.addProduct(product);
                count = count + 1;
            }
            productGroups.add(productGroup);
        }
        return productGroups;
    }

    private static Category findCategory(final Set<Category> categories, final String catName)
    {
        for (final Category category : categories)
        {
            if (category.getName().equals(catName))
            {
                return category;
            }
        }
        return null;
    }
}
