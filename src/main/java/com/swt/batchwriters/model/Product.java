package com.swt.batchwriters.model;


import com.thoughtworks.xstream.annotations.XStreamAlias;

import java.math.BigDecimal;

public class Product {
    private int productId;
    private String productName;
    // to add the alias for xml tag if this is not specified then parameter name will becomes the tag name
    @XStreamAlias("ProductDescription")
    private String productDesc;
    private BigDecimal price;
    private Integer unit;

    @Override
    public String toString() {
        return "Product{" +
                "productId=" + productId +
                ", productName='" + productName + '\'' +
                ", productDesc='" + productDesc + '\'' +
                ", price=" + price +
                ", unit=" + unit +
                '}';
    }

    public int getProductId() {
        return productId;
    }

    public void setProductId(int productId) {
        this.productId = productId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductDesc() {
        return productDesc;
    }

    public void setProductDesc(String productDesc) {
        this.productDesc = productDesc;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public Integer getUnit() {
        return unit;
    }

    public void setUnit(Integer unit) {
        this.unit = unit;
    }
}
