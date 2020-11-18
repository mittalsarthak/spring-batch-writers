package com.swt.batchwriters.processor;

import com.swt.batchwriters.model.Product;
import org.springframework.batch.item.ItemProcessor;

public class ProductProcessor implements ItemProcessor<Product, Product> {
    @Override
    public Product process(Product product) throws Exception {
        if(product.getProductId() == 2) {
            throw new RuntimeException("Id is 2");
        } else
            product.setProductDesc(product.getProductDesc().toUpperCase());
        return product;
    }
}
