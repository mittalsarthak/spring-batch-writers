package com.swt.batchwriters.processor;

import com.swt.batchwriters.model.Product;
import org.springframework.batch.item.ItemProcessor;

public class ProductProcessor implements ItemProcessor<Product, Product> {
    @Override
    public Product process(Product product) throws Exception {
        if(product.getProductId() == 2)
            return null;
        else
            product.setProductDesc(product.getProductDesc().toUpperCase());
        return product;
    }
}
