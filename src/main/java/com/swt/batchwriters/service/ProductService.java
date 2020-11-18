package com.swt.batchwriters.service;

import com.swt.batchwriters.model.Product;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;


@Service
public class ProductService {

    public Product getProducts(){
        RestTemplate restTemplate = new RestTemplate();
        String url = "http://localhost:8080/products";
        Product p = restTemplate.getForObject(url, Product.class);
        return p;
    }
}
