package com.elasticsearch.demo.service;

import com.elasticsearch.demo.model.Product;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;

public interface ProductService {
    Product createProduct(Product product);

    boolean createBulkProduct(MultipartFile file);

    Iterable<Product> getAllProducts() throws IOException;

    Product getProductById(String id) throws IOException;

    Product updateProduct(String id, Product product) throws IOException;

    boolean deleteProduct(String id) throws IOException;

    List<Product> getProductByCategory(String category) throws IOException;

    List<Product> searchByPriceRange(double minPrice, double maxPrice) throws IOException;
}
