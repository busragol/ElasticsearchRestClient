package com.elasticsearch.demo.controller;

import com.elasticsearch.demo.model.Product;
import com.elasticsearch.demo.service.ProductService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@RestController
@RequestMapping("/api/products")
public class ProductController {
    @Autowired
    private ProductService productService;


    @PostMapping
    public ResponseEntity<Product> createProduct(@RequestBody Product product) {
        Product savedProduct = productService.createProduct(product);
        return new ResponseEntity<>(savedProduct, HttpStatus.CREATED);
    }

    @PostMapping("/create/bulk")
    public ResponseEntity<List<Product>> createBulkProduct(@RequestBody MultipartFile file) throws IOException {
        boolean result = productService.createBulkProduct(file);
        if (result) {
            return ResponseEntity.noContent().build();
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping
    public ResponseEntity<Iterable<Product>> getAllProducts() throws IOException {
        Iterable<Product> allProducts = productService.getAllProducts();
        return new ResponseEntity<>(allProducts, HttpStatus.OK);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Product> getProductById(@PathVariable String id) throws IOException {
        Product product = productService.getProductById(id);
        return ResponseEntity.ok(product);
    }

    @PutMapping("/{id}")
    public ResponseEntity<Product> updateProduct(@PathVariable String id, @RequestBody Product product) throws IOException {
        Product updatedProduct = productService.updateProduct(id, product);
        return ResponseEntity.ok(updatedProduct);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> deleteProduct(@PathVariable String id) throws IOException {
        boolean deleted = productService.deleteProduct(id);

        if (deleted) {
            return ResponseEntity.noContent().build();
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/search/{category}")
    public ResponseEntity<List<Product>> getProductByCategory(@PathVariable String category) throws IOException {
        List<Product> products = productService.getProductByCategory(category);
        return ResponseEntity.ok(products);
    }

    @GetMapping("searchByPriceRange")
    public ResponseEntity<List<Product>> searchByPriceRange(@RequestParam double minPrice, @RequestParam double maxPrice) throws IOException {
        List<Product> products = productService.searchByPriceRange(minPrice, maxPrice);
        return ResponseEntity.ok(products);
    }

}