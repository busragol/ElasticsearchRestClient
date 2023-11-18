package com.elasticsearch.demo.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.CreateOperation;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import com.elasticsearch.demo.model.Product;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@Service
public class ProductServiceImpl implements ProductService {

    @Autowired
    ElasticsearchClient client;

    @Override
    public Product createProduct(Product product) {
        try {
            IndexResponse response = client.index(i -> i
                    .index("products")
                    .id(product.getId())
                    .document(product));
            return product;
        } catch (IOException exception) {
            exception.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean createBulkProduct(MultipartFile file) {
        InputStream inputStream;
        try {
            inputStream = file.getInputStream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        List<Product> productList = getFileContent(file);
        return importDataToES(productList);
    }

    @Override
    public Iterable<Product> getAllProducts() throws IOException {

        SearchRequest request = new SearchRequest.Builder()
                .index("products")
                .build();
        SearchResponse<Product> response = client.search(request, Product.class);

        List<Hit<Product>> hits = response.hits().hits();

        List<Product> products = new ArrayList<>();
        for (Hit<Product> hit : hits) {
            Product product = hit.source();
            products.add(product);
        }

        return products;
    }

    @Override
    public Product getProductById(String id) throws IOException {
        GetRequest request = new GetRequest.Builder()
                .index("products")
                .id(id)
                .build();

        GetResponse<Product> response = client.get(request, Product.class);
        if (response.found()) {
            Product product = (Product) response.source();
            return product;
        } else {
            return null;
        }
    }

    @Override
    public Product updateProduct(String id, Product product) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest.Builder()
                .index("products")
                .id(id)
                .doc(product)
                .build();

        UpdateResponse updateResponse = client.update(updateRequest, Product.class);

        if (updateResponse.result() == Result.Updated || updateResponse.result() == Result.Created) {
            return product;
        } else {
            return null;
        }

    }

    @Override
    public boolean deleteProduct(String id) throws IOException {
        DeleteRequest request = new DeleteRequest.Builder()
                .index("products")
                .id(id)
                .build();
        DeleteResponse response = client.delete(request);

        if (response.result() == Result.Deleted) {
            return true;
        } else {
            return false;
        }

    }

    @Override
    public List<Product> getProductByCategory(String category) throws IOException {
        SearchRequest request = new SearchRequest.Builder()
                .index("products")
                .query(q -> q
                        .match(t -> t
                                .field("category")
                                .query(category)))
                .build();

        SearchResponse<Product> response = client.search(request, Product.class);

        List<Hit<Product>> hits = response.hits().hits();

        List<Product> products = new ArrayList<>();

        for (Hit<Product> hit : hits) {
            Product product = hit.source();
            products.add(product);
        }

        return products;
    }

    @Override
    public List<Product> searchByPriceRange(double minPrice, double maxPrice) throws IOException {
        SearchRequest request = new SearchRequest.Builder()
                .index("products")
                .query(q -> q
                        .range(r -> r
                                .field("price")
                                .gte(JsonData.of(minPrice))
                                .lte(JsonData.of(maxPrice))))
                .build();

        SearchResponse<Product> response = client.search(request, Product.class);

        List<Hit<Product>> hits = response.hits().hits();

        List<Product> products = new ArrayList<>();

        for (Hit<Product> hit : hits) {
            Product product = hit.source();
            products.add(product);
        }

        return products;
    }

    private List<Product> getFileContent(MultipartFile file) {
        List<Product> productList = new ArrayList<>();
        try {
            Workbook workbook = WorkbookFactory.create(file.getInputStream());
            for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
                Sheet sheet = workbook.getSheetAt(i);
                for (int y = 0; y <= sheet.getLastRowNum(); y++) {
                    Row row = sheet.getRow(y);
                    Product product = convertRowToProduct(row);
                    productList.add(product);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return productList;
    }

    private Product convertRowToProduct(Row row) {
        Product product = new Product();
        product.setId(row.getCell(0) == null ? "" : row.getCell(0).toString());
        product.setName(row.getCell(1) == null ? "" : row.getCell(1).toString());
        product.setDescription(row.getCell(2) == null ? "" : row.getCell(2).toString());
        product.setPrice(row.getCell(3) == null ? 0 : row.getCell(3).getNumericCellValue());
        product.setCategory(row.getCell(4) == null ? null : row.getCell(2).toString());
        return product;
    }

    private boolean importDataToES(List<Product> productList) {
        //Create Bulk Request
        BulkRequest.Builder builder = new BulkRequest.Builder()
                .waitForActiveShards(asBuilder -> asBuilder.count(1))
                .refresh(Refresh.True);

        List<BulkOperation> bulkOperationList = new ArrayList<>();
        //Add products to bulk request
        for (Product product : productList) {
            bulkOperationList.add(new CreateOperation.Builder<Product>()
                    .index("products")
                    .id(product.getId())
                    .document(product)
                    .build()
                    ._toBulkOperation());
        }
        builder.operations(bulkOperationList);
        BulkResponse response;
        try {
            //Perform bulk request
            response = client.bulk(builder.build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return !response.errors();
    }
}
