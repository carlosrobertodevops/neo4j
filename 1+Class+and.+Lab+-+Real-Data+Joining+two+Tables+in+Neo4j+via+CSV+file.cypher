 LOAD CSV WITH HEADERS FROM "file:///suppliers.csv" AS row
MERGE(supplier:Supplier {supplierID: row.supplierID,supplierName:row.companyName})   

WITH supplier
LOAD CSV WITH HEADERS FROM "file:///products.csv" AS row
MERGE (product:Product {productID: row.productID, supplierID:row.supplierID, productName:row.productName})  
    ON CREATE SET product.unitsInStock = toInteger(row.unitsInStock),
                  product.unitPrice = toFloat(row.UnitPrice)

WITH supplier,product
MATCH(supplier{supplierID:supplier.supplierID})
MATCH(product{supplierID:product.supplierID })
WHERE supplier.supplierID = product.supplierID
MERGE (supplier)-[:SUPPLIED]->(product)
RETURN *



