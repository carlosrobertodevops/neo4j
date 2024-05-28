
LOAD CSV WITH HEADERS FROM "file:///orders.csv" AS row
MERGE(order:Order{orderID:row.orderID, customerID:row.customerID, employeeID:row.employeeID, shipperID:row.shipVia})

WITH order
LOAD CSV WITH HEADERS FROM "file:///shippers.csv" AS row
MERGE(shipper:Shipper{shipperName:row.companyName, shipperID:row.shipperID})

WITH order, shipper
LOAD CSV WITH HEADERS FROM "file:///customers.csv" AS row
MERGE(customer:Customer{customerID:row.customerID,customerName:row.companyName})

WITH order, shipper, customer
LOAD CSV WITH HEADERS FROM "file:///employees.csv" AS row
MERGE(employee:Employee{employeeID:row.employeeID, employeeName:row.firstName})

WITH order, shipper, customer, employee
MATCH(order{orderID:order.orderID})
MATCH(shipper{shipperID:shipper.shipperID})
MATCH(customer{customerID:customer.customerID})
MATCH(employee{employeeID:employee.employeeID})
WHERE order.shipperID = shipper.shipperID AND order.customerID = customer.customerID AND employee.employeeID = order.employeeID
MERGE (shipper)-[:DELIVERED]->(order)<-[:PLACED]-(customer)
MERGE (employee)-[:FULFILLED]->(order)
RETURN *



