# Data-spreading
Spring boot Spark app to transpose rows to columns based on other key columns

In this example we have a store that gives the ability to more than one client to be registered into the same client ID.

Using the data spreading, the store will be able to check how much all the clients under the same client ID spend for a specific product.

The results will give the store owner for each (clientID, Product) all the names of the customers and the sum of the money they all spent.

The data-spreading so is a transformation that duplicate data from rows into columns and in the same time aggregate some other data to make all the data appear in one row.


Test data are on the folder "Data" and the app is a simple main class.

