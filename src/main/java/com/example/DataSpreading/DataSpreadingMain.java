package com.example.DataSpreading;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DataSpreadingMain {

	public static void main(String[] args) {
		
		SpringApplication.run(DataSpreadingMain.class, args);
		
		// Context and Data
        SparkSession spark = SparkSession.builder().appName("Workshop").master("local[*]").getOrCreate();
        Dataset<Row> df = spark.read().option("header", "true").csv("Data/test.csv");
        SQLContext sqlContext= new SQLContext(spark);
        sqlContext.registerDataFrameAsTable(df, "sales");
        
        
        // Prepare inputs ( Table, list of columns to group by, list of cols to duplicate, number of duplicates, list of cols to aggregate with the aggregation method)
        String table="sales";
        
        String[] colsToGroupBy= {"Client_ID","Product_Code"};

        String[] colsToDuplicate= {"FIRST_NAME","LAST_NAME"};
        int nbrOfDuplicates=3;

        HashMap<String, String> colsToAgg = new HashMap<String, String>();
        colsToAgg.put("max", "Age");
        colsToAgg.put("sum", "Price");
        
        
        //Prepare query and select 
        //Will run in two steps: 
        //// Group by concatenate the columns to duplicate and aggregate the columns to aggregate
        //// Split concatenated columns into new columns
        
        String query="Select ";
        String groupBy=" from "+ table +" Group by ";
        
        ArrayList<String> selectExpr = new ArrayList<String>();
        
        //Add the columns to group by to the query and to the select expression
        for (int i=0; i<colsToGroupBy.length; i++) {
            query=query+colsToGroupBy[i]+", ";
            groupBy=groupBy+ colsToGroupBy[i]+ ", ";
            selectExpr.add(colsToGroupBy[i]);
        }
        
        // Add columns to duplicate with the concatenation functions to the query and to the select expression with the split function
        for (int j=0; j<colsToDuplicate.length; j++) {
        	query=query+"concat_ws( '@', collect_list(" + colsToDuplicate[j]+ ")) as " + colsToDuplicate[j] +", ";
        	
        	//Add as much split as number of duplications
        	for (int r=0; r<nbrOfDuplicates; r++) {
            	selectExpr.add("split("+colsToDuplicate[j]+", '@')["+r+"] as "+ colsToDuplicate[j]+"_"+r);
        	}
        	
        }
        
        // Add columns to aggregate with the aggregation method to the query and to the select expression
        Iterator iterator = colsToAgg.entrySet().iterator();
        while(iterator.hasNext()) {
            Map.Entry mentry = (Map.Entry)iterator.next();
            
            //Add to query with aggregation method
            query=query+mentry.getKey()+"("+mentry.getValue()+") as "+ mentry.getValue() +", ";
        	
            // Add column to select expression
            selectExpr.add(mentry.getValue().toString());
         }
        
        // Finalize the query by removing the two last characters (when we were preparing the query we always close by ", " so we need to remove the last one)
        query=query.substring(0, query.length() - 2)+ groupBy.substring(0, groupBy.length() - 2);
        
        
        //Run the query then the split
        Dataset<Row> frame = sqlContext.sql(query).selectExpr(selectExpr.toArray(new String[0]));
        
        frame.show();
        
     
 	}
}
