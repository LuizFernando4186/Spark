package com.apache;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Log {
 
    

      //metodo para visualizar os datasets
      public static void visualizar(Dataset<Row> collection_events, Dataset<Row> instance_events){ 
        System.out.println("\nCollection Events\n");
        collection_events.show();//verificar as 20 primeiras linhas
        collection_events.printSchema();//Mostrar os tipos que o spark fornece

        System.out.println("\nInstance Events\n");
        instance_events.show();
        instance_events.printSchema();
    
    }


    public static void salvarCSV(String csv, Dataset<Row> a){
        try{
        a.write().format("com.databricks.spark.csv")
        .option("delimiter", ",")
        .option("header", true)//a primeira linha sao o nome dos atributos
        .save("E:/Downloads/"+csv);
        
        } catch(Exception e){
            e.printStackTrace();
        }

    }
}