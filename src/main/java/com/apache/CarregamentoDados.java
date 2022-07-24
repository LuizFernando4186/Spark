package com.apache;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

//NESTA CLASSE PRECISA ALTERAR O CAMINHO DOS DATASETS

public class CarregamentoDados {


   Dataset<Row> collection_events;
   Dataset<Row> instance_events;



    public void carregamentoDataframesEmDataset(SparkSession spark) throws IOException{ 
        
        Logger.getLogger("org.apache").setLevel(Level.WARN);//Filtra informacoes mostradas
    
        try{ 

          System.out.println("\nCARREGANDO OS DATASETS ...\n");


        //Carrega o arquivo csv do collection_events
        collection_events = spark.read()
        .option("header", true)//a primeira linha sao o nome dos atributos
        .option("inferSchema",true)//Deixo o spark definir o tipo
        .csv("E:/EP_SD_DADOS/google-traces/collection_events/*.csv");


        //Carrega o arquivo csv do instance_events
        instance_events = spark.read()
        .option("header", true)//a primeira linha sao o nome dos atributos
        .option("inferSchema",true)//Deixo o spark definir o tipo
        .csv("E:/EP_SD_DADOS/google-traces/instance_events/*.csv");
        
        //Em algum momento nao e possivel encontrar as coluna de cpus e memoria, por isso foi renomeado
        instance_events = instance_events.withColumnRenamed("resource_request.cpus", "cpus")
        .withColumnRenamed("resource_request.memory", "memory");
        


    } catch (IllegalStateException erroEstado) {
        System.err.println("Erro ao ler o arquivo. Finalizando.");
    }
    

  }

  



}