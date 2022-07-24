package com.apache;

import java.io.IOException;

import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main( String[] args ) throws IOException{
        try{ 

            //Cria uma sessao de conexao com o spark
            //Para usar todas as operacoes do spark
            SparkSession spark = SparkSession.builder()
            .appName("Dataframes")
            .master("local[*]")
            .getOrCreate();
            
            //Todas as operacoes vao ser feita quando executar este Main
            GerenciamentoRecursosSpark g1 = new GerenciamentoRecursosSpark(spark);
            g1.todasOperacoes();

            spark.close();

    
            } catch(Exception e){
              System.out.println("ERRO NA APLICACAO!");
              e.printStackTrace();
        }
    }
}
