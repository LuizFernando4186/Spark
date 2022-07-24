package com.apache;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;


/* 
EM ALGUNS METODOS FOI USADO O METODO Log.salvarCSV() 
DA CLASSE Log.java PARA PLOTAR OS GRAFICOS E EXTRAIR AS INFORMACOES 
*/


public class GerenciamentoRecursosSpark extends CarregamentoDados{

    SparkSession spark;
    public static final int HORASUMASEMANA = 168;
    

    //construtor passando o SparkSession
    public GerenciamentoRecursosSpark(SparkSession spark){
        this.spark = spark;
    }



    //Chama todas operacoes disponivel no enunciado
    public void todasOperacoes() throws IOException{
    carregamentoDataframesEmDataset(spark);
    Log.visualizar(collection_events,instance_events);
    jobsPorHora();
    tarefasPorHora();
    categoriaPorPrioridades();
    tempoInicioJob();
    requisicaoRecursos();
   }



 

    //Como e a requisição de recursos computacionais (memoria e CPU) do cluster durante o tempo? 
    //Por hora, qual e a media, mediana, desvio padrao, quartis dos recursos de memoria e CPU do cluster.
    public void requisicaoRecursos() throws IOException{

        System.out.println("\n--------- REQUISICOES DE RECURSOS ----------\n");


         //Ordeno a tabela de acordo com o tempo
         Dataset<Row> ordenado =  instance_events.select("time", "cpus", "memory").sort("time");
         //Log.salvarCSV("timeOrdenados", ordenado);//custoso gravar no csv

         int limiteInferior = 0;
         int limiteSuperior = 1;


         
         for(int i= 0; i < HORASUMASEMANA; i++){

         //Convertendo microssegundos em hora
         String condicao = "(time/3600000000) >=" + limiteInferior + " and (time/3600000000) < " + limiteSuperior;
         limiteInferior = limiteSuperior;
         limiteSuperior = limiteSuperior+1;
         Dataset<Row> filtrado = ordenado.where(condicao);

         filtrado.persist();//colocar em cache

         System.out.println("\nINTERVALO DE " + (limiteInferior-1) +" A " + (limiteSuperior-1) + " HORAS");
         filtrado.agg(functions.avg("cpus").as("mediaCpus"), functions.avg("memory").as("mediaMemory")).show(false);
         filtrado.agg(functions.stddev("cpus").as("desvioPadraoCpus"), functions.stddev("memory").as("desvioPadraoMemory")).show(false);//desvio padrao
         filtrado.agg(functions.sum("cpus").as("somaCpus"), functions.sum("memory").as("somaMemory")).show(false);
         
         filtrado.unpersist();//tirar do cache
         }



         //Amplitude total
         //Esta usando o dataset ordenado porque vai ser aplicado pelo conjunto todo as operacoes
         ordenado.agg(functions.max("cpus").as("maxCpus"),functions.max("memory").as("maxMemory")).show(false);
         ordenado.agg(functions.min("cpus").as("minCpus"),functions.min("memory").as("minCpus")).show(false);
         ordenado.agg(functions.corr("cpus","memory").as("correlacao")).show(false);//correlacao de pearson
        
     }




    //As diversas categorias de jobs possuem características diferentes (requisicao de recursos computacionais, frequencia de submissao, etc.)? 
    //As categorias sao separadas pela prioridade.
    public void categoriaPorPrioridades() throws IOException{

        System.out.println("\n--------- SEPARANDO POR PRIORIDADES ----------\n");


        
        int limiteInferior = 0;
        int [] limiteSuperior = {99,115,119,359,360};//Todos os limites superiores disponivel no enunciado para criar as categorias
        String condicao = " ";


        for(int i = 0; i < limiteSuperior.length; i++){

        System.out.println("\nPROCESSANDO: " + limiteInferior +" A " + limiteSuperior[i]+ "\n");

        if(i == 4){ //Chegou na ultima condicao e so precisa ser maior 
        condicao = "priority >= " + limiteSuperior[i];

        } else {
        condicao = "priority >=" + limiteInferior + " and priority <= " + limiteSuperior[i];

        }


        //de acordo com a condicao de prioridade e possivel separar e selecionando apenas algumas colunas
        Dataset<Row> dataset = instance_events.select("time","priority", "cpus", "memory").where(condicao).sort("time");


        dataset.show(false);

        dataset.agg(functions.avg("cpus").as("mediaCpus"+" ate "+limiteSuperior[i]), functions.avg("memory").as("mediaMemory"+" ate "+limiteSuperior[i])).show(false);//calcula a media
        dataset.agg(functions.stddev("cpus").as("desvioPadraoCpus"+" ate "+limiteSuperior[i]), functions.stddev("memory").as("desvioPadraoMemory"+" ate "+limiteSuperior[i])).show(false);//desvio padrao
        

        //Log.salvarCSV("prioridade_" + limiteInferior +"_A_" + limiteSuperior[i], dataset);

        limiteInferior = limiteSuperior[i]+1;

    }

  }





    //Quantos jobs são submetidos por hora? 
    //Os Jobs sao cada colecao, então como e uma semana, analisa por hora.
    public void jobsPorHora() throws IOException{

        System.out.println("\n--------- JOBS POR HORA ----------\n");


        int limiteInferior = 0;
        int limiteSuperior = 1;

    
       for(int i= 0; i < HORASUMASEMANA; i++){

       //Convertendo microssegundos em hora
       String condicao = "(time/3600000000) >=" + limiteInferior + " and (time/3600000000) < " + limiteSuperior;
       limiteInferior = limiteSuperior;
       limiteSuperior = limiteSuperior+1;

       //Filtrando usando o tempo em horas
       Dataset<Row> filtrado = collection_events.select("time").where(condicao);

       long nLinhas = filtrado.count();
       System.out.println((i+1) + " HORAS COM " + nLinhas + " JOBS");
      }

    }





    //Quantas tarefas sao submetidas por hora? 
    //Tarefas sao operações dentro da colecao, ou seja, vai ser usado as instancias.
    public void tarefasPorHora() throws IOException{

        System.out.println("\n--------- TAREFAS POR HORA ----------\n");

        int limiteInferior = 0;
        int limiteSuperior = 1;

        for(int j= 0; j < HORASUMASEMANA; j++){

        //Convertendo microssegundos em hora
        String condicao = "(time/3600000000) >=" + limiteInferior + " and (time/3600000000) < " + limiteSuperior;
        limiteInferior = limiteSuperior;
        limiteSuperior = limiteSuperior+1;
        //Filtrando usando o tempo em horas
        Dataset<Row> filtrado = instance_events.where(condicao);

        long nLinhas = filtrado.count();
        System.out.println((j+1) + " HORA COM " + nLinhas + " TAREFAS");

        }

    }







    //Quanto tempo demora para a primeira tarefa de um job começar a ser executada? 
    //Seria o tipo 0 ate o tipo 3, soma os tempos.
    public void tempoInicioJob() throws IOException{

        System.out.println("\n--------- INICIO DA EXECUCAO DOS JOBS ----------\n");

    
        //Filtrar porque nao sera usado algumas colunas no join, para nao ser custoso
        Dataset<Row> nova_collection_events = collection_events.drop("priority");
        Dataset<Row> nova_instance_events = instance_events.drop("cpus").drop("memory").drop("priority");


        //Precisa renomear porque mostra um erro de ambiguidade
        nova_collection_events = nova_collection_events
       .withColumnRenamed("type", "type_collection")
       .withColumnRenamed("time", "time_collection")
       .withColumnRenamed("collection_id", "id");

       //Crio uma versao que pode ser vista no spark.sql
       nova_collection_events.createOrReplaceTempView("nova_collection_events");
       nova_instance_events.createOrReplaceTempView("nova_instance_events");

       Dataset<Row> joinDatasets = spark.sql("SELECT * FROM nova_collection_events INNER JOIN nova_instance_events ON nova_instance_events.collection_id = nova_collection_events.id;");


       //filtro para fazer as operacoes no indice e time
       Dataset<Row> filtroTipo0 = joinDatasets.where("type == 0");
       Dataset<Row> filtroTipo3 = joinDatasets.where("type == 3");


       //Filtrar ate o tipo 3 que e o CRONOGRAMA seria o comeco da execucao
       filtroTipo0 = filtroTipo0.groupBy("collection_id").agg(functions.min("instance_index").as("instance_index"), functions.min("time").as("timeTipo0")); 
       filtroTipo3 = filtroTipo3.groupBy("collection_id").agg(functions.min("instance_index").as("instance_index_2"), functions.min("time").as("timeTipo3")); 

       filtroTipo3 = filtroTipo3.withColumnRenamed("collection_id", "id");

       filtroTipo0.createOrReplaceTempView("filtroTipo0");
       filtroTipo3.createOrReplaceTempView("filtroTipo3");

       //Por fim sera feito um novo join com as informacoes necessarias
       Dataset<Row> joinFinal = spark.sql("SELECT * FROM filtroTipo0 INNER JOIN filtroTipo3 ON filtroTipo0.collection_id = filtroTipo3.id;");

       joinFinal.createOrReplaceTempView("joinFinal");

       //Usando comandos sql para extrair o tempo do tipo 3 - tipo 0 que e o tempo exato que demorou para executar no momento que chegou
       Dataset<Row> joinF = spark.sql("select collection_id, (timeTipo3 - timeTipo0) as tempo from joinFinal;");

       //Log.salvarCSV("join", joinF);

       joinF.agg(functions.avg("tempo").as("mediaTempo"), functions.stddev("tempo").as("desvioPadraoTempo")).show();

       joinF.show(false);
     

    }


}