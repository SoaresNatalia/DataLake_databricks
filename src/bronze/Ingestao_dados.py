# Databricks notebook source
import delta

# COMMAND ----------

df_original = spark.read.format("parquet").load("/Volumes/bronze/dadosbrutos_raw/original/cliente.parquet") 

#exibindo os dados para verificar as colunas e tipos 
display(df_original.limit(10))

#Salvando os dados em uma tabela Delta no Unity Catalog
#Usando coalesce para reunir os dados distribuidos em uma unica tabela
df_original.coalesce(1).write.mode("overwrite").format("delta").saveAsTable("bronze.tabelas_bronze.cliente")

# COMMAND ----------

df_cdc = spark.read.format("parquet").load("/Volumes/bronze/dadosbrutos_raw/atualizado/cliente_cdc.csv")

df_cdc.createOrReaplaceTempView("cliente")

# COMMAND ----------

# DBTITLE 1,Pegando o Ultimo dado atualizado
#Retorna apenas a linha mais recente por cliente com base na data_modificacao mais recente
query = '''
SELECT * 
FROM cliente
QUALIFY ROW_NUMBER() OVER( PARTITION BY Id_cliente ORDER BY data_modificacao DESC) = 1
'''
#criando um DataFrame a partir do resultado da Query feita
df_atualizado = spark.sql(query)

display(df_atualizado.limit(5))

# COMMAND ----------

tabela_cliente = delta.DeltaTable.forName(spark, "bronze.tabelas_bronze.cliente")

# COMMAND ----------

#Utilizando metodos da Biblioteca Delta para fazer um merge

# Merge Tabela Delta 'clientes' com o Dataframe 'df_atualizado', tipo um join dos dados originais com os dados atualizados

# .whenMatchedDelete -> Quando uma operacao for 'D' (Delete) deleta a linha da tabela Delta
# .whenMatchedUpdateAll-> Quando uma operacao for 'U' (Update) atualiza a linha toda
# .whenNotMatchedInsertAll-> Quando não achar um 'Id_cliente' parecido Insere na tabela. Pode ser que um cliente sido inserido e já atualizado, por isso insere com operação de Update também

   (tabela_cliente.alias("t") 
    .merge(df_atualizado.alias("a"), "t.Id_cliente = a.Id_cliente") 
    .whenMatchedDelete(condition = "a.operacao = 'D'")   
    .whenMatchedUpdateAll(condition = "a.operacao ='U'") 
    .whenNotMatchedInsertAll(condition = "a.operacao = 'I' OR a.operacao = 'U'") 
    .execute() )
