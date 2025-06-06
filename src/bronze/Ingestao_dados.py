# Databricks notebook source
import delta

# COMMAND ----------

catalog= "bronze"
schema= "tabelas_bronze"

tabelanome = dbutils.widgets.get("tabelanome")
id_tabela = dbutils.widgets.get("id_tabela")

# COMMAND ----------

#Verificando se a tabela ja existe e Contando a quantidade dela, através do Dataframe que retorna as tabelas no Schema/Banco de Dados no Unity Catalog
#Função retorna True ou False, se tiver tabela a contagem será 1 (True), se for 0 (False)
def tabela_existe(catalog, bancodados, tabela):

    contagem = ( spark.sql(f"SHOW TABLES FROM {catalog}.{bancodados}")
                .filter(f" database = '{bancodados}' AND tableName = '{tabela}'")
                .count() )

    return contagem == 1

# COMMAND ----------

if not tabela_existe(catalog, schema, tabelanome):
    df_original = spark.read.format("parquet").load(f"/Volumes/bronze/dadosbrutos_raw/original/{tabelanome}.parquet") 

    #exibindo os dados para verificar as colunas e tipos 
    display(df_original.limit(10))

    #Salvando os dados em uma tabela Delta no Unity Catalog
    #Usando coalesce para reunir os dados distribuidos em uma unica tabela
    (df_original.coalesce(1)
     .write.format("delta")
     .mode("overwrite")
     .saveAsTable(f"{catalog}.{schema}.{tabelanome}") ) 
else:
    print("Tabela já existe, ignorando dados Originais(Full Load)")

# COMMAND ----------

df_cdc = spark.read.format("parquet").load(f"/Volumes/bronze/dadosbrutos_raw/atualizado/{tabelanome}_cdc.csv")

df_cdc.createOrReaplaceTempView(f"view_{tabelanome}")

# COMMAND ----------

# DBTITLE 1,Pegando o Ultimo dado atualizado
#Retorna apenas a linha mais recente por cliente com base na data_modificacao mais recente
query = f'''
SELECT * 
FROM view_{tabelanome}
QUALIFY ROW_NUMBER() OVER( PARTITION BY {id_tabela} ORDER BY data_modificacao DESC) = 1
'''
#criando um DataFrame a partir do resultado da Query feita
df_atualizado = spark.sql(query)

display(df_atualizado.limit(5))

# COMMAND ----------

tabela_cliente = delta.DeltaTable.forName(spark, f"{catalog}.{schema}.{tabelanome}")

# COMMAND ----------

#Utilizando metodos da Biblioteca Delta para fazer um merge

# Merge Tabela Delta 'clientes' com o Dataframe 'df_atualizado', tipo um join dos dados originais com os dados atualizados

# .whenMatchedDelete -> Quando uma operacao for 'D' (Delete) deleta a linha da tabela Delta
# .whenMatchedUpdateAll-> Quando uma operacao for 'U' (Update) atualiza a linha toda
# .whenNotMatchedInsertAll-> Quando não achar um 'Id_cliente' parecido Insere na tabela. Pode ser que um cliente sido inserido e já atualizado, por isso insere com operação de Update também

   (tabela_cliente.alias("t") 
    .merge(df_atualizado.alias("a"), f"t.{id_tabela} = a.{id_tabela}") 
    .whenMatchedDelete(condition = "a.operacao = 'D'")   
    .whenMatchedUpdateAll(condition = "a.operacao ='U'") 
    .whenNotMatchedInsertAll(condition = "a.operacao = 'I' OR a.operacao = 'U'") 
    .execute() )
