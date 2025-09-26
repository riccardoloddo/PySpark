#!/usr/bin/env python
# coding: utf-8

# In[1]:


import csv
from datetime import datetime
import os
import re
import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType
from pyspark.sql.functions import current_date, col, to_date, when, current_timestamp, monotonically_increasing_id, lit, upper


# In[2]:


class TabellaDipendenti:
    
    logging.basicConfig(
        filename="tlog.txt",
        level=logging.INFO,
        format="%(message)s"   # niente asctime qui
    )
    
    # Costruttore
    def __init__(self, path_csv, idrun):
        self.spark = SparkSession.builder.master("local[1]").appName("FlussoDipendenti").getOrCreate()
        schema = StructType([
            StructField("CF", StringType(), True),
            StructField("NOME", StringType(), True),
            StructField("DN", StringType(), True),
            StructField("SALARIO", StringType(), True)
        ])
        self.idrun = idrun
        self.df = self.spark.read.csv(path_csv, header=True, schema=schema)
        self.df = self.df.withColumn("DINS", current_date())  
        
        logging.info("IDRUN=%s, Operazione=Costruttore, Stato=OK, File=%s, Data=%s", 
             idrun, path_csv, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # Funzione che mostra la "tabella". 
    def show(self):
        if self.df:
            self.df.show(truncate=False)
            logging.info("IDRUN=%s, Operazione=Show, Stato=OK, Righe=%s, Data=%s", 
             self.idrun, self.df.count(), datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        else:
            print("DataFrame vuoto.")
            logging.info("IDRUN=%s, Operazione=Show, Stato=VUOTO, Data=%s", 
             self.idrun, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def GivemeDataFrame(self):
        logging.info("IDRUN=%s, Operazione=GivemeDataFrame, Stato=OK, Data=%s", 
             self.idrun, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return self.df
    
    def pulisci(self):
        """
        Filtra i dati: ritorna due oggetti TabellaDipendenti,
        uno con i dati validi (OK) e uno con i dati scartati (KO).
        La tabella OK ha colonne convertite, la tabella scarti rimane stringa.
        """
        df = self.df
        # 1. SALARIO numerico e positivo
        df_valid_salario = df.withColumn(
            "valid_salario",
            when(col("SALARIO").rlike("^[0-9]+(\.[0-9]+)?$") & (col("SALARIO").cast("double") > 0), True).otherwise(False)
        )
        logging.info("IDRUN=%s, Operazione=pulisci, Filtro=SALARIO, Righe=%s, Data=%s",
                 self.idrun, df_valid_salario.filter(col("valid_salario")==True).count(),
                 datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        # 2. DN valido formato YYYY-MM-DD e giorno/mese coerenti
        df_valid_date = df_valid_salario.withColumn(
            "valid_dn",
            when(
                (col("DN").rlike(r'^\d{4}-\d{2}-\d{2}$')) &
                (col("DN").substr(6,2).cast("int").between(1,12)) &
                (col("DN").substr(9,2).cast("int").between(1,31)),
                True
            ).otherwise(False)
        )
        logging.info("IDRUN=%s, Operazione=pulisci, Filtro=DN, Righe=%s, Data=%s",
                 self.idrun, df_valid_date.filter(col("valid_dn")==True).count(),
                 datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        # 3. NOME senza numeri e non nullo
        df_valid_name = df_valid_date.withColumn(
            "valid_nome",
            when(col("NOME").isNotNull() & (~col("NOME").rlike(r"[0-9]")), True).otherwise(False)
        )
        logging.info("IDRUN=%s, Operazione=pulisci, Filtro=NOME, Righe=%s, Data=%s",
                 self.idrun, df_valid_name.filter(col("valid_nome")==True).count(),
                 datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        # 4. CF lungo 16 e non nullo
        df_valid_cf = df_valid_name.withColumn(
            "valid_cf",
            when(col("CF").isNotNull() & col("CF").rlike("^.{16}$"), True).otherwise(False)
        )
        logging.info("IDRUN=%s, Operazione=pulisci, Filtro=CF, Righe=%s, Data=%s",
                 self.idrun, df_valid_cf.filter(col("valid_cf")==True).count(),
                 datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        # 5. Colonna "valid" finale
        df_valid_cf = df_valid_cf.withColumn(
            "valid",
            col("valid_salario") & col("valid_dn") & col("valid_nome") & col("valid_cf")
        )
        logging.info("IDRUN=%s, Operazione=pulisci, Totale righe OK=%s, Totale righe KO=%s, Data=%s",
                 self.idrun, df_valid_cf.filter(col("valid")==True).count(),
                 df_valid_cf.filter(col("valid")==False).count(),
                 datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        # 6. DF OK → righe valide
        df_ok = df_valid_cf.filter(col("valid") == True)             .withColumn("IDRUN", lit(self.idrun))             .withColumn("SALARIO", col("SALARIO").cast("double"))             .withColumn("DN", to_date("DN", "yyyy-MM-dd"))             .withColumn("DINS", current_date())             .drop("valid_salario", "valid_dn", "valid_nome", "valid_cf", "valid")             .select("IDRUN", "CF", "NOME", "DN", "SALARIO", "DINS")

        # 7. DF scarti → righe non valide, tutte stringhe, nessuna conversione
        df_scarti = df_valid_cf.filter(col("valid") == False)             .withColumn("IDRUN", lit(self.idrun))             .withColumn("DINS", current_date())             .drop("valid_salario", "valid_dn", "valid_nome", "valid_cf", "valid")             .select("IDRUN", "CF", "NOME", "DN", "SALARIO", "DINS")
        
        logging.info("IDRUN=%s, Operazione=pulisci, DF_OK righe=%s, DF_SCARTI righe=%s, Data=%s",
                 self.idrun, df_ok.count(), df_scarti.count(),
                 datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        # 8. Creo nuovi oggetti TabellaDipendenti senza leggere CSV
        tabella_ok = TabellaDipendenti.__new__(TabellaDipendenti)
        tabella_ok.spark = self.spark
        tabella_ok.df = df_ok
        tabella_ok.idrun = self.idrun

        tabella_scarti = TabellaDipendenti.__new__(TabellaDipendenti)
        tabella_scarti.spark = self.spark
        tabella_scarti.df = df_scarti
        tabella_scarti.idrun = self.idrun

        return tabella_ok, tabella_scarti


# In[9]:


tabella1 = TabellaDipendenti("Flusso.csv", 1)
tabella1.show()


# In[10]:


tabella2 = TabellaDipendenti("Flusso2.csv", 2)
tabella2.show()


# In[12]:


tabella1_ok, tabella1_scarti = tabella1.pulisci()
tabella2_ok, tabella2_scarti = tabella2.pulisci()


# In[13]:


print("=== Tabella1 OK ===")
tabella1_ok.show()

print("=== Tabella1 Scarti ===")
tabella1_scarti.show()

print("=== Tabella1 OK ===")
tabella2_ok.show()

print("=== Tabella1 Scarti ===")
tabella2_scarti.show()


# In[14]:


tab1 = tabella1_ok.GivemeDataFrame()
df_filtrato = tab1.select("IDRUN","CF", "NOME", "SALARIO").where(col("SALARIO") > 3000)
df_filtrato.show()


# In[15]:


df_ok_totale = tabella1_ok.GivemeDataFrame().unionByName(tabella2_ok.GivemeDataFrame())
df_scarti_totale = tabella1_scarti.GivemeDataFrame().unionByName(tabella2_scarti.GivemeDataFrame())


# In[16]:


df_ok_totale.show()


# In[ ]:




