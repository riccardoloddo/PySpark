# PySpark — Microprogetto di pulizia e validazione dati dipendenti

Questo repository contiene un **microprogetto in PySpark** per la lettura, la pulizia e la validazione di dataset relativi ai dipendenti.  
L’obiettivo è mostrare come organizzare una semplice pipeline di data processing con Spark, mantenendo log delle operazioni e distinguendo i dati validi da quelli errati.

---

## Funzionalità principali
- Lettura di file CSV di input (es. `Flusso.csv`, `Flusso2.csv`)
- Validazione dei campi principali: **Codice Fiscale, Data di Nascita, Nome, Salario**
- Separazione automatica dei record **OK** e **KO**
- Log delle operazioni in file dedicato
- Unione e gestione di flussi multipli

---

## Struttura del progetto
``` 
PySpark/
├─ src/ # codice sorgente (es. tabella_dipendenti.py)
├─ data/ # CSV di esempio (Flusso.csv, Flusso2.csv)
├─ logs/ # log di esempio (non committare log runtime)
├─ notebooks/ # eventuali Jupyter notebook
├─ tests/ # test (se aggiunti)
├─ docs/ # documentazione aggiuntiva
├─ .gitignore
├─ README.md
└─ requirements.txt
```

---

## Prerequisiti
- **Python 3.8+**
- [Apache Spark](https://spark.apache.org/) con PySpark installato  

Installazione rapida di PySpark:
```bash```
pip install pyspark
