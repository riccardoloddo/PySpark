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

## Istruzioni per l'esecuzione

1. Copia i file CSV di input nella cartella `data/`.  
   Esempio:
   ```
   data/Flusso.csv
   data/Flusso2.csv
   ```

2. Installa le dipendenze del progetto:
```bash
pip install -r requirements.txt
```

3. Esegui gli script Python dalla cartella `src/`.  
   Esempio:
```bash
python src/tabella_dipendenti.py
```

4. I log generati durante l'esecuzione saranno salvati in `logs/`.


## Note

- Assicurati che tutti i CSV siano nella cartella `data/` prima di eseguire gli script.  
- Gli script sono modulari: puoi aggiungere nuovi flussi o validazioni modificando i file in `src/`.  
- I record validi e non validi vengono separati automaticamente secondo le regole definite negli script.

