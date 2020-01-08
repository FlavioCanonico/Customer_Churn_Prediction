# Customer_Churn_Prediction
Customer Churn Prediction with Gradient Bosted Tree on Apache Hadoop cluster

Il progetto mira ad ottenere degli strumenti statistici che permettano di prevedere e studiare il fenomeno dell'abbandono dei clienti (Customer Churn Prediction). Il lavoro è stato svolto per una società che opera principalmente nel settore energetico e delle telecomunicazioni.

I principali **obiettivi** dell'analisi sono:

- La stima della probabilità di churn per singolo cliente
- Lo studio delle cause che determinano un aumento piuttosto che una diminuizione della probabilità di churn

Tra i molteplici **vantaggi** per l'azienda direttamente derivabili dal raggiungimento degli obiettivi sopra elencati ci sono:

- La possibilità di implementare strategie specifiche per la riduzione del tasso di Churn attraverso azioni mirate, individuando i clienti a rischio di abbandono e le cause che determinano il fatto che abbiano un'elevata churn probability
- La possibilità di indirizzare le risorse aziendali verso target specifici di clienti. Si potrebbe decidere ad esempio che delle azioni di caring dispendiose vadano fatte solo per clienti a con una churn probability bassa.

# Strumenti utilizzati

Per l'analisi statistica è stato utilizzata la metodologia di machine learning [Gradient Boosted Trees (GBT)](https://machinelearningmastery.com/gentle-introduction-gradient-boosting-algorithm-machine-learning/)
Si tratta di una metologia all'avanguardia che permette di raggiungere performance molto elevate.
Si tratta di un'analisi svolta in modo completamente integrato con l'infrastruttura Big Data aziendale. Nello specifico si ha a disposizione un cluster [Hadoop](https://hadoop.apache.org/) con i relativi tool (Yarn, Nifi, Hive, Spark etc.), i dati sono su Hadoop Distribuited File System (HDFS) e la parte di analisi dei dati è stata fatta in gran parte utilizzando la libreria [Spark](https://spark.apache.org/) per Rstudio [```SparklyR```](https://spark.rstudio.com/), che ha a disposizione gli algoritmi di machine learning della libreria ```Mllib``` per Spark.

# Dataset

È stata operata una suddivisione del parco clienti in 3 diversi dataset in base alla tipologia contrattuale e sono stati addestrati 3 algoritimi GBT differenti. In questo repository si farà riferimento all'analisi effettuata sui clienti Business. Il dataframe è composto da circa 40mila osservazioni e 256 variabili. Di questi circa il 50% hanno sperimentato il churn. Volendo suddividere in macro categorie le variabili incluse si possono distinguere:

- Variabili socio-anagrafiche legate al referente del contratto: quali l’età, l’area di residenza, il genere e così via.
- Variabili legate alle caratteristiche contrattuali del cliente: quali numero e tipo di utenze da quando è diventato cliente dell'azienda;
-numero e tipo di utenze attive al momento dell’analisi; se nella loro storia hanno cambiato tipologia contrattuale ; tutta una serie di
variabili sulle specifiche tecniche dell’utenza, ad esempio il tipo di linea, la potenza impiegata e così via.
- Variabili legate a fatturazione e pagamenti: quali fattura media, modalità di spedizione della fattura, modalità di pagamento, numero  di mesi in cui ci sono stati ritardi nei pagamenti, numero di blocchi temporanei delle utenze per morosità e così via.
- Numero di casi aperti sull’anagrafica del cliente che riguardano la richiesta o ricezione di Informazioni: possono essere informazioni di carattere tecnico, sui contratti, a proposito di offerte dedicate e così via.
- Numero di casi aperti sull’anagrafica del cliente che riguardano operazioni di Variazione: possono riguardare variazioni dell’offerta, dell’anagrafica, delle caratteristiche tecniche del servizio e così via.
- Numero di casi aperti sull’anagrafica del cliente che riguardano la ricezione di una Campagna: si tratta delle volte in cui il cliente rientra in una campagna specifica e viene contattato. Le campagne possono avere a che fare con tentativi di retention, la necessità di rimodulare la taglia dell’offerta, azioni di cross-selling per far sottoscrivere abbonamenti a prodotti aggiuntivi, comunicazione di nuovi servizi e così via.
- Numero di casi aperti sull’anagrafica del cliente che riguardano l’apertura di un Reclamo: si può trattare di reclami per guasti tecnici, per incongruenze di fatturazione, ritardi di attivazione e così via.
- Numero di casi aperti sull’anagrafica del cliente che riguardano un invio di documentazione: si tratta di quei contatti con il cliente quando avviene una richiesta esplicita per l’invio del contratto, della fattura, di modulistica e così via. Può anche riguardare l’invio di documentazione inerente al credito.
- Infine le variabili di risposta sono due, una indica se l'osservazione ha sperimentato l'evento terminale (churn), mentre l'altra indica il numero di mesi trascorsi dalla stipula del contratto.
Per ragioni di rispetto della privacy non è possibile caricare i dati su questo repository.

Una parte del pre-processing dei dati è stata fatta direttamente con i tool del cluster Hadoop, mentre la gran parte dell'analisi è stata portata avanti in RStudio. Il codice completo è consultabile e scaricabile nei file allegati al presente repository Github, in questo file di introduzione invece ci si soffermerà solo su alcuni punti del codice. Va sottolineato che in alcuni punti degli script R (in particolare per quel che riguarda la parte di data pre-processing) il codice è poco conciso a causa dell'impossibilità dell'utilizzo di gran parte delle funzioni R sulle Spark Table. Sono utilizzabili infatti solo con alcune delle funzioni del [```tydiverse```](https://www.tidyverse.org/) e in particolar modo di ```dplyr``` per la manipolazione dei dati.

In questa breve presentazione verrà commentato solo il codice inserito nel file wrapper.R, mentre per il codice completo richiamato nel wrapper si rimanda al codice completo caricato nel presente repository.
Per prima cosa vengono caricate le librerie R sono stati configurati i paramentri della connessione al cluster da ambiente R. Chiaramente i paramentri vanno impostati in base alle risorse computazionali a disposizione sul cluster, sulla base della potenza necessaria al task da svolgere, alla necessità o meno di lavorare in più persone contemporaneamente sul cluster, e così via.

```
#carico le librerie
source(paste(home,"libreries.R",sep=''))

#Richiamo la function importDatiFromSql (non inserita per intero perché contiene dati di accesso riservati)
source("/home/gennaronew/Library/importDatiFromSQL.R")

# Creo la Spark Connection e setto i parametri
java_home_path='/opt/java'
yarn_conf_dir='/opt/conf.cloudera.yarn/'
hadoop_conf_dir='/opt/conf.cloudera.yarn/'
hive_home='/opt/conf.cloudera.yarn/'
hadoop_user_name='your_user'

yarn_archive='hdfs://path/to/yarnarchive.zip'
spark_home="/opt/spark"

Sys.setenv(JAVA_HOME = java_home_path)
Sys.setenv(YARN_CONF_DIR = yarn_conf_dir)
Sys.setenv(HADOOP_CONF_DIR = hadoop_conf_dir)
Sys.setenv(HIVE_HOME = hive_home)
Sys.setenv(HADOOP_USER_NAME = hadoop_user_name)
cfg = spark_config()
cfg$sparklyr.yarn.cluster.start.timeout <- 200
cfg$spark.executor.cores <- 5 
cfg$spark.executor.memory = "5g"
cfg$spark.executor.instances <- 14
cfg$spark.yarn.archive=yarn_archive
#nuove aggiunte:
cfg$spark.driver.memory = cfg$spark.executor.memory
cfg$spark.driver.cores = cfg$spark.executor.cores

Sys.time()
sc = spark_connect(master = "master-name",spark_home=spark_home, config = cfg)
Sys.time()
```

Una volta stabilita la connessione, vengono generati i dati aggiornati e richiamati nel formato parquet in cui vengono salvati nello script data.R:

```
source(paste(home,"data.R",sep=''))

pib_pq = spark_read_parquet(sc, "pib", "/user/flavio/pib",overwrite = TRUE)
```

Nel corso del data pre-processing vengono attuate tutta una serie di operazioni di creazione di nuove variabili di sintesi, normalizzazioni, standardizzazioni delle variabili numeriche e imputazione dei dati mancanti.


# GBT training

Per l'addestramento dell'algoritmo sono state selezionate 55 variabili indipendenti. Dal dataset è stato sottratto il 30% delle osservazioni in modo casuale per la consueta fase di test. L'addestramento del modello con Spark è molto prestante, richiedendo soltanto 30 secondi circa. L'accuracy ottenuta utilizzando il metodo del calcolo dell'area sottostante la curva di ROC (ROC-AUC index) restituisce un'accuracy molto elevata, superiore al 90%. Se invece si guarda all'indice di concordanza, ottenuto impostando il threshold per il churn quando la probabilità di churn supera il 50%, e verificando la percentuale di falsi positivi o falsi negativi sul totale, si ottiene un'accuracy dell'85%. Chiaramente il valore è più basso ma comunque più che soddisfacente in termini assoluti.

Si è proceduto poi alla costruzione di una heatmap, per studiare le relazioni tra le variabili indipendenti e la probabilità di abbandono. Nello specifico si sono raggruppati i clienti in quattro classi sulla base della loro stima di chrn probability (0-25% = green, 25-50% = yellow, 50-75% = orange , 75-100% = red) e si è costruita così l'asse delle ascisse. Sull'asse delle ordinate sono state poste le variabili indipendenti in ordine di Variable Importance dal basso verso l'alto.

![heatmap](https://github.com/FlavioCanonico/Customer_Churn_Prediction/blob/master/heatmap.png)

Un colore rosso di intensità maggiore all’interno dei rettangoli indica che i clienti con quella stima di churn probability hanno un valore molto superiore rispetto alla media per quella variabile a cui il rettangolo corrisponde. Un colore vicino al bianco indica che questi valori sono in media. Infine un colore blu di intensità maggiore indica che i valori della variabile sono sotto media per la variabile in questione data una certa stima di churn probability. Ad esempio, guardando al primo rettangolo in basso a destra, si può dire che quei clienti per i quali l'algoritmo stima una probabilità di churn elevata sono i clienti che hanno sottoscritto un contratto da meno tempo rispetto alla media del parco analizzato.
Questo chiaramente vale per le variabili numeriche. Per le variabili categoriali viene riportata una riga per ogni categoria della variabile stessa. L’interpretazione tuttavia è simile. Nel caso della variabile che registra il tipo di cliente le categorie possibili sono tre: cliente Energia e/o Gas (ENG), cliente Voce, Adsl e/o Mobile (TLC) o cliente misto (ENG/TLC). In questo caso, ad esempio, il colore tendente al rosso per la riga all’intreccio tra la categoria ENG e la stima di churn denominata "red" (75-100%), indica che i clienti maggiormente a rischio di abbandono sono maggiormente quelli con solo prodotto energia.

# Predizioni

Il modello viene richiamato nel wrapper:

```
gbt_pib <- ml_load(sc, path = "/user/flavio/models/gbt_pib")
```

E il modello viene utilizzato per fare predizioni sull'intero parco clienti, anche quelli non utilizzati per l'addestramento per varie ragioni e quelli neo-acquisiti:

```
pred_pib_all <- ml_predict(gbt_pib,pib_pq)
```

Vengono aggiunti poi gli idcliente tramite una semplice operazione di join con una tabella importata dai DB aziendali in Rstudio e viene scritta la tabella dei risultati in formato parquet sull'HDFS (Hadoop Distribuited File System):

```
dim_clt = importDatiFromSql(sqlText = "select IdCliente, cluster from [mi-dataw].[it_dwh].[dbo].[dim_crm_clienti]")
pred_all = pred_all %>% inner_join(dim_clt, by=c("cliente" = "IdCliente"), copy=T)
spark_write_parquet(pred_all, "/user/flavio/predicted_business", mode="overwrite")
```

Infine si valuta il tempo impiegato per l'esecuzione dell'intero processo per valutare le prestazioni e per comprenderne le modalità di inserimento nei processi in produzione sui sistemi aziendali.

```
end_time <- Sys.time()
tempo_impiegato = end_time - start_time
print(paste("start time:",start_time,'-',"end time:",end_time))
print(tempo_impiegato)
```

A questo punto i sistemi del CRM aziendale leggono i dati dall'HDFS e viene visualizzata in anagrafica del cliente la churn probability:

![NBA](https://github.com/FlavioCanonico/Customer_Churn_Prediction/blob/master/NBA.png)

Il dato inoltre contribuisce ad alimentare i criteri di creazione delle Next Best Action attuabili sul cliente in seguito a una chiamata inbound, create nell'ambito del progetto ritrovabile del [progetto per la stima del lifetime e del lifetime value](https://github.com/FlavioCanonico/customer_lt_ltv/blob/master/README.md) dei singoli clienti.

Il dato viene aggiornato ogni giorno e la stima viene ricalcolata, per avere stime aggiornate con cadenza giornaliera e poter attuare strategie specifiche sui clienti in modo tempestivo.

# Conclusioni

Lo studio del fenomeno dell'abbandono dei clienti mediante strumenti statistici permette alle aziende di avere grossi vantaggi. In questo lavoro si sono mostrate solo alcune delle applicazioni possibili grazie a un calcolo accurato della stima della churn probability e dello studio delle sue cause. Altri aspetti del fenomeno dell'abbandono dei clienti [sono stati studiati](https://github.com/FlavioCanonico/customer_lt_ltv/blob/master/README.md) e approfonditi con altre tecniche di analisi e poi integrati in un più generico macro-progetto. Si tratta di strumenti molto flessibili. Il concetto di Next Best Action ad esmepio può essere utilizzato per molteplici scopi e in diversi campi di business, dal campo della customer base management fino al marketing, alle vendite e così via.
