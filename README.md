# Customer_Churn_Prediction
Customer Churn Prediction with XGBoost with Apache Spark and SparklyR

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

Una parte del pre-processing dei dati è stata fatta direttamente con i tool del cluster Hadoop, mentre la gran parte dell'analisi è stata portata avanti in RStudio. Il codice completo è consultabile e scaricabile nei file allegati al presente repository Github, in questo file di introduzione invece ci si soffermerà solo su alcuni punti del codice. Va sottolineato che in alcuni punti degli script R il codice è poco conciso a causa dell'impossibilità dell'utilizzo di gran parte delle funzioni R sulle Spark Table. Sono utilizzabili infatti solo con alcune delle funzioni del [```tydiverse```](https://www.tidyverse.org/) e in particolar modo di ```dplyr``` per la manipolazione dei dati.




