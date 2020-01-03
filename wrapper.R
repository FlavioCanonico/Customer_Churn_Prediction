spark_disconnect(sc)

rm(list=ls())

start_time = Sys.time()

home = '/home/flavio/dev/BigData/churn_survival/'

#carico le librerie
source(paste(home,"librerie.R",sep=''))

#Richiamo la function importDatiFromSql (non inserita per intero perché contiene dati di accesso riservati)
source("/home/gennaronew/Library/importDatiFromSQL.R")

# Creo la Spark Connection e setto i parametri
java_home_path='/opt/java'
yarn_conf_dir='/opt/conf.cloudera.yarn/'
hadoop_conf_dir='/opt/conf.cloudera.yarn/'
hive_home='/opt/conf.cloudera.yarn/'
hadoop_user_name='flavio'

yarn_archive='hdfs://nameservice1/globals/spark-220-libs.zip'
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
sc = spark_connect(master = "yarn-cluster",spark_home=spark_home, config = cfg)
Sys.time()

###########################################################################################################
###### 
detach("package:plyr", unload=TRUE) #può creare conflitti con dplyr

# Generazione dei dati
Sys.time()
source(paste(home,"Dati.R",sep=''))
Sys.time()

# Carico i dati
pib_pq = spark_read_parquet(sc, "pib", "/user/flavio/pib",overwrite = TRUE)

# Carico il modello
gbt_pib <- ml_load(sc, path = "/user/flavio/models/gbt_pib")

# Predizioni
pred_pib_all <- ml_predict(gbt_pib,pib_pq)

pred_all<- pred_pib_all %>% sdf_separate_column("probability", c("p0","p1")) %>%
  select(cliente,clienteattivo
         , mesicliente,n_utenze
         ,flag_sciolto,tipocliente
         ,fattmedia_imp,areanielsencliente_imp
         ,etareferente_imp,
         primamodalita,ultimamodalita
         , canalidigitali ,mobile
         ,ultimocanale,campagna_autolettura
         ,campagna_credito,campagna_crossselling
         ,campagna_fattura,
         campagna_promo_offerte,campagna_recall
         ,campagna_rimodulazione,campagna_survey
         ,campagna_spedizione,campagna_tecnico
         ,info_attivazione,info_canali_digitali
         ,info_contorelax,info_contratto
         ,info_credito,info_disattivazione
         ,info_fattura,info_modulistica
         ,info_pagamento,info_promo_offerte
         ,info_recapito_consulente,info_rimodulazione
         ,info_sconto_rimborso,info_spedizione
         ,info_tecnico,info_voltura
         ,invio_credito, invio_contratto
         ,invio_fattura,invio_modulistica
         ,reclamo_attivazione,reclamo_credito
         ,reclamo_fattura,reclamo_guasto
         ,reclamo_legale,reclamo_spedizione
         ,variazione_agevolazione, variazione_anagrafica
         ,variazione_autolettura,variazione_credito
         ,variazione_fattura,variazione_pagamento
         ,variazione_rimodulazione,variazione_sconto_rimborso
         ,variazione_tecnico,tiposped
         ,bu_co, probability=p1) 


dim_clt = importDatiFromSql(sqlText = "select IdCliente, cluster from [mi-dataw].[it_dwh].[dbo].[dim_crm_clienti]")

pred_all = pred_all %>% inner_join(dim_clt, by=c("cliente" = "IdCliente"), copy=T)

spark_write_parquet(pred_all, "/user/flavio/predicted_business", mode="overwrite")

end_time <- Sys.time()
tempo_impiegato = end_time - start_time
print(paste("start time:",start_time,'-',"end time:",end_time))
print(tempo_impiegato)

spark_disconnect(sc)

