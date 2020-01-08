spark_disconnect(sc)

rm(list=ls())

start_time = Sys.time()

home = '/home/flavio/dev/BigData/churn_survival/'

#carico le librerie
source(paste(home,"librerie.R",sep=''))

#Richiamo la function importDatiFromSql
source("/home/gennaronew/Library/importDatiFromSQL.R")

# Spark Connection
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
detach("package:plyr", unload=TRUE)

# Generazione dei dati
Sys.time()
source(paste(home,"Dati.R",sep=''))
Sys.time()

Sys.time()
source(paste(home,"Dati_old.R",sep=''))
Sys.time()


# Carico i dati
survival_pq = spark_read_parquet(sc, "survival", "/user/flavio/survival",overwrite = TRUE)
pib_pq = survival_pq %>% filter(classe == "pib")
pic_pq = survival_pq %>% filter(classe == "pic")
sciolto_pq = survival_pq %>% filter(classe == "sciolto")

survival_old_pq = spark_read_parquet(sc, "survival_old", "/user/flavio/survival_old",overwrite = TRUE)
pib_old_pq = survival_old_pq %>% filter(classe == "pib")
pic_old_pq = survival_old_pq %>% filter(classe == "pic")
sciolto_old_pq = survival_old_pq %>% filter(classe == "sciolto")

# pib_pq = spark_read_parquet(sc, "survival_pib", "/user/flavio/survival_pib",overwrite = TRUE)
# pib_old_pq = spark_read_parquet(sc, "survival_old_pib", "/user/flavio/survival_old_pib",overwrite = TRUE)
pib_pq_all = dplyr::union(pib_pq, pib_old_pq)

# pic_pq = spark_read_parquet(sc, "survival_pic", "/user/flavio/survival_pic",overwrite = TRUE)
# pic_old_pq = spark_read_parquet(sc, "survival_old_pic", "/user/flavio/survival_old_pic",overwrite = TRUE)
pic_pq_all = dplyr::union(pic_pq, pic_old_pq)

# sciolto24_pq = spark_read_parquet(sc, "survival_sciolto24", "/user/flavio/survival_sciolto24",overwrite = TRUE)
# sciolto_old_pq = spark_read_parquet(sc, "survival_old_sciolto", "/user/flavio/survival_old_sciolto",overwrite = TRUE)
sciolto_pq_all = dplyr::union(sciolto_pq, sciolto_old_pq)

# Carico i modelli
gbt_pib <- ml_load(sc, path = "/user/flavio/models/gbt_pib")
gbt_pic <- ml_load(sc, path = "/user/flavio/models/gbt_pic")
gbt_sciolto <- ml_load(sc, path = "/user/flavio/models/gbt_sciolto")

# Predizioni
pred_pib_all <- ml_predict(gbt_pib,pib_pq_all)
pred_pic_all <- ml_predict(gbt_pic,pic_pq_all)
pred_sciolto_all <- ml_predict(gbt_sciolto,sciolto_pq_all)

pred_pib_all<- pred_pib_all %>% sdf_separate_column("probability", c("p0","p1")) %>%
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


pred_pic_all<- pred_pic_all %>% sdf_separate_column("probability", c("p0","p1")) %>%
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

pred_sciolto_all<- pred_sciolto_all %>% sdf_separate_column("probability", c("p0","p1")) %>%
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


pred_all = dplyr::union(pred_pib_all, pred_pic_all)
pred_all = dplyr::union(pred_all, pred_sciolto_all)

dim_clt = importDatiFromSql(sqlText = "select IdCliente, cluster from [mi-dataw].[it_dwh].[dbo].[dim_crm_clienti]")

pred_all = pred_all %>% inner_join(dim_clt, by=c("cliente" = "IdCliente"), copy=T)

predicted_business = pred_all %>% filter(cluster == "BUSINESS")
predicted_consumer = pred_all %>% filter(cluster == "CONSUMER")

spark_write_parquet(predicted_business, "/user/flavio/predicted_business", mode="overwrite")
spark_write_parquet(predicted_consumer, "/user/flavio/predicted_business", mode="overwrite")

# spark_write_parquet(predicted_Bus_all, "/user/flavio/pred_pib_all", mode="overwrite")
# spark_write_parquet(predicted_Con_all, "/user/flavio/pred_pic_all", mode="overwrite")
# spark_write_parquet(predicted_Con_all, "/user/flavio/pred_sciolto_all", mode="overwrite")

end_time <- Sys.time()
tempo_impiegato = end_time - start_time
print(paste("start time:",start_time,'-',"end time:",end_time))
print(tempo_impiegato)

spark_disconnect(sc)

