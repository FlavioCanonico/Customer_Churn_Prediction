
library(sparklyr)

pib_pq = spark_read_parquet(sc, "pib", "/user/flavio/pib",overwrite = TRUE)

# split train-test set
partitions <- sdf_random_split(pib_pq,training=0.7,test=0.3, seed = 12345)
pib_train <- partitions$training
pib_test <- partitions$test

#NB status: 1 è churned 0 è attivo
Sys.time()
gbt_pib <- pib_train %>%
  ml_gbt_classifier(status ~ mesicliente
                    +n_utenze
                    +flag_sciolto
                    +tipocliente
                    +fattmedia_imp
                    +areanielsencliente_imp
                    +etareferente_imp
                    +primamodalita
                    +ultimamodalita
                    +canalidigitali
                    +mobile
                    
                    +campagna_autolettura
                    +campagna_credito
                    +campagna_crossselling
                    
                    +campagna_promo_offerte
                    +campagna_recall
                    +campagna_rimodulazione
                    +campagna_spedizione
                    +campagna_tecnico
                    
                    +info_attivazione
                    +info_canali_digitali
                    +info_contorelax
                    +info_contratto
                    +info_credito
                    +info_disattivazione
                    +info_fattura
                    +info_modulistica
                    +info_pagamento
                    +info_promo_offerte
                    +info_recapito_consulente
                    +info_rimodulazione
                    +info_sconto_rimborso
                    +info_spedizione
                    +info_tecnico
                    +info_voltura
                    
                    +invio_credito
                    +invio_contratto
                    +invio_fattura
                    +invio_modulistica
                    
                    +reclamo_attivazione
                    +reclamo_credito
                    +reclamo_fattura
                    +reclamo_guasto
                    +reclamo_legale
                    +reclamo_spedizione
                    
                    +variazione_agevolazione
                    +variazione_anagrafica
                    +variazione_autolettura
                    +variazione_credito
                    +variazione_fattura
                    +variazione_pagamento
                    +variazione_rimodulazione
                    +variazione_sconto_rimborso
                    +variazione_tecnico
                    +tiposped
  )
Sys.time() #3 cores -> 32 secondi

ml_save(gbt_pib,path = "/user/flavio/models/gbt_pib",meta=ml_save_meta(gbt_pib, path="/user/flavio/models/gbt_pib"),overwrite = T)
# gbt_pib <- ml_load(sc,path = "hdfs://nameservice1/models/gbt_pib")

pred_gbt_pib <- ml_predict(gbt_pib,pib_test) #produce una serie di colonne, label è la vera risposta,
# probability_1 è la prob che ci sia il churn, prediction è la label predetta fissando la soglia a 0.5

#Accuracy con curva ROC-AUC
ml_binary_classification_evaluator(pred_gbt_pib)
tmp_pib <- ml_feature_importances(gbt_pib)

tmp_pib$feature <- factor(tmp_pib$feature, levels = tmp_pib$feature[order(tmp_pib$importance)])
# 
library(ggplot2)

png(file = "vimp_pib.png", height = 550, width = 550, pointsize = 18, bg = "transparent", res = 95) 
ggplot(data=tmp_pib, aes(x=feature, y=importance)) +
  geom_bar(colour="#DD8888", fill="#DD8888", width=.8, stat="identity") +
  guides(fill=FALSE) +
  xlab("Variabili") + ylab("Importanza") +
  theme(axis.text.x = element_text(angle = 90))+
  coord_flip()
dev.off()
# 
# ## 
# ## 
#matrice di confusione
Label_Pred_pib <- tibble(label = sdf_read_column(pred_gbt_pib,"label"), prediction = sdf_read_column(pred_gbt_pib,"prediction"))
gbt_stats_pib <- caret::confusionMatrix(table(Label_Pred_pib))

