
library(plyr) #la carico perché nel wrapper faccio il detach

#prima manda il wrapper per ottenere le tabelle che servono
df_heatmap_pib <- pred_pib_all %>% select(cliente ,probability) %>%
  
  inner_join(survival_pib_unscaled, by="cliente") %>%
  
  dplyr::mutate(PredClass=case_when(probability >= 0 && probability <= 0.1 ~ '01.0-10',
                                    probability > 0.1 && probability <= 0.2 ~ '02.11-20',
                                    probability > 0.2 && probability <= 0.3 ~ '03.21-30',
                                    probability > 0.3 && probability <= 0.4 ~ '04.31-40',
                                    probability > 0.4 && probability <= 0.5 ~ '05.41-50',
                                    probability > 0.5 && probability <= 0.6 ~ '06.51-60',
                                    probability > 0.6 && probability <= 0.7 ~ '07.61-70',
                                    probability > 0.7 && probability <= 0.8 ~ '08.71-80',
                                    probability > 0.8 && probability <= 0.9 ~ '09.81-90',
                                    probability > 0.9 && probability <= 1 ~ '10.91-100',
                                    TRUE ~ NA),
                tipocliente = as.factor(tipocliente)) %>% 
  
  dplyr::mutate(PredClass3=case_when(probability >= 0 && probability <= 0.5 ~ '01.Green',
                                     probability > 0.5 && probability <= 0.75 ~ '02.Yellow',
                                     probability > 0.75 && probability <= 1 ~ '03.Red',
                                     TRUE ~ NA)) %>%
  
  dplyr::mutate(PredClass4=case_when(probability >= 0 && probability <= 0.25 ~ '01.Green',
                                     probability > 0.25 && probability <= 0.5 ~ '02.Yellow',
                                     probability > 0.5 && probability <= 0.75 ~ '03.Orange',
                                     probability > 0.75 && probability <= 1 ~ '04.Red',
                                     TRUE ~ NA))

# spark_write_parquet(pred_pib_all, path = '/user/flavio/pred_pib_all',mode='overwrite')
# df_heatmap_pib = spark_read_parquet(sc, "df_heatmap_pib", "/user/flavio/df_heatmap_pib",overwrite = TRUE)

df_heatmap_pib<-collect(df_heatmap_pib)

#Heatmap PIB (li ordino in base alla Variable Importance)
a = df_heatmap_pib[, c("probability","PredClass4","mesicliente","variazione_credito"	,"campagna_crossselling"	,"campagna_rimodulazione"	,"tipocliente",
                       "info_disattivazione"	,"campagna_credito"	,"info_fattura"	,"variazione_anagrafica"	,"invio_modulistica"	,
                       "canalidigitali"	,"info_credito"	,"n_utenze"	,"ultimamodalita"	,"invio_credito"	,"mobile"	,
                       "invio_fattura"	,"reclamo_guasto"	,"variazione_sconto_rimborso"	,"campagna_tecnico"	,"info_modulistica",
                       "info_contratto"	,"info_attivazione"	,"campagna_promo_offerte"	,"info_pagamento"	,"reclamo_fattura"	,
                       "info_voltura"	,"variazione_pagamento"	,"primamodalita"	,"info_canali_digitali"	,"info_contorelax"
                       ,"campagna_autolettura"	,"variazione_autolettura"	,"variazione_tecnico"	,"variazione_rimodulazione"	,"fattmedia"
                       ,"invio_contratto"	,"flag_sciolto"	,"reclamo_legale"	,"info_spedizione"	,"reclamo_credito"	,"info_tecnico"	
                       ,"campagna_recall"	,"tiposped"	,"variazione_fattura"	,"info_sconto_rimborso"	,"reclamo_attivazione"
                       ,"info_recapito_consulente"	,"info_rimodulazione"	,"variazione_agevolazione"	,"etareferente"	
                       ,"areanielsencliente"	,"info_promo_offerte"	,"campagna_spedizione"	,"reclamo_spedizione")]

nomi = names(a)
ne=nomi[-which(nomi %in% c("cliente", "tipocliente","areanielsencliente","primamodalita","tiposped","ultimamodalita"))]
tipocliente <- to.dummy(df_heatmap_pib$tipocliente, "tipocliente")
areanielsencliente <- to.dummy(df_heatmap_pib$areanielsencliente, "areanielsencliente")
primamodalita <- to.dummy(df_heatmap_pib$primamodalita, "primamodalita")
ultimamodalita <- to.dummy(df_heatmap_pib$ultimamodalita, "ultimamodalita")
tiposped <- to.dummy(df_heatmap_pib$tiposped, "tiposped")

b3=cbind.data.frame(df_heatmap_pib[,ne],tipocliente,areanielsencliente,primamodalita,tiposped,ultimamodalita)

b3 = b3[, c("PredClass4","mesicliente","variazione_credito"	,"campagna_crossselling"	,"campagna_rimodulazione"	,colnames(tipocliente),
            "info_disattivazione"	,"campagna_credito"	,"info_fattura"	,"variazione_anagrafica"	,"invio_modulistica"	,
            "canalidigitali"	,"info_credito"	,"n_utenze"	,colnames(ultimamodalita)	,"invio_credito"	,"mobile"	,
            "invio_fattura"	,"reclamo_guasto"	,"variazione_sconto_rimborso"	,"campagna_tecnico"	,"info_modulistica",
            "info_contratto"	,"info_attivazione"	,"campagna_promo_offerte"	,"info_pagamento"	,"reclamo_fattura"	,
            "info_voltura"	,"variazione_pagamento"	,colnames(primamodalita)	,"info_canali_digitali"	,"info_contorelax"
            ,"campagna_autolettura"	,"variazione_autolettura"	,"variazione_tecnico"	,"variazione_rimodulazione"	,"fattmedia"
            ,"invio_contratto"	,"flag_sciolto"	,"reclamo_legale"	,"info_spedizione"	,"reclamo_credito"	,"info_tecnico"	
            ,"campagna_recall"	,colnames(tiposped)	,"variazione_fattura"	,"info_sconto_rimborso"	,"reclamo_attivazione"
            ,"info_recapito_consulente"	,"info_rimodulazione"	,"variazione_agevolazione"	,"etareferente"	
            ,colnames(areanielsencliente)	,"info_promo_offerte"	,"campagna_spedizione"	,"reclamo_spedizione")]


b4 = melt(b3,id.vars="PredClass4")

b5 = b4%>%group_by(PredClass4,variable)%>%dplyr::summarise(value=mean(value,na.rm=T)) %>% ungroup()

# b5 = b5 %>% group_by(variable) %>% mutate(rescale = scale(value)) %>% ungroup()
b5=plyr::ddply(b5, .(variable), transform, rescale = scale(value)) #(value-mean(value,na.rm=T))/sd(value,na.rm = T)

base_size <- 9

b5$PredClass4=as.factor(b5$PredClass4)

p <- ggplot(b5, aes(PredClass4,variable)) + geom_tile(aes(fill = rescale), colour = "white") + scale_fill_gradient2(low="blue",mid="white",high="red",midpoint =0)
q <- p + theme_grey(base_size = base_size) + labs(y = "") + scale_x_discrete(expand = c(0, 0)) + theme( axis.text.x = element_text(size = base_size , angle = 330, hjust = 0, colour = "grey50"))

png(file = "/home/flavio/dev/BigData/churn_survival/plots/heatmap_pib.png",
    height = 570, width = 550, pointsize = 18, bg = "transparent", res = 95); q; dev.off()


