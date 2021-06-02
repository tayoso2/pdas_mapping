# predict for remaining assets

NotionalPath <- "C:/Users/TOsosanya/Desktop/STW PDAS 2/ModalandNearestNeighbour/Notional Assets for ML/Sprint5Notionals_Age_and_Length/sprint_5_notionals_agelength.shp"
Notional <- st_read(NotionalPath, stringsAsFactors = F)
Notionalgeom = Notional
st_geometry(Notional) <- NULL

Notionalgeom$index <- seq.int(nrow(Notionalgeom))
Notional$index <- seq.int(nrow(Notional))
Notional_filtered <- Notional %>%  
  as_tibble() %>% 
  dplyr::rename(PROPERTY_TYPE = P_Type) %>% 
  dplyr::rename(n2_Y = ConNghY) %>% 
  dplyr::rename(n2_M = CnNghMt) %>% 
  dplyr::rename(n2_D = ConNghD) %>% 
  dplyr::rename(PURPOSE = CnNghTy) %>% 
  dplyr::mutate(PROPERTY_TYPE = ifelse(PROPERTY_TYPE == "Detatched","Detached",PROPERTY_TYPE),
                n2_D = as.numeric(as.character(n2_D)),
                PURPOSE = as.factor(PURPOSE),
                n2_M = as.factor(n2_M),
                n2_Y = as.numeric(n2_Y),
                PROPERTY_TYPE = as.factor(PROPERTY_TYPE),
                County = as.factor(County)) %>% 
  dplyr::select(index, n2_D, n2_M, n2_Y, PURPOSE, County, PROPERTY_TYPE, count)


# model 1
predicted_pdas_1 <- cbind(pdas_1st_DAS,MATERIA = rf.prednotionalpdas_mat1st,DIAMETE = rf.prednotionalpdas_diam1st) %>% 
  dplyr::mutate(DIAMETE = as.numeric(as.character(DIAMETE))) %>% 
  dplyr::mutate(Population_Method = "A") %>% 
  dplyr::mutate(Sewer_Type = "PDAS")
predicted_s24_1 <- cbind(s24_1st_DAS, MATERIA = "VC", DIAMETE = rf.prednotionals24_diam1st) %>% 
  dplyr::mutate(DIAMETE = as.numeric(as.character(DIAMETE))) %>% 
  dplyr::mutate(Population_Method = "A") %>% 
  dplyr::mutate(Sewer_Type = "S24")

# model 2
predicted_pdas_2 <- cbind(pdas_2nd_County,MATERIA = rf.prednotionalpdas_mat2nd,DIAMETE = rf.prednotionalpdas_diam2nd) %>% 
  dplyr::mutate(DIAMETE = as.numeric(as.character(DIAMETE))) %>% 
  dplyr::mutate(Population_Method = "B") %>% 
  dplyr::mutate(Sewer_Type = "PDAS")
predicted_s24_2 <- cbind(s24_2nd_DAS, MATERIA = "VC", DIAMETE = rf.prednotional_s24diam2nd) %>%
  dplyr::mutate(DIAMETE = as.numeric(as.character(DIAMETE)))%>%
  dplyr::mutate(Population_Method = "B") %>%
  dplyr::mutate(Sewer_Type = "S24")

# model 3
predicted_pdas_3 <- cbind(pdas_3rd_County,MATERIA = rf.prednotionalpdas_mat3rd,DIAMETE = rf.prednotionalpdas_diam3rd) %>% 
  dplyr::mutate(DIAMETE = as.numeric(as.character(DIAMETE))) %>% 
  dplyr::mutate(Population_Method = "C") %>% 
  dplyr::mutate(Sewer_Type = "PDAS")
predicted_s24_3 <- cbind(s24_3rd_County, MATERIA = "VC", DIAMETE = rf.prednotional_s24diam3rd) %>%
  dplyr::mutate(DIAMETE = as.numeric(as.character(DIAMETE)))%>%
  dplyr::mutate(Population_Method = "C") %>%
  dplyr::mutate(Sewer_Type = "S24")

# model 4
predicted_pdas_4 <- cbind(pdas_4th_County,MATERIA = rf.prednotionalpdas_mat4th,DIAMETE = rf.prednotionalpdas_diam4th) %>% 
  dplyr::mutate(DIAMETE = as.numeric(as.character(DIAMETE))) %>% 
  dplyr::mutate(Population_Method = "D") %>% 
  dplyr::mutate(Sewer_Type = "PDAS")


# not predicted
predicted_pdas_5 <- cbind(not_predicted_pdas,MATERIA = "VC",DIAMETE = "100") %>% 
  dplyr::mutate(DIAMETE = as.numeric(as.character(DIAMETE))) %>% 
  dplyr::mutate(Population_Method = "E") %>% 
  dplyr::mutate(Sewer_Type = "PDAS")
predicted_s24_5 <- cbind(not_predicted_s24, MATERIA = "VC", DIAMETE = "150") %>%
  dplyr::mutate(DIAMETE = as.numeric(as.character(DIAMETE)))%>%
  dplyr::mutate(Population_Method = "E") %>%
  dplyr::mutate(Sewer_Type = "S24")


# merge all together
predicted_pdas <- plyr::rbind.fill(predicted_pdas_1,predicted_pdas_2,predicted_pdas_3,predicted_pdas_4, predicted_pdas_5) %>% 
  dplyr::mutate(DIAMETE = as.numeric(as.character(DIAMETE))) %>% 
  dplyr::select(index,County, PROPERTY_TYPE, Year, MATERIA, DIAMETE, Population_Method, Sewer_Type, PURPOSE)
write.csv(predicted_pdas, "predicted_pdas.csv")

predicted_s24 <- plyr::rbind.fill(predicted_s24_1,predicted_s24_2,predicted_s24_3,predicted_s24_5) %>%  
  dplyr::mutate(DIAMETE = as.numeric(as.character(DIAMETE))) %>% 
  dplyr::select(index,County, PROPERTY_TYPE, Year, MATERIA, DIAMETE, Population_Method, Sewer_Type, PURPOSE)
write.csv(predicted_s24, "predicted_s24.csv")

notional_predicted <- rbind(predicted_pdas,predicted_s24)

# add the variables
notional_predicted2 <- notional_predicted %>% 
  left_join(Notional[c("fid","propgrp","index", "postcode","DA",
                       "Length", "ConNghY" ,"CnNghMt", "ConNghD",  "CnNghTg")], by = "index") %>% 
  dplyr::rename(post_year = Year)
write.csv(notional_predicted2, "notional_predicted.csv")


# for the assets with no year ---------------------------------------------

notional_predicted2 <- read.csv("G:/Desktop/STW PDaS/Git/pdas_mapping/assign-attributes/notional_predicted.csv")

not_predictedgeom <- Notional_filtered %>% anti_join(notional_predicted2, by = "index") %>% 
  left_join(Notionalgeom[,c("index", "geometry")], by = "index")

notional_predictedgeom <- notional_predicted2 %>% 
  left_join(Notionalgeom[,c("index", "geometry")], by = "index")

not_predictedgeom <- st_as_sf(not_predictedgeom, crs =27700)
notional_predictedgeom <- st_as_sf(notional_predictedgeom, crs =27700)

# find nearest features - not predicted and predicted
get_nearest_features <- function(x, y, pipeid, id, feature) {
  
  # pipeid is id of x
  # id is id of y
  # feature is attribute to be added to x from y
  
  # Determine CRSs
  message(paste0("x Coordinate reference system is ESPG: ", st_crs(x)$epsg))
  message(paste0("y Coordinate reference system is ESPG: ", st_crs(y)$epsg))
  
  # Transform y CRS to x CRS if required
  if (st_crs(x) != st_crs(y)) {
    message(paste0(
      "Transforming y coordinate reference system to ESPG: ",
      st_crs(x)$epsg
    ))
    y <- st_transform(y, st_crs(x))
  }
  
  # Get rownumber of x
  x$rowguid <- seq.int(nrow(x))
  y$rowguid <- seq.int(nrow(y))
  
  # edit column names
  colnames(x)[which(names(x) == pipeid)] <- "id"
  colnames(y)[which(names(y) == id)] <- "id"
  colnames(y)[which(names(y) == feature)] <- "feature"
  
  # get nearest features
  nearest <- st_nearest_feature(x,y)
  x$index <- nearest
  y2 <- dplyr::select(y, id, feature, rowguid)
  st_geometry(y2) <- NULL
  x2 <- x %>% left_join(as.data.frame(y2), by = c("index" = "rowguid"))
  output <- x2
  
  # revert column names
  colnames(output)[which(names(output) == "id")] <- pipeid
  colnames(output)[which(names(output) == "feature")] <- feature
  print(output)
}
pair <- get_nearest_features(not_predictedgeom,notional_predictedgeom,"index","index","Sewer_Type")

# get the diameter and material from "y" in function above
pair2 <- pair %>% left_join(as.data.frame(notional_predictedgeom)[,c("index","DIAMETE","MATERIA")], by = c("id.y" = "index"))

# attach the attributes from the original notional asset base 
pair3 <- pair2[,c("id.x","DIAMETE","MATERIA","Sewer_Type")] %>% 
  dplyr::rename(index = id.x) %>% 
  left_join(Notional[c("fid","propgrp","index", "postcode","DA", "post_year",
                       "Length", "ConNghY" ,"CnNghMt", "ConNghD",  "CnNghTg")], by = "index") %>% 
  dplyr::mutate(Population_Method = "X") %>% 
  dplyr::mutate(Sewer_Type = as.factor(Sewer_Type))
st_geometry(pair3) <- NULL
summary(pair3)

write.csv(pair3, "G:/Desktop/STW PDaS/Git/pdas_mapping/assign-attributes/notional_predicted_with_nearest_neghbour.csv")





# plot the predicted assets ###############

## pdas
## Materials
for(purposes in c("C","F","S")) {
  print(ggplot(predicted_pdas %>% 
                 filter(PURPOSE == !!purposes)) +
          geom_bar(aes(x = MATERIA),fill = "#eb5e34") +
          scale_y_continuous(
            limits = c(0,750000),
            labels = function(x) {
              paste0(x / 10 ^ 3, 'K')
            }
          )  + gg +
          ggtitle(paste0("Type of pipe: ",purposes)) +
          ylab("Number of pipes") + 
          xlab("Pipe Material"))
}

## Diameter
for(purposes in c("C","F","S")) {
  print(ggplot(predicted_pdas %>% 
                 filter(PURPOSE == !!purposes)) +
          geom_histogram(aes(x = DIAMETE),fill = "#eb5e34") +
          scale_y_continuous(
            limits = c(0,500000),
            labels = function(x) {
              paste0(x / 10 ^ 3, 'K')
            }
          )  + gg +
          ggtitle(paste0("Type of pipe: ",purposes)) + 
          xlim(1,250))
}



##s24

## Materials
for(purposes in c("C","F","S")) {
  print(ggplot(predicted_s24 %>% 
                 filter(PURPOSE == !!purposes)) +
          geom_bar(aes(x = MATERIA),fill = "#eb5e34") +
          scale_y_continuous(
            limits = c(0,500000),
            labels = function(x) {
              paste0(x / 10 ^ 3, 'K')
            }
          )  + gg +
          ggtitle(paste0("Type of pipe: ",purposes)) +
          ylab("Number of pipes") + 
          xlab("Pipe Material")) 
  }   

## Diameter
for(purposes in c("C","F","S")) {
  print(ggplot(predicted_s24 %>% 
                 filter(PURPOSE == !!purposes)) +
          geom_histogram(aes(x = DIAMETE),fill = "#eb5e34") +
          scale_y_continuous(
            limits = c(0,500000),
            labels = function(x) {
              paste0(x / 10 ^ 3, 'K')
            }
          )  + gg +
          ggtitle(paste0("Type of pipe: ",purposes)) + 
          xlim(1,250))
}









# remove the non predicted assets ####
not_predicted <- Notional_filtered %>% anti_join(notional_predicted, by = "index")
#write.csv(not_predicted,"not_predicted_notionals.csv")
summary(not_predicted)

remaining_pdas <- not_predicted %>%  
  as_tibble() %>% 
  dplyr::filter(ConNghY > 1937) %>% 
  dplyr::rename(PROPERTY_TYPE = P_Type) %>% 
  dplyr::rename(n2_Y = ConNghY) %>% 
  dplyr::rename(n2_M = CnNghMt) %>% 
  dplyr::rename(n2_D = ConNghD) %>% 
  dplyr::rename(PURPOSE = CnNghTy) %>% 
  dplyr::mutate(PROPERTY_TYPE = ifelse(PROPERTY_TYPE == "Detatched","Detached",PROPERTY_TYPE),
                n2_D = as.numeric(as.character(n2_D)),
                PURPOSE = as.factor(PURPOSE),
                n2_M = as.factor(n2_M),
                n2_Y = as.numeric(n2_Y),
                PROPERTY_TYPE = as.factor(PROPERTY_TYPE),
                County = as.factor(County),
                DAS = as.factor(DA)) %>% 
  dplyr::select(index, n2_D, n2_M, n2_Y, PURPOSE, DAS, County, PROPERTY_TYPE) %>% 
  na.omit()

