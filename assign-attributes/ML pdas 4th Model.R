
# Packages ----------------------------------------------------------------

library(tidyverse)
library(sf)
library(caret)
library(data.table)
library(rgdal)

library(party)
library(mlbench)

library(doParallel)
library(foreach)
library(tictoc)


# bring back the train and test datasets
nonafactors2.year <- nonafactors2 %>% dplyr::filter(MATERIA != "CI") %>% 
  dplyr::mutate(MATERIA = as.character(MATERIA)) %>% 
  dplyr::mutate(MATERIA = as.factor(MATERIA))
nonafactors2.year %>% summary()
set.seed(3)
train.base.year <- sample(1:nrow(nonafactors2.year),(nrow(nonafactors2.year)*0.75),replace = FALSE)
traindata.base.year <- nonafactors2.year[train.base.year, ]
testdata.base.year <- nonafactors2.year[-train.base.year, ]

# If you want to do it in parallel
cl <- makePSOCKcluster(3)
registerDoParallel(cl)

rf.fit.pdas_3rd_diam <- train(DIAMETE ~ Year + PURPOSE + County + PROPERTY_TYPE,
                              tuneLength = 1,
                              data = traindata.base.year,
                              method = "ranger",
                              trControl = trainControl(
                                method = "cv",
                                allowParallel = F,
                                number = 5,
                                verboseIter = T
                              ))


# Predict and test the model
tic()
rf.pred3rd_diam <- predict(rf.fit.pdas_3rd_diam, newdata = testdata.base.year)
toc()
caret::confusionMatrix(rf.pred3rd_diam, testdata.base.year$DIAMETE) # 3rd model (63.6%)


## material
cl <- makePSOCKcluster(3)
registerDoParallel(cl)

rf.fit.pdas_3rd_mat <- train(MATERIA ~ Year + PURPOSE + County + PROPERTY_TYPE,
                             tuneLength = 1,
                             data = traindata.base.year,
                             method = "ranger",
                             trControl = trainControl(
                               method = "cv",
                               allowParallel = F,
                               number = 5,
                               verboseIter = T
                             ))


# Predict and test the model
tic()
rf.pred3rd_diam <- predict(rf.fit.pdas_3rd_mat, newdata = testdata.base.year)
toc()
caret::confusionMatrix(rf.pred3rd_diam, testdata.base.year$MATERIA) # 3rd model (93.2%)


# Output Model ------------------------------------------------------------

rf.fit.pdas_4th_diam %>% 
  write_rds("G:/Desktop/STW PDaS/Git/pdas_mapping/assign-attributes/pdasdiam_model_county_4th.RDS")

rf.fit.pdas_4th_mat %>% 
  write_rds("G:/Desktop/STW PDaS/Git/pdas_mapping/assign-attributes/pdasmat_model_county_4th.RDS")

# Load model --------------------------------------------------------------

rf.fit.pdas_4th_diam <- read_rds("G:/Desktop/STW PDaS/Git/pdas_mapping/assign-attributes/pdasdiam_model_county_4th.RDS")

rf.fit.pdas_4th_mat <- read_rds("G:/Desktop/STW PDaS/Git/pdas_mapping/assign-attributes/pdasmat_model_county_4th.RDS")


# Load and Predict Notional assets -------------------------------------------

PdasPath4 <- "C:/Users/TOsosanya/Desktop/STW PDAS 2/ModalandNearestNeighbour/Notional Assets for ML/Sprint5Notionals_Age_and_Length/sprint_5_notionals_agelength.shp"
pdas4 <- st_read(PdasPath4, stringsAsFactors = F)
st_geometry(pdas4) <- NULL

# get the data for the 2nd model
pdas4$index <- seq.int(nrow(pdas4))

## get the data for the 4th pdas model
pdas_4th_County <- pdas4 %>% anti_join(pdas_1st_DAS, by = "index") %>% 
  anti_join(pdas_2nd_County, by = "index") %>%
  anti_join(pdas_3rd_County, by = "index") %>%
  as_tibble() %>% 
  dplyr::mutate(ConNghY = ifelse(!is.na(post_year) & is.na(ConNghY),as.numeric(post_year),as.numeric(ConNghY))) %>% # add this to filter more pdas/s24
  dplyr::filter(ConNghY > 1937) %>% 
  dplyr::rename(PROPERTY_TYPE = P_Type) %>% 
  dplyr::rename(n2_Y = ConNghY) %>% 
  dplyr::rename(n2_M = CnNghMt) %>% 
  dplyr::rename(Year = post_year) %>%
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
  dplyr::select(index, PURPOSE, County, PROPERTY_TYPE, Year) #%>% 

# remove 0 and NA
pdas_4th_County <- pdas_4th_County %>% 
  dplyr::filter(Year != 0, !is.na(PURPOSE)) 

summary(pdas_4th_County)


# predict notionals ------------------------------------------------------------------------------------

## predict diameter 
tic()
# model 2
rf.prednotionalpdas_diam4th <- predict(rf.fit.pdas_4th_diam, newdata = pdas_4th_County)
toc()

## predict material
tic()
# model 2
rf.prednotionalpdas_mat4th <- predict(rf.fit.pdas_4th_mat, newdata = pdas_4th_County)
toc()



# not predicted at all

not_predicted_pdas <- pdas4 %>% anti_join(pdas_1st_DAS, by = "index") %>% 
  anti_join(pdas_2nd_County, by = "index") %>% 
  anti_join(pdas_3rd_County, by = "index") %>% 
  anti_join(pdas_4th_County, by = "index") %>% 
  as_tibble() %>% 
  dplyr::filter(ConNghY > 1937)

not_predicted_pdas
