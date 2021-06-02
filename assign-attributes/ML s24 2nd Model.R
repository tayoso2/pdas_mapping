
# Packages ----------------------------------------------------------------

library(tidyverse)
library(sf)
library(caret)
library(data.table)

library(party)
library(mlbench)

library(doParallel)
library(foreach)
library(tictoc)

# Data Load ---------------------------------------------------------------


# Your data set
# Set everything up into the right data formats
nonafactors3.year <- nonafactors3 %>% 
  as_tibble() %>% 
  mutate(DIAMETE = as.factor(DIAMETE),
         PURPOSE = as.factor(PURPOSE),
         DAS = as.factor(DAS), 
         PROPERTY_TYPE = as.factor(PROPERTY_TYPE))
nonafactors3.year %>% summary()

#split data for ML
set.seed(5)
train.base4 <- sample(1:nrow(nonafactors3.year),(nrow(nonafactors3.year)*0.75),replace = FALSE)
traindata.base4 <- nonafactors3.year[train.base4, ]
testdata.base4 <- nonafactors3.year[-train.base4, ]

# If you want to process in parallel do the following
cl <- makePSOCKcluster(3)
registerDoParallel(cl)

# Train the model
rf.fit41_2ndModel <- train(DIAMETE ~  PURPOSE + DAS + PROPERTY_TYPE + Year,
                  tuneLength = 1,
                  data = traindata.base4,
                  method = "ranger",
                  trControl = trainControl(
                    method = "cv",
                    allowParallel = TRUE,
                    number = 5,
                    verboseIter = T
                  ))

# Predict and test the model
tic()
rf.pred2 <- predict(rf.fit41_2ndModel, newdata = testdata.base4)
toc()
caret::confusionMatrix(rf.pred2, testdata.base4$DIAMETE) # DA and ppt_type and Year and n2 neighbours (81% and NIR 74%)
                                                         # DA and ppt_type and Year and Purpose (79.2% and NIR 74%)

# variable importance
rf.fit2 <- cforest(DIAMETE ~ n2_D + n2_M + n2_Y + PURPOSE + County + PROPERTY_TYPE + Year, 
                   data = traindata.base4, controls = cforest_unbiased(ntree = 50, mtry = 4))
rev(sort(varimp(rf.fit2)))


# Output Model ------------------------------------------------------------

rf.fit41_2ndModel %>% 
  write_rds("G:/Desktop/STW PDaS/Git/pdas_mapping/assign-attributes/S24diam_model DAS 2nd Model.RDS")




# Load model --------------------------------------------------------------

rf.fit41_2ndModel <- read_rds("G:/Desktop/STW PDaS/Git/pdas_mapping/assign-attributes/S24diam_model DAS 2nd Model.RDS")

rf.preds24 <- predict(rf.fit41_2ndModel, newdata = testdata.base4)
caret::confusionMatrix(rf.preds24, testdata.base4$DIAMETE)


# Load and Predict Notional assets -------------------------------------------

s24Path3 <- "C:/Users/TOsosanya/Desktop/STW PDAS 2/ModalandNearestNeighbour/Notional Assets for ML/Sprint5Notionals_Age_and_Length/sprint_5_notionals_agelength.shp"
s243 <- st_read(s24Path3, stringsAsFactors = F)
st_geometry(s243) <- NULL

s243$index <- seq.int(nrow(s243))
s24_2nd_DAS <- s243 %>%
  anti_join(s24_1st_DAS, by = "index") %>% 
  as_tibble() %>% 
  dplyr::mutate(ConNghY = ifelse(!is.na(post_year) & is.na(ConNghY),as.numeric(post_year),as.numeric(ConNghY))) %>% # add this to filter more pdas/s24
  dplyr::filter(ConNghY <= 1937) %>% 
  dplyr::rename(PROPERTY_TYPE = P_Type) %>% 
  dplyr::rename(n2_Y = ConNghY) %>% 
  dplyr::rename(n2_M = CnNghMt) %>% 
  dplyr::rename(n2_D = ConNghD) %>% 
  dplyr::rename(Year = post_year) %>%
  dplyr::rename(PURPOSE = CnNghTy) %>% 
  dplyr::mutate(PROPERTY_TYPE = ifelse(PROPERTY_TYPE == "Detatched","Detached",PROPERTY_TYPE),
                n2_D = as.numeric(as.character(n2_D)),
                PURPOSE = as.factor(PURPOSE),
                n2_M = as.factor(n2_M),
                n2_Y = as.numeric(n2_Y),
                DAS = as.factor(DA),
                PROPERTY_TYPE = as.factor(PROPERTY_TYPE),
                County = as.factor(County)) %>% 
  dplyr::select(index, PURPOSE, DAS, PROPERTY_TYPE, Year)


# remove 0 and NA
s24_2nd_DAS <- s24_2nd_DAS %>% 
  dplyr::filter(Year != 0, !is.na(PURPOSE))



## remove the NA
s24_2nd_DAS <- s24_2nd_DAS %>% 
  na.omit()
summary(s24_2nd_DAS)

## select notional s24 with same DAS in model selection i.e. data for 1st model -------------------------

s243_filtered4 <- s24_2nd_DAS %>% dplyr::select(DAS) %>% distinct()
nonafactors.s24.2ndModel <- nonafactors3.year %>% dplyr::select(DAS) %>% distinct() %>% unlist() %>% as.character()
s24_2nd_DAS <- s24_2nd_DAS %>% filter(DAS %in% nonafactors.s24.2ndModel)

## predict diameter
tic()
rf.prednotional_s24diam2nd <- predict(rf.fit41_2ndModel, newdata = s24_2nd_DAS)
toc()







