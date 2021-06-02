
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

#PdasPath1 <- "G:/Desktop/STW PDaS/S24 PDaS Filtered GIS/Joined_PDaS_DA.shp"
PdasPath2 <- "C:/Users/TOsosanya/Desktop/STW PDAS 2/ModalandNearestNeighbour/PDASwithnn/PDASwithnn_merged.shp"
PdasPath3 <- "C:/Users/TOsosanya/Desktop/STW PDAS 2/ModalandNearestNeighbour/PDaS_with_property_type.csv"
#pdas1 <- st_read(PdasPath1, stringsAsFactors = F)
pdas2 <- st_read(PdasPath2, stringsAsFactors = F)
pdas3 <- read.csv(PdasPath3)
#st_geometry(pdas1) <- NULL
#pdas <- pdas2 %>% left_join(pdas1[,c(1,10)], by = "PIPEID")
pdas3 <- pdas3 %>% select(PIPEID,LENGTH,PURPOSE,dom_prop80) %>% #there is pipegroup
  dplyr::rename(PROPERTY_TYPE = dom_prop80)
pdas <- pdas2 %>% left_join(pdas3[,c("PIPEID","PROPERTY_TYPE")], by = "PIPEID") %>% 
  filter(PROPERTY_TYPE != "")
summary(pdas)



# Functions ---------------------------------------------------------------

rollmeTibble <- function(df1, values, col1, method) {
  
  df1 <- df1 %>% 
    mutate(merge = !!sym(col1)) %>% 
    as.data.table()
  
  values <- as_tibble(values) %>% 
    mutate(band = row_number()) %>% 
    mutate(merge = as.numeric(value)) %>% 
    as.data.table()
  
  setkeyv(df1, c('merge'))
  setkeyv(values, c('merge'))
  
  Merged = values[df1, roll = method]
  
  Merged
}

# Data exploration --------------------------------------------------------

# Initial distributions

# Materials
for(purposes in c("C","F","S")) {
  print(ggplot(pdas %>% 
                 filter(PURPOSE == !!purposes)) +
          geom_bar(aes(x = MATERIA)) +
          ggtitle(paste0("Type of pipe: ",purposes)) +
          ylab("Number of pipes") + 
          xlab("Pipe Material"))
}

# Diameter
for(purposes in c("C","F","S")) {
  print(ggplot(pdas %>% 
                 filter(PURPOSE == !!purposes)) +
          geom_histogram(aes(x = DIAMETE)) +
          ggtitle(paste0("Type of pipe: ",purposes)) + 
          xlim(1,250))
}

# Get counts
pdas %>% 
  st_drop_geometry() %>% 
  count(PURPOSE, DIAMETE)

# Filter out those that don't have neighbours with all attributes

noNAs <- pdas %>% 
  filter(!is.na(n2_D)) %>% 
  filter(!is.na(n2_M)) %>% 
  filter(!is.na(n2_Y))

# Get rid of that damn geometry
# Select the "important cols"
noNAssmall <- noNAs %>%
  st_drop_geometry() %>% 
  select(PIPEID, PURPOSE, County,
         MATERIA, DIAMETE,PROPERTY_TYPE,
         n2_D, n2_M, n2_Y)  %>% 
  mutate(n2_D = as.numeric(n2_D),
         DIAMETE = as.numeric(DIAMETE))


# Nearest Dia -------------------------------------------------------------

# Need to do this to roll the diameters into classes
# These are the decided classes
diameterclasses <-c(0, 50, 100, 150, 225, 300, 375, 450 ,525, 600, 675, 750, 825,
                    900, 975, 1050, 1200, 1350, 1500)

n24_reduced <- rollmeTibble(df1 = noNAssmall,
                            values = diameterclasses,
                            col1 = "n2_D",
                            method = 'nearest') %>%
  select(-n2_D, -band, -merge) %>% 
  rename(n2_D = value) %>% 
  
  rollmeTibble(values = diameterclasses,
               col1 = "DIAMETE",
               method = 'nearest') %>%
  select(-DIAMETE, -band, -merge) %>% 
  rename(DIAMETE = value)

# Check 0 counts
n24_reduced %>% 
  as_tibble() %>% 
  count(DIAMETE)

# Check NAs in material
n24_reduced %>% 
  as_tibble() %>% 
  sapply(function(x) sum(is.na(x)))

# Remove any diameters with 0 counts
n24s_dia <- n24_reduced %>%
  filter(DIAMETE != 0) %>% 
  filter(n2_D != 0)




# Machine Learning --------------------------------------------------------


# Diameter ----------------------------------------------------------------

# Your data set
# Set up the correct data types
nonafactors <- n24s_dia %>% 
  as_tibble() %>% 
  mutate(DIAMETE = as.factor(DIAMETE),
         n2_D = as.factor(n2_D),
         PURPOSE = as.factor(PURPOSE),
         n2_M = as.factor(n2_M),
         n2_Y = as.numeric(n2_Y),
         County = as.factor(County), 
         PROPERTY_TYPE = as.factor(PROPERTY_TYPE)) %>% 
  filter(!is.na(PURPOSE))

# Check no NA's
nonafactors %>% 
  as_tibble() %>% 
  sapply(function(x) sum(is.na(x)))

#split data for ML
# Using a smaller split due to smaller amount of records
set.seed(3)
train.base <- sample(1:nrow(nonafactors),(nrow(nonafactors)*0.75),replace = FALSE)
traindata.base <- nonafactors[train.base, ]
testdata.base <- nonafactors[-train.base, ]

# If you want to do it in parallel
cl <- makePSOCKcluster(3)
registerDoParallel(cl)

# Ideally want to change this so it has DA in it instead of COUNTY
rf.fitPDaSdia <- train(DIAMETE ~ n2_D + n2_M + n2_Y + PURPOSE + County + PROPERTY_TYPE,
                       tuneLength = 5,
                       data = traindata.base,
                       method = "ranger",
                       trControl = trainControl(
                         method = "cv",
                         allowParallel = TRUE,
                         number = 5,
                         verboseIter = T))

tic()
rf.predPDaSdia <- predict(rf.fitPDaSdia, newdata = testdata.base)
toc()
caret::confusionMatrix(rf.predPDaSdia, testdata.base$DIAMETE) # 65% accuracy using DA with NIR of 56%, 
                                                              # 69.4% with DA and Property type with NIR of 57.7%
                                                              # 68% with county and Property type with NIR of 57.7%


# variable importance
rf.fit1 <- cforest(DIAMETE ~ n2_D + n2_M + n2_Y + PURPOSE + County + PROPERTY_TYPE, 
                   data = traindata.base, controls = cforest_unbiased(ntree = 50, mtry = 4))
rev(sort(varimp(rf.fit1)))

# Material ----------------------------------------------------------------

# This is all the same as above but for MATERIAL instead of DIAMETER
## band unbanded materials
unband_band <- read.csv("G:\\Desktop\\STW PDaS\\old stuff\\Assets - Rulesets - Material Banding Table v2.csv")
n24s_dia <- n24s_dia %>%
  left_join(unique(unband_band[,c(1,2)]),by = c("MATERIA"="MaterialTable_UnbandedMaterial")) %>% 
  dplyr::mutate(MATERIA = (MATERIA = as.character(MaterialTable_BandedMaterial)))
unique(n24s_dia$MATERIA)

n24s_dia$MATERIA[n24s_dia$MATERIA == ""] <- NA
nonafactors2 <- n24s_dia %>% 
  as_tibble() %>% 
  mutate(DIAMETE = as.factor(DIAMETE),
         n2_D = as.factor(n2_D),
         PURPOSE = as.factor(PURPOSE),
         n2_M = as.factor(n2_M),
         n2_Y = as.numeric(n2_Y),
         MATERIA = as.factor(MATERIA),
         PROPERTY_TYPE = as.factor(PROPERTY_TYPE),
         County = as.factor(County)) %>% 
  filter(!is.na(PURPOSE)) %>% 
  filter(!is.na(MATERIA)) %>% 
  select(-MaterialTable_BandedMaterial) 
unique(nonafactors2$MATERIA)

nonafactors2 %>% 
  as_tibble() %>% 
  sapply(function(x) sum(is.na(x)))

#split data for ML
set.seed(8)
train.baseMAT <- sample(1:nrow(nonafactors2),(nrow(nonafactors2)*0.75),replace = FALSE)
traindata.baseMAT <- nonafactors2[train.baseMAT, ]
testdata.baseMAT <- nonafactors2[-train.baseMAT, ]

rf.fitMAT <- train(MATERIA ~ n2_D + n2_M + n2_Y + PURPOSE + County + PROPERTY_TYPE,
                   tuneLength = 5,
                   data = traindata.baseMAT,
                   method = "ranger",
                   trControl = trainControl(
                     method = "cv",
                     allowParallel = TRUE,
                     number = 5,
                     verboseIter = T
                   ))

tic()
rf.predMAT <- predict(rf.fitMAT, newdata = testdata.baseMAT)
toc()
caret::confusionMatrix(rf.predMAT, testdata.baseMAT$MATERIA) #60% accuracy using county and 
                                                             # das (92.25% and NIR 88.77%) 
                                                            # das + ppttype (95.48% and NIR 89.43%)
                                                            # county + ppttype (94.54% and NIR 89.43%)

# variable importance
rf.fit21 <- cforest(MATERIA ~ n2_D + n2_M + n2_Y + PURPOSE + County + PROPERTY_TYPE, 
                   data = traindata.baseMAT, controls = cforest_unbiased(ntree = 50, mtry = 4))
rev(sort(varimp(rf.fit21)))


# Write models out --------------------------------------------------------


rf.fitPDaSdia %>% 
  write_rds("G:/Desktop/STW PDaS/Git/pdas_mapping/assign-attributes/PDaSdiam_model County.RDS")

rf.fitMAT %>% 
  write_rds("G:/Desktop/STW PDaS/Git/pdas_mapping/assign-attributes/PDaSmat_model County.RDS")


# Load model --------------------------------------------------------------

rf.fitPDaSdia <- read_rds("G:/Desktop/STW PDaS/Git/pdas_mapping/assign-attributes/PDaSdiam_model County.RDS")

rf.pred1 <- predict(rf.fitPDaSdia, newdata = testdata.base)
caret::confusionMatrix(rf.pred1, testdata.base$DIAMETE) 

# Mat
rf.fitMAT <- read_rds("G:/Desktop/STW PDaS/Git/pdas_mapping/assign-attributes/PDaSmat_model County.RDS")

rf.predMAT <- predict(rf.fitMAT, newdata = testdata.baseMAT)
caret::confusionMatrix(rf.predMAT, testdata.baseMAT$MATERIA) 


# Load and Predict Notional assets -------------------------------------------

PdasPath4 <- "C:/Users/TOsosanya/Desktop/STW PDAS 2/ModalandNearestNeighbour/Notional Assets for ML/sprint_5_notionals_merged/sprint_5_notionals.shp"
pdas4 <- st_read(PdasPath4, stringsAsFactors = F)
st_geometry(pdas4) <- NULL

pdas4_filtered <- pdas4 %>%  
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
         County = as.factor(County)) %>% 
  dplyr::select(index, n2_D, n2_M, n2_Y, PURPOSE, County, PROPERTY_TYPE) %>% 
  na.omit()


# band the n2_D
pdas4_filtered2 <- rollmeTibble(df1 = pdas4_filtered,
                                values = diameterclasses,
                                col1 = "n2_D",
                                method = 'nearest') %>%
  select(-n2_D, -band, -merge) %>% 
  rename(n2_D = value) %>% 
  dplyr::filter(n2_D != 0) %>% 
  dplyr::mutate(n2_D = as.factor(n2_D))

## band the n2_M
pdas4_filtered2 <- pdas4_filtered2 %>%
  left_join(unique(unband_band[,c(1,2)]),by = c("n2_M"="MaterialTable_UnbandedMaterial")) %>% 
  dplyr::mutate(n2_M = (n2_M = as.factor(MaterialTable_BandedMaterial))) %>% 
  dplyr::filter(!is.na(n2_M)) %>% 
  dplyr::select(-MaterialTable_BandedMaterial)
unique(pdas4_filtered2$n2_M)
pdas4_filtered2$n2_M[pdas4_filtered2$n2_M == ""] <- NA

## remove the NA
pdas4_filtered2 <- pdas4_filtered2 %>% 
  na.omit()
summary(pdas4_filtered2)

## predict material
tic()
rf.prednotionalpdas_mat <- predict(rf.fitMAT, newdata = pdas4_filtered2)
toc()

## predict diameter
tic()
rf.prednotionalpdas_diam <- predict(rf.fitPDaSdia, newdata = pdas4_filtered2)
toc()

