
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

# raw S24 data, nothing has been infilled on this as far as I'm aware
s24path1 <- "C:/Users/TOsosanya/Desktop/STW PDAS 2/ModalandNearestNeighbour/S24withnn/S24withnn_merged.shp"
s24Path2 <- "C:/Users/TOsosanya/Desktop/STW PDAS 2/ModalandNearestNeighbour/s24_with_property_type.csv"
s241 <- st_read(s24path1, stringsAsFactors = F)
s242 <- read.csv(s24Path2)
#st_geometry(pdas1) <- NULL
#pdas <- pdas2 %>% left_join(pdas1[,c(1,10)], by = "PIPEID")
s242 <- s242 %>% select(Tag,Length,System,dom_prop80) %>% 
  dplyr::rename(PROPERTY_TYPE = dom_prop80) %>% # no pipegroup
  dplyr::rename(PIPEID = Tag) %>% 
  dplyr::rename(LENGTH = Length) %>% 
  dplyr::rename(PURPOSE = System) %>% 
  dplyr::mutate(PIPEID = as.character(PIPEID))
s24 <- s241 %>% 
  dplyr::left_join(s242[,c("PIPEID","PROPERTY_TYPE")], by = "PIPEID") %>% 
  dplyr::filter(PROPERTY_TYPE != "") %>% 
  dplyr::mutate(DIAMETE = as.integer(DIAMETE))
summary(s24)


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
  print(ggplot(s24 %>% 
                 filter(PURPOSE == !!purposes)) +
          geom_bar(aes(x = MATERIA)) +
          ggtitle(paste0("Type of pipe: ",purposes)) +
          ylab("Number of pipes") + 
          xlab("Pipe Material"))
}

# Diameter
for(purposes in c("C","F","S")) {
  print(ggplot(s24 %>% 
                 filter(PURPOSE == !!purposes)) +
          geom_histogram(aes(x = DIAMETE)) +
          ggtitle(paste0("Type of pipe: ",purposes)) + 
          xlim(1,250))
}

# Get counts
s24 %>% 
  st_drop_geometry() %>% 
  count(PURPOSE, DIAMETE)

# Filter out those that don't have neighbours with all attributes

noNAs <- s24 %>% 
  filter(!is.na(n2_D)) %>% 
  filter(!is.na(n2_M)) %>% 
  filter(!is.na(n2_Y))

# Get rid of that damn geometry
# Select the "important cols"
noNAssmall <- noNAs %>%
  st_drop_geometry() %>% 
  select(PIPEID, PURPOSE, County,
         MATERIA, DIAMETE, PROPERTY_TYPE,
         n2_D, n2_M, n2_Y)  %>% 
  mutate(n2_D = as.numeric(n2_D),
         DIAMETE = as.numeric(DIAMETE))


# Nearest Dia -------------------------------------------------------------

# This is required to run the ML.
# It will assign the NEAREST DIAMETER CLASS TO EACH PIPE

# These are the decided classes
diameterclasses <-c(0, 100, 150, 225, 300, 375, 450 ,525, 600, 675, 750, 825,
                    900, 975, 1050, 1200, 1350, 1500)

# This will roll them on
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


# Filter out any with 0's in DIAMETE (S24) or n2_D (nearest neighbour)
n24s_dia <- n24_reduced %>%
  filter(DIAMETE != 0, DIAMETE <= 225) %>% 
  filter(n2_D != 0)




# Machine Learning --------------------------------------------------------

# Your data set
# Set everything up into the right data formats
nonafactors3 <- n24s_dia %>% 
  as_tibble() %>% 
  mutate(DIAMETE = as.factor(DIAMETE),
         n2_D = as.factor(n2_D),
         PURPOSE = as.factor(PURPOSE),
         n2_M = as.factor(n2_M),
         n2_Y = as.numeric(n2_Y),
         County = as.factor(County), 
         PROPERTY_TYPE = as.factor(PROPERTY_TYPE))

# ensure worcestershire is spelt the same as that of pdas
nonafactors3$County <- gsub("Worcestershire Gloucestershire","Worcestershire/Gloucestershire",nonafactors3$County)

#split data for ML
set.seed(5)
train.base2 <- sample(1:nrow(nonafactors3),(nrow(nonafactors3)*0.75),replace = FALSE)
traindata.base2 <- nonafactors3[train.base2, ]
testdata.base2 <- nonafactors3[-train.base2, ]

# If you want to process in parallel do the following
cl <- makePSOCKcluster(3)
registerDoParallel(cl)

# Train the model
rf.fit41 <- train(DIAMETE ~ n2_D + n2_M + n2_Y + PURPOSE + County + PROPERTY_TYPE,
                  tuneLength = 1,
                  data = traindata.base2,
                  method = "ranger",
                  trControl = trainControl(
                    method = "cv",
                    allowParallel = TRUE,
                    number = 5,
                    verboseIter = T
                  ))

# Predict and test the model
tic()
rf.pred2 <- predict(rf.fit41, newdata = testdata.base2)
toc()
caret::confusionMatrix(rf.pred2, testdata.base2$DIAMETE) # DA and ppt_type (80.1% and NIR 74%)

# variable importance
rf.fit2 <- cforest(DIAMETE ~ n2_D + n2_M + n2_Y + PURPOSE + County + PROPERTY_TYPE, 
                   data = traindata.base2, controls = cforest_unbiased(ntree = 50, mtry = 4))
rev(sort(varimp(rf.fit2)))


# Output Model ------------------------------------------------------------

rf.fit41 %>% 
  write_rds("G:/Desktop/STW PDaS/Git/pdas_mapping/assign-attributes/S24diam_model County.RDS")




# Load model --------------------------------------------------------------

rf.fit41 <- read_rds("G:/Desktop/STW PDaS/Git/pdas_mapping/assign-attributes/S24diam_model County.RDS")

rf.preds24 <- predict(rf.fit41, newdata = testdata.base2)
caret::confusionMatrix(rf.preds24, testdata.base2$DIAMETE)


# Load and Predict Notional assets -------------------------------------------

s24Path3 <- "C:/Users/TOsosanya/Desktop/STW PDAS 2/ModalandNearestNeighbour/Notional Assets for ML/sprint_5_notionals_merged/sprint_5_notionals.shp"
s243 <- st_read(s24Path3, stringsAsFactors = F)
st_geometry(s243) <- NULL

s243_filtered <- s243 %>%  
  as_tibble() %>% 
  dplyr::filter(ConNghY <= 1937) %>% 
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
s243_filtered2 <- rollmeTibble(df1 = s243_filtered,
                                values = diameterclasses,
                                col1 = "n2_D",
                                method = 'nearest') %>%
  select(-n2_D, -band, -merge) %>% 
  rename(n2_D = value) %>% 
  dplyr::filter(n2_D != 0) %>% 
  dplyr::mutate(n2_D = as.factor(n2_D))

## band the n2_M
s243_filtered2 <- s243_filtered2 %>%
  left_join(unique(unband_band[,c(1,2)]),by = c("n2_M"="MaterialTable_UnbandedMaterial")) %>% 
  dplyr::mutate(n2_M = (n2_M = as.factor(MaterialTable_BandedMaterial))) %>% 
  dplyr::filter(!is.na(n2_M)) %>% 
  dplyr::select(-MaterialTable_BandedMaterial)
unique(s243_filtered2$n2_M)
s243_filtered2$n2_M[s243_filtered2$n2_M == ""] <- NA

## remove the NA
s243_filtered2 <- s243_filtered2 %>% 
  na.omit()
summary(s243_filtered2)

## predict diameter
tic()
rf.prednotional_s24diam <- predict(rf.fit41, newdata = s243_filtered2)
toc()



