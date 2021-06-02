

# Packages ---------------------------------------------------------

library(dplyr)
library(sf)
library(caret)
library(data.table)
library(rgdal)
library(lwgeom)

library(party)
library(mlbench)

library(doParallel)
library(foreach)
library(tictoc)

library(readr)
library(tibble)


# S24 ----------------------------------------------------------

# load data -----------------------------------------------------

link_one <- "D:/STW PDAS/Phase 2 datasets/updated_building_polygons/"
link_nbp <- "D:/STW PDAS/Phase 2 datasets/sprint_4_props_with_atts/"
link_e <- "D:/STW PDAS/Phase 2 datasets/Postcode/"
link_da <- "D:/STW PDAS/Phase 2 datasets/Sewerage_Drainage_Area/"
s24_assetbase_link <- "D:/STW PDAS/s24_phase_2/" # change this when you have the assetbase

the_postcode_2 <- st_read(paste0(link_e,"the_postcode_2.shp"))
props_with_county_and_postcode <- read.csv(paste0(link_e,"props_with_county_and_postcode.csv"))
Sewerage_Drainage_Area <- st_read(paste0(link_da,"Sewerage_Drainage_Area.shp"))
s24_assetbase <- st_read(paste0(s24_assetbase_link,"merged_linestrings_s24_22012021.shp")) # change this when you have the assetbase
dlx <- readRDS(paste0(link_one,"postcode_age.rds"))
modal_atts <- st_read("D:/STW PDAS/ModalandNearestNeighbour/ModalAttributes/ModalAttributes_Merged.shp")
fids_props <- fread("D:\\STW PDAS\\Phase 2 datasets\\Flag overlap\\new_fids_all2.csv") # added
year_lookup <- fread("D:/STW PDAS/Phase 2 datasets/updated_building_polygons/year_lookup_updated.csv")



# call funcs

get_nearest_features <- function(x, y, pipeid, id, feature) {
  
  # pipeid is id of x
  # id is id of y
  # feature is attribute to be added to x from y
  # function needs some improvement
  
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
  x$x_index <- nearest
  y2 <- y[,c("id", "feature", "rowguid")]
  st_geometry(y2) <- NULL
  x2 <- left_join(x, as.data.frame(y2), by = c("x_index" = "rowguid"))
  output <- x2
  
  # revert column names
  colnames(output)[which(names(output) == "id.x")] <- pipeid
  colnames(output)[which(names(output) == "id.y")] <- id
  colnames(output)[which(names(output) == "feature")] <- feature
  return(output)
}
get_nearest_features_xycentroid <- function(x, y, pipeid, id, feature) {
  
  # pipeid is id of x
  # id is id of y
  # feature is attribute to be added to x from y
  # function needs some improvement
  
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
  
  # convert both x and y to centroid
  x <- st_centroid(x)
  y <- st_centroid(y)
  
  # get nearest features
  nearest <- st_nearest_feature(x,y)
  x$x_index <- nearest
  y2 <- y[,c("id", "feature", "rowguid")]
  st_geometry(y2) <- NULL
  x2 <- merge(x, as.data.frame(y2), by.x = "x_index", by.y = "rowguid")
  output <- x2
  
  # revert column names
  colnames(output)[which(names(output) == "id.x")] <- pipeid
  colnames(output)[which(names(output) == "id.y")] <- id
  colnames(output)[which(names(output) == "feature")] <- feature
  return(output)
}
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
remove_anomalies_cat_pred <- function(x,y,col){
  #for (y in 1:length(x))
  # x is the model training data
  # y is the new data you want to predict on
  # j is the number of columns
  x_col <- x[,col] %>% unlist()
  y_col <- y[,col] %>% unlist()
  
  "%ni%" <- Negate("%in%")
  y_filtered <- 
    y_filtered <- y[match(unlist(y[,col]), x_col, nomatch = 0) == 0, ]
  y_filtered_2 <- y %>% anti_join(y_filtered, by = col)
  return(y_filtered_2)
}
divide_and_conquer <- function(x,your_func = your_func, func_arg = func_arg, x_id,y_id,y_var, splits = 100,each = 1,verbose = F){
  my_split <- rep(1:splits,each = each, length = nrow(x))
  my_split2 <- mutate(x, my_split = my_split)
  my_split3 <- tidyr::nest(my_split2, - my_split) 
  models <- purrr::map(my_split3$data, ~ your_func(., func_arg,x_id,y_id,y_var)) #lengthy processing time - 6.5 hrs
  toc()
  bind_lists <- plyr::rbind.fill(models)
  return(bind_lists)
}
endorstart <- function(x, type = "end") {
  if(type == "end") {
    return(st_endpoint(x))
  } else {
    return(st_startpoint(x))
  }
}
giveneighattrs2 <- function(s24s, n24s) {
  
  if (dim(s24s)[1] == 0) {
    return(NULL)
  }
  
  s24s 
  
  n24s <- n24s %>% 
    mutate(index = row_number())
  
  a <- st_endpoint(s24s) %>% 
    st_buffer(2.5, nQuadSegs = 1, endCapStyle = "SQUARE") %>% 
    st_intersects(n24s)
  
  b <- st_startpoint(s24s) %>% 
    st_buffer(2.5, nQuadSegs = 1, endCapStyle = "SQUARE") %>% 
    st_intersects(n24s)
  
  # See if there are connectinos on either the end point or the start point
  # Do a join onto those points then using the VALUE key
  # Also record if it:s joining on the start or end point
  
  mapply(function(a, b) c("endpoint" = a[1], "startpoint" = b[1], "value" = c(a,b)[1]) %>% 
           bind_rows, a, b, SIMPLIFY = F) %>% 
    bind_rows() %>%
    mutate(connectionend = ifelse(!is.na(endpoint), "endpoint",
                                  ifelse(!is.na(startpoint), "startpoint", "none"))) %>% 
    select(-c(endpoint, startpoint)) %>% 
    bind_cols(s24s) %>% 
    left_join(n24s %>% 
                st_drop_geometry() %>% 
                select(c("n2_D", "n2_M", "n2_Y",
                         "Tag", "index")) %>% 
                rename(value = index),
              by = "value")
}
get_intersection <- function(x,y,x_row.id=x_row.id,y_col.id=y_col.id){
  
  # group.id = usally the uid on the lhs, what to group the the object by and subsequently find the modal value for that group
  # modal = find the mode value of this var
  
  myintersect <- st_intersects(x,y)
  myintersect <- as.data.frame(myintersect)
  
  # convert myintersect column names
  myintersect$x_row.id <- myintersect$row.id
  myintersect$y_col.id <- myintersect$col.id
  myintersect <- myintersect[,c("x_row.id","y_col.id")]
  
  # add new UIDs
  x$x_row.id <- seq.int(nrow(x))
  y$y_col.id <- seq.int(nrow(y))
  
  # Get the id of the buildings in buffer
  
  x_col_id <- left_join(myintersect, as.data.frame(x), by = "x_row.id") %>% select(-geometry)
  df_combine <- left_join(x_col_id, as.data.frame(y), by = "y_col.id") %>% select(-geometry)
  
  return(df_combine)
}
divide_and_conquer2 <- function(x,your_func = your_func, func_call = func_call, splits = 100,each = 1){
  my_split <- rep(1:splits,each = each, length = nrow(x))
  my_split2 <- mutate(x, my_split = my_split)
  my_split3 <- tidyr::nest(my_split2, - my_split) 
  models <- purrr::map(my_split3$data, ~ your_func(., func_call))
  bind_lists <- plyr::rbind.fill(models)
  return(bind_lists)
}
get_nearest_features_using_grid <- function(listofgrids,columns = columns){
  for(i in 1:length(listofgrids[[1]])){
    message(paste0("Grid:", i))
    test1 <- listofgrids[[1]][[i]] %>% st_sf() %>% st_set_crs(27700)
    test1$pipe_uq_id <- seq.int(nrow(test1))
    test2 <- listofgrids[[2]][[i]] %>% st_sf() %>% st_set_crs(27700)
    
    # get_nearest_features adding 2 new columns to x
    test_res <- test2[st_nearest_feature(test1,test2), columns]
    added_col <- as.data.frame(test_res[,columns])
    
    # merge the obj above
    added_col <- added_col[,columns]
    test_result <- cbind(as.data.frame(test1),added_col)
    final_test_result <- plyr::rbind.fill(test_result)
    
    if (i == 1) {
      final.out <- final_test_result
    } else{
      final.out <- plyr::rbind.fill(final.out, final_test_result)
    }
  }
  return(final.out)
}

# transform before sending to model -----------------------------------------------------------------------

# add the property type
source("D:/STW PDAS/Git/pdas_mapping/pipe-depth-from-lidar-images/scripts/4 - Subsetter.R")
# tic()
# listofgrids <- createGrid(mainshapes = s24_assetbase, phase_2_props_with_age, XCount = 10, YCount = 10,
#                           buffer = 300, mainexpand = F, extrasexpand = T, removeNull = T, report = T)
# toc()
# 
# tic()
# s24_assetbase_fid <- get_nearest_features_using_grid(listofgrids, columns = c("fid","P_Type","res_buil_1"))
# toc()
# 
# s24_assetbase_fid_trimmed <- s24_assetbase_fid %>% 
#   select(type, positin, nrst_swr_t,  nrst_swr_d, is_gnrt,
#          propgrp, ActualX, ActualY, drwng__, directn,
#          pipe_id, new_length,fid, P_Type, res_buil_1, geometry) %>% st_sf() %>% st_set_crs(27700)

fids_props <- fids_props %>% 
  mutate(P_Type = ifelse(P_Type == "1-detached","Detached",
                         ifelse(P_Type == "2-semi","Semi",
                                ifelse(P_Type == "3-terrace","Terraced","Terraced"))),
         P_Type = as.factor(P_Type)) # fix P-Type
s24_assetbase_fid_trimmed <- s24_assetbase %>% 
  left_join(fids_props[,c("fid","P_Type")], by = "fid") # added

# rename columns and clean some vars
s24_assetbase <- s24_assetbase_fid_trimmed
s24_assetbase$pipe_uq_id <- seq.int(nrow(s24_assetbase))
st_crs(s24_assetbase) <- 27700
s24_assetbase <- s24_assetbase %>% 
  rename(PURPOSE = nrst_swr_t) %>% 
  rename(PROPERTY_TYPE = P_Type) %>% 
  mutate(pipe_uq_id = row_number(),PURPOSE = ifelse(PURPOSE == "Single","S",as.character(PURPOSE))) %>% # convert Single to Surface
  mutate(PROPERTY_TYPE = ifelse(PROPERTY_TYPE == "Detatched","Detached",as.character(PROPERTY_TYPE))) # fix typo

# add DAS and get Postcode year
Sewerage_Drainage_Area <- st_transform(Sewerage_Drainage_Area,crs = 27700)
s24_da <- get_intersection(s24_assetbase,Sewerage_Drainage_Area[,c("OBJECTID","ID_DA_CODE")],"pipe_uq_id","OBJECTID") # add DA
s24_da <- s24_da %>% select(-x_row.id, -y_col.id) %>% distinct()
s24_da <- s24_da[!duplicated(s24_da$pipe_uq_id), ]
dlx <- dlx[,c("Postcod","Year")]
dlx <- st_transform(dlx, 27700)
s24_da0 <-  s24_da %>% # as.data.frame() %>% select(-geometry) %>%
  right_join(s24_assetbase[,c("pipe_uq_id")], by = "pipe_uq_id") %>% 
  st_as_sf(crs = 27700) # add the multilinestring back
s24_da_pc <- divide_and_conquer(s24_da0,get_nearest_features,dlx,"pipe_uq_id","Postcod","Year") # add postcode and Year

# add modal atts - n2_m, n2_D and n2_Y ----------------------------------------------
# cast to linestrings and get the ones connected to n24s
s24_assetbase_sample <- s24_assetbase %>% 
  st_cast("LINESTRING") %>% 
  select(fid,drwng__,pipe_uq_id) 
st_crs(s24_assetbase_sample) <- 27700
st_crs(modal_atts) <- 27700
rownames(s24_assetbase_sample) <- 1:nrow(s24_assetbase_sample)
which_is_nearest <- giveneighattrs2(s24_assetbase_sample,modal_atts) %>% as.data.frame()
which_is_nearest <- which_is_nearest %>% # mutate(id_1 = row_number()) %>% 
  st_sf() %>% st_set_crs(27700)

which_is_nearest <- which_is_nearest %>% filter(connectionend == "endpoint" | connectionend == "startpoint") %>% mutate(id_1 = row_number())
my_n24 <- modal_atts %>% 
  mutate(id_1 = row_number()) %>% 
  as.data.frame()
my_n24 <- my_n24 %>% st_sf %>% st_set_crs(27700)
the_int <- st_intersects(which_is_nearest,my_n24) %>% as.data.frame()

my_ans <- which_is_nearest %>% 
  left_join(the_int, by = c("id_1"="row.id"))
my_ans <- my_ans[!duplicated(my_ans$id_1), ] %>% as.data.frame() %>% 
  select(fid,drwng__,pipe_uq_id,id_1,Tag,n2_M,n2_D,n2_Y, col.id) %>% distinct() %>% 
  filter(!is.na(n2_M),!is.na(n2_D),!is.na(n2_Y))

# add the multilinestring back
s24_da_pc <- s24_da_pc %>% as.data.frame() %>% select(-geometry) %>% 
  left_join(s24_assetbase[,c("pipe_uq_id","geometry")], by = "pipe_uq_id") %>% 
  st_as_sf(crs = 27700)  
s24_da_pc_ma_2 <- s24_da_pc %>% left_join(my_ans[,c("pipe_uq_id", "id_1", "Tag", "n2_M", "n2_D", "n2_Y")], by = "pipe_uq_id")

# modify column data type
s24_da_pc_ma_2 <- s24_da_pc_ma_2[!duplicated(s24_da_pc_ma_2$pipe_uq_id), ]  %>% 
  mutate(n2_M = as.character(n2_M),
         n2_D = as.integer(as.character(n2_D)),
         n2_Y = as.integer(as.character(n2_Y))) 

# st_write(s24_da_pc_ma_2,"D:/STW PDAS//s24_phase_2/s24 n2 attrs.shp")

# test
# which_is_nearest %>% filter(drwng__ == "osgb1000025441256")
# my_ans %>% filter(drwng__ == "osgb1000025441256")
# s24_da_pc_ma_2 %>% filter(drwng__ == "osgb1000025441256")

# fix diameter
diameterclasses <-c(0, 100, 150, 225, 300, 375, 450 ,525, 600, 675, 750, 825,
                    900, 975, 1050, 1200, 1350, 1500)

s24_da_pc_ma_3 <- rollmeTibble(df1 = s24_da_pc_ma_2,
                               values = diameterclasses,
                               col1 = "n2_D",
                               method = 'nearest') %>%
  select(-n2_D, -band, -merge) %>% 
  rename(n2_D = value)


# fix material
unband_band <- read.csv("D:/STW PDAS\\old stuff\\Assets - Rulesets - Material Banding Table v2.csv")
s24_da_pc_ma_4 <- s24_da_pc_ma_3 %>%
  left_join(unique(unband_band[,c(1,2)]),by = c("n2_M"="MaterialTable_UnbandedMaterial")) %>% 
  dplyr::mutate(n2_M = (n2_M = as.factor(MaterialTable_BandedMaterial)))

# add County
s24_da_pc_ma_5 <- s24_da_pc_ma_4 %>% 
  left_join(props_with_county_and_postcode[,c("fid","County")], by = "fid") %>% 
  st_sf() %>% st_set_crs(27700)
no_county <- s24_da_pc_ma_5 %>% filter(is.na(County)) %>% st_sf() %>% st_set_crs(27700)
s24_da_pc_ma_5 <- s24_da_pc_ma_5 %>% filter(!is.na(County)) %>% st_sf() %>% st_set_crs(27700)

# infill Counties with NA using st_nearest_feature
no_county_get_county <- s24_da_pc_ma_5[st_nearest_feature(no_county,s24_da_pc_ma_5), "County"]
no_county$County <- no_county_get_county$County

# bind and convert back to dataframe
s24_da_pc_ma_5 <- plyr::rbind.fill(s24_da_pc_ma_5,no_county) %>% as.data.frame()

# remove some objects from the env
rm(s24_da)
rm(s24_assetbase_sample)
rm(phase_2_props_with_age)
rm(s24_da_pc)
rm(s24_da_pc_ma_2)
rm(s24_da_pc_ma_3)
rm(s24_da_pc_ma_4)

unique(s24_da_pc_ma_4$n2_M)

# final janitorial touch on the master prediction data -------------------------------------------------------------------------
s24_da_pc_ma_5 <- s24_da_pc_ma_5 %>% distinct() # MASTER DATA


s24_da_pc_ma_5$n2_M[s24_da_pc_ma_5$n2_M == ""] <- NA
s24_da_pc_ma_6 <- s24_da_pc_ma_5 %>% 
  mutate(n2_M = as.character(n2_M)) %>% 
  as_tibble() %>% 
  filter(!is.na(PURPOSE),!is.na(n2_Y),!is.na(n2_M),n2_M != "",n2_D != 0,Year != 0,
         !is.na(PROPERTY_TYPE), !is.na(County)) %>% # Remove any diameters with 0 counts
  mutate(n2_D = as.factor(n2_D),
         PURPOSE = as.factor(PURPOSE),
         n2_M = as.factor(n2_M),
         n2_Y = as.numeric(n2_Y),
         PROPERTY_TYPE = as.factor(PROPERTY_TYPE),
         County = as.factor(County),
         DAS = as.factor(ID_DA_CODE)) %>% 
  select(-MaterialTable_BandedMaterial) 
unique(s24_da_pc_ma_6$n2_M)

s24_da_pc_ma_6 %>% 
  as_tibble() %>% 
  sapply(function(x) sum(is.na(x)))

summary(s24_da_pc_ma_6)


## save and load image
# save.image("G:/s24_d_m_y.rdata")
# load("G:/s24_d_m_y.rdata")

rm(s24_assetbase_sample)
rm(s24_assetbase_fid_trimmed)
rm(s24_da)
rm(fids_props)


# Load model 1--------------------------------------------------------------

rf.fits24dia_DAS <- read_rds("D:/STW PDAS/Git/pdas_mapping/assign-attributes/S24diam_model DAS.RDS")
# remove missing factors and predict
s24_da_pc_ma_7 <- remove_anomalies_cat_pred(rf.fits24dia_DAS$trainingData,s24_da_pc_ma_6,"DAS")
rf.pred1_s24 <- predict(rf.fits24dia_DAS, newdata = s24_da_pc_ma_7)


# join back to master data and prep for model 2
result_pred_1_s24 <- cbind(Confidence = "A",DIAMETE = rf.pred1_s24,MATERIA = "VC",s24_da_pc_ma_7) # model 1 result
pred_2_s24 <- s24_da_pc_ma_5 %>% 
  anti_join(result_pred_1_s24, by = "pipe_uq_id") %>% 
  distinct()

pred_2_s24$n2_M[pred_2_s24$n2_M == ""] <- NA
pred_2_s24 <- pred_2_s24 %>% 
  mutate(n2_M = as.character(n2_M)) %>% 
  as_tibble() %>% 
  filter(!is.na(PURPOSE),!is.na(PROPERTY_TYPE),!is.na(n2_Y),!is.na(n2_M),n2_M != "",n2_D != 0,Year != 0) %>% # Remove any diameters with 0 counts
  mutate(n2_D = as.factor(n2_D),
         PURPOSE = as.factor(PURPOSE),
         n2_M = as.factor(n2_M),
         n2_Y = as.numeric(n2_Y),
         PROPERTY_TYPE = as.factor(PROPERTY_TYPE),
         County = as.factor(County),
         DAS = as.factor(ID_DA_CODE)) %>% 
  select(-MaterialTable_BandedMaterial) 

pred_2_s24 %>% 
  as_tibble() %>% 
  sapply(function(x) sum(is.na(x)))

summary(pred_2_s24)

# Load model 3 for prediction number 2--------------------------------------------------------------

rf.fitS24dia_County_noY <- read_rds("D:/STW PDAS/Git/pdas_mapping/assign-attributes/S24diam_model County noY.RDS")
rf.pred2_s24 <- predict(rf.fitS24dia_County_noY, newdata = pred_2_s24)



# join back to master data and prep for model 4
result_pred_2_s24 <- cbind(Confidence = "B",DIAMETE = rf.pred2_s24,MATERIA = "VC",pred_2_s24) # model 3 result
pred_3_s24 <- s24_da_pc_ma_5 %>% 
  anti_join(result_pred_1_s24, by = "pipe_uq_id") %>%
  anti_join(result_pred_2_s24, by = "pipe_uq_id") %>%
  #anti_join(result_pred_3_s24, by = "pipe_uq_id") %>%
  distinct()

# Load model 4 for prediction number 3 --------------------------------------------------------------
# join back to master data
result_pred_3_s24 <- cbind(Confidence = "D",DIAMETE = "150",MATERIA = "VC",pred_3_s24) # model 4 result

result_pred_3_s24 <- result_pred_3_s24 %>% mutate(n2_D = as.factor(n2_D),
                                                  PURPOSE = as.factor(PURPOSE),
                                                  n2_M = as.factor(n2_M),
                                                  n2_Y = as.numeric(n2_Y),
                                                  PROPERTY_TYPE = as.factor(PROPERTY_TYPE),
                                                  County = as.factor(County),
                                                  DAS = as.factor(ID_DA_CODE)) %>%
  select(-MaterialTable_BandedMaterial)


# merge all together    # changed all the way down
results_s24 <- rbind(result_pred_1_s24,result_pred_2_s24,result_pred_3_s24) %>% 
  select(pipe_uq_id, Confidence, DIAMETE,  MATERIA) %>% 
  left_join(s24_assetbase, by = "pipe_uq_id") %>% 
  distinct()


summary(results_s24)

# rename building polygon age col
results <- rbind(result_pred_1,result_pred_2,result_pred_3,result_pred_4,result_pred_5) %>%
  #select(Confidence, DIAMETE, MATERIA, n2_D, drwng__, fid, propgrp, PROPERTY_TYPE, PURPOSE, Postcod, County, DAS) %>%
  # select(pipe_uq_id, Confidence, DIAMETE,  MATERIA) %>%
  left_join(fids_props[,c("fid","age")], by = "fid") # %>% # changed
# select(-path)


summary(results)

# rename building polygon age column and edit the yearlaid column to median 
results <- results %>%
  dplyr::rename(YEARLAI = age) %>%
  select(pipe_uq_id, PROPERTY_TYPE, PURPOSE, Confidence,MATERIA,YEARLAI, 
         drwng__, DIAMETE, County, ID_DA_CODE,  propgrp, fid) %>% 
  left_join(year_lookup[,c("res_buil_1", "MEDIAN_YEARLAID")], by = c("YEARLAI" = "res_buil_1")) %>% 
  select(-YEARLAI) %>% 
  dplyr::rename(YEARLAI = MEDIAN_YEARLAID) %>% 
  left_join(s24_assetbase[,c("pipe_uq_id")]) %>% 
  st_sf() %>% st_set_crs(27700) 

# infill the missing yearlaid rows and merge them back together
results_withna <- results %>% filter(is.na(YEARLAI))
results_withoutna <- results %>% filter(!is.na(YEARLAI))
tic()
listofgrids <- createGrid(mainshapes = results_withna, results_withoutna, XCount = 5, YCount = 5,
                          buffer = 300, mainexpand = F, extrasexpand = T, removeNull = T, report = T)
toc()

tic()
results_withna_yearlaid <- get_nearest_features_using_grid(listofgrids, columns = c("pipe_uq_id","YEARLAI"))
toc()
my_columns <- c("pipe_uq_id", "PROPERTY_TYPE", "PURPOSE", "Confidence", "MATERIA", "drwng__", 
                "DIAMETE", "County", "ID_DA_CODE", "propgrp", "fid", "YEARLAI", "geometry")
results_withoutna$index <- seq.int(nrow(results_withoutna))
st_geometry(results_withoutna) <- NULL
results_withna_yearlaid2 <- results_withna_yearlaid %>% 
  select(-YEARLAI) %>% 
  left_join(results_withoutna[,c("index","YEARLAI")], by = "index")
results_withna_yearlaid2 <- results_withna_yearlaid2[,my_columns] %>% st_sf() %>% st_set_crs(27700)
results_withoutna <- results %>% filter(!is.na(YEARLAI))
results <- rbind(results_withoutna,results_withna_yearlaid2)
results <- results[!duplicated(results$geometry),]

summary(results)
st_write(results,"D:/STW PDAS/s24_phase_2/s24_lines_with_attrs_14012021.shp")

# save.image("D:/STW PDAS/Phase 2 datasets/s24_d_m_y_04122020.rdata")
# load("D:/STW PDAS/Phase 2 datasets/s24_d_m_y_04122020.rdata")
