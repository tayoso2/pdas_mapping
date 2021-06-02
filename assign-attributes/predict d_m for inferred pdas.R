# Packages ---------------------------------------------------------

library(dplyr)
library(sf)
library(caret)
library(data.table)
library(rgdal)

library(party)
library(mlbench)

library(doParallel)
library(foreach)
library(tictoc)

library(readr)
library(tibble)
library(lwgeom)

## load image
# save.image("D:/STW PDAS/Phase 2 datasets/pdas_d_m_y.rdata")
# load("D:/STW PDAS/Phase 2 datasets/pdas_d_m_y.rdata")

# load data ----------------------------------------------------------

link_one <- "D:/STW PDAS/Phase 2 datasets/updated_building_polygons/"
link_e <- "D:/STW PDAS/Phase 2 datasets/Postcode/"
link_da <- "D:/STW PDAS/Phase 2 datasets/Sewerage_Drainage_Area/"
pdas_assetbase_link <- "D:/STW PDAS/Git/pdas_mapping/segment_pdas_s24/Segment Dataflow_nh_fix/fold_out" # change this when you have the assetbase

the_postcode_2 <- st_read(paste0(link_e,"the_postcode_2.shp"))
props_with_county_and_postcode <- read.csv(paste0(link_e,"props_with_county_and_postcode.csv"))
Sewerage_Drainage_Area <- st_read(paste0(link_da,"Sewerage_Drainage_Area.shp"))
pdas_assetbase <- st_read(paste0(pdas_assetbase_link,"/merged_linestrings_pdas.shp")) # change this when you have the assetbase
dlx <- readRDS(paste0(link_one,"postcode_age.rds"))
modal_atts <- st_read("D:/STW PDAS/ModalandNearestNeighbour/ModalAttributes/ModalAttributes_Merged.shp")
fids_props <- fread("D:\\STW PDAS\\Phase 2 datasets\\Flag overlap\\new_fids_all2.csv") # added
year_lookup <- fread("D:/STW PDAS/Phase 2 datasets/updated_building_polygons/year_lookup_updated.csv")

# call funcs -----------------------------------------------------------

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
  x$x_pipe_uq_id <- nearest
  y2 <- y[,c("id", "feature", "rowguid")]
  st_geometry(y2) <- NULL
  x2 <- left_join(x, as.data.frame(y2), by = c("x_pipe_uq_id" = "rowguid"))
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
  x$x_pipe_uq_id <- nearest
  y2 <- y[,c("id", "feature", "rowguid")]
  st_geometry(y2) <- NULL
  x2 <- merge(x, as.data.frame(y2), by.x = "x_pipe_uq_id", by.y = "rowguid")
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
    mutate(pipe_uq_id = row_number())
  
  a <- st_endpoint(s24s) %>%
    st_buffer(2.5, nQuadSegs = 1, endCapStyle = "SQUARE") %>%
    st_intersects(n24s)
  
  b <- st_startpoint(s24s) %>%
    st_buffer(2.5, nQuadSegs = 1, endCapStyle = "SQUARE") %>%
    st_intersects(n24s)
  
  # See if there are connectinos on either the end point or the start point
  # Do a join onto those points then using the VALUE key
  # Also record if it;s joining on the start or end point
  
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
                         "Tag", "pipe_uq_id")) %>%
                rename(value = pipe_uq_id),
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
# listofgrids <- createGrid(mainshapes = pdas_assetbase, phase_2_props_with_age, XCount = 5, YCount = 5,
#                           buffer = 300, mainexpand = F, extrasexpand = T, removeNull = T, report = T)
# toc()

# tic()
# pdas_assetbase_fid <- get_nearest_features_using_grid(listofgrids, columns = c("fid","P_Type","res_buil_1"))
# toc()
# 
# pdas_assetbase_fid_trimmed <- pdas_assetbase_fid %>%
#   select(type, positin, nrst_swr_t,  nrst_swr_d, is_gnrt,
#          propgrp, ActualX, ActualY, drwng__, directn,
#          pipe_id, new_length, fid, P_Type, res_buil_1, geometry) %>% st_sf() %>% st_set_crs(27700) # commented

fids_props <- fids_props %>% 
  mutate(P_Type = ifelse(P_Type == "1-detached","Detached",
                         ifelse(P_Type == "2-semi","Semi",
                                ifelse(P_Type == "3-terrace","Terraced","Terraced"))),
         P_Type = as.factor(P_Type)) # fix P-Type
pdas_assetbase_fid_trimmed <- pdas_assetbase %>% 
  left_join(fids_props[,c("fid","P_Type")], by = "fid") # added


# rename columns and clean some vars
pdas_assetbase <- pdas_assetbase_fid_trimmed
pdas_assetbase$pipe_uq_id <- seq.int(nrow(pdas_assetbase))
st_crs(pdas_assetbase) <- 27700
pdas_assetbase <- pdas_assetbase %>%
  rename(PURPOSE = nrst_swr_t) %>%
  rename(PROPERTY_TYPE = P_Type) %>%
  mutate(PURPOSE = ifelse(PURPOSE == "Single","S",as.character(PURPOSE))) %>% # convert Single to Surface
  mutate(PROPERTY_TYPE = ifelse(PROPERTY_TYPE == "Detatched","Detached",as.character(PROPERTY_TYPE))) # fix typo

# add DAS and get Postcode year
Sewerage_Drainage_Area <- st_transform(Sewerage_Drainage_Area,crs = 27700)
pdas_da <- get_intersection(pdas_assetbase,Sewerage_Drainage_Area[,c("OBJECTID","ID_DA_CODE")],"pipe_uq_id","OBJECTID") # add DA
pdas_da <- pdas_da %>% select(-x_row.id, -y_col.id) %>% distinct()
pdas_da <- pdas_da[!duplicated(pdas_da$pipe_uq_id), ]
dlx <- dlx[,c("Postcod","Year")]
dlx <- st_transform(dlx, 27700)
pdas_da <-  pdas_da %>% # as.data.frame() %>% select(-geometry) %>%
  left_join(pdas_assetbase[,c("pipe_uq_id")], by = "pipe_uq_id") %>%
  st_as_sf(crs = 27700) # add the multilinestring back
pdas_da_pc <- get_nearest_features(pdas_da,dlx,"pipe_uq_id","Postcod","Year") # add postcode and Year

# add modal atts: n2_m, n2_D and n2_Y ----------------------------------------------------
pdas_assetbase_sample <- pdas_assetbase %>%
  st_cast("LINESTRING") %>%
  select(fid,drwng__,pipe_uq_id) %>% mutate(id_0 = row_number())

st_crs(pdas_assetbase_sample) <- 27700
st_crs(modal_atts) <- 27700
which_is_nearest <- giveneighattrs2(pdas_assetbase_sample,modal_atts) %>% as.data.frame()
which_is_nearest <- which_is_nearest %>% filter(connectionend == "endpoint" | connectionend == "startpoint") %>%
  mutate(id_0 = row_number()) %>% st_sf() %>% st_set_crs(27700)

st_crs(modal_atts) <- 27700

my_n24 <- modal_atts %>%
  mutate(id_0 = row_number()) %>%
  as.data.frame()

my_n24 <- my_n24 %>% st_sf %>% st_set_crs(27700)
the_int <- st_intersects(which_is_nearest,my_n24) %>% as.data.frame()

my_ans <- which_is_nearest %>%
  left_join(the_int, by = c("id_0"="row.id"))
my_ans <- my_ans[!duplicated(my_ans$id_0), ] %>% as.data.frame() %>%
  select(fid,drwng__,pipe_uq_id,id_0,Tag,n2_M,n2_D,n2_Y, col.id) %>% distinct() %>%
  filter(!is.na(n2_M),!is.na(n2_D),!is.na(n2_Y))


# add the multilinestring back
pdas_da_pc <- pdas_da_pc %>% as.data.frame() %>% select(-geometry) %>%
  left_join(pdas_assetbase[,c("pipe_uq_id","geometry")], by = "pipe_uq_id") %>%
  st_as_sf(crs = 27700)
pdas_da_pc_ma_2 <- pdas_da_pc %>% left_join(my_ans[,c("pipe_uq_id", "Tag", "n2_M", "n2_D", "n2_Y")], by = "pipe_uq_id")

# modify column data type
pdas_da_pc_ma_2 <- pdas_da_pc_ma_2[!duplicated(pdas_da_pc_ma_2$pipe_uq_id), ] %>%
  mutate(n2_M = as.character(n2_M),
         n2_D = as.integer(as.character(n2_D)),
         n2_Y = as.integer(as.character(n2_Y)))

# merge the outstanding pdas assets back
outstanding_pdas <- pdas_assetbase_sample %>%
  anti_join(as.data.frame(pdas_da_pc_ma_2[,c("pipe_uq_id")]), by = "pipe_uq_id")
pdas_da_pc_ma_2 <- plyr::rbind.fill(pdas_da_pc_ma_2,outstanding_pdas)


# st_write(pdas_da_pc_ma_2,"D:/STW PDAS/pdas_phase_2/pdas n2 attrs.shp")

# # test
# pdas_da_pc_ma_2 %>% filter(drwng__ == "osgb1000021391250")
# pdas_da_pc_ma_2 %>% filter(drwng__ == "osgb1000017053266")
# pdas_da_pc_ma_2 %>% filter(drwng__ == "osgb1000017251564")
# pdas_assetbase_sample %>% filter(drwng__ == "osgb1000017053266")


# fix diameter
diameterclasses <-c(0, 100, 150, 225, 300, 375, 450 ,525, 600, 675, 750, 825,
                    900, 975, 1050, 1200, 1350, 1500)

pdas_da_pc_ma_3 <- rollmeTibble(df1 = pdas_da_pc_ma_2,
                                values = diameterclasses,
                                col1 = "n2_D",
                                method = 'nearest') %>%
  select(-n2_D, -band, -merge) %>%
  rename(n2_D = value)


# fix material
unband_band <- read.csv("D:\\STW PDAS\\old stuff\\Assets - Rulesets - Material Banding Table v2.csv")
pdas_da_pc_ma_4 <- pdas_da_pc_ma_3 %>%
  left_join(unique(unband_band[,c(1,2)]),by = c("n2_M"="MaterialTable_UnbandedMaterial")) %>%
  dplyr::mutate(n2_M = (n2_M = as.factor(MaterialTable_BandedMaterial)))

# add County
pdas_da_pc_ma_5 <- pdas_da_pc_ma_4 %>%
  left_join(props_with_county_and_postcode[,c("fid","County")], by = "fid") %>%
  st_sf() %>% st_set_crs(27700)
no_county <- pdas_da_pc_ma_5 %>% filter(is.na(County)) %>% st_sf() %>% st_set_crs(27700)
pdas_da_pc_ma_5 <- pdas_da_pc_ma_5 %>% filter(!is.na(County)) %>% st_sf() %>% st_set_crs(27700)

# infill Counties with NA using st_nearest_feature
no_county_get_county <- pdas_da_pc_ma_5[st_nearest_feature(no_county,pdas_da_pc_ma_5), "County"]
no_county$County <- no_county_get_county$County

# bind and convert back to dataframe
pdas_da_pc_ma_5 <- plyr::rbind.fill(pdas_da_pc_ma_5,no_county) %>% as.data.frame()

# free up the env
rm(phase_2_props_with_age)
rm(pdas_da_pc)
rm(pdas_da_pc_ma_2)
rm(pdas_da_pc_ma_3)
rm(pdas_da_pc_ma_4)

summary(pdas_da_pc_ma_5)

# final janitorial touch on the master prediction data -------------------------------------------------------------------------
pdas_da_pc_ma_5 <- pdas_da_pc_ma_5 %>% distinct() # MASTER DATA

# pdas_da_pc_ma_5 <- pdas_da_pc_ma_5[!duplicated(pdas_da_pc_ma_5$geometry),]
pdas_da_pc_ma_5$n2_M[pdas_da_pc_ma_5$n2_M == ""] <- NA
pdas_da_pc_ma_6 <- pdas_da_pc_ma_5 %>%
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
unique(pdas_da_pc_ma_6$n2_M)

pdas_da_pc_ma_6 %>%
  as_tibble() %>%
  sapply(function(x) sum(is.na(x)))

summary(pdas_da_pc_ma_6)


# Load model 1--------------------------------------------------------------

rf.fitPDaSdia_DAS <- read_rds("D:/STW PDAS/Git/pdas_mapping/assign-attributes/PDaSdiam_model DAS.RDS")
# remove missing factors and predict
pdas_da_pc_ma_7 <- remove_anomalies_cat_pred(rf.fitPDaSdia_DAS$trainingData,pdas_da_pc_ma_6,"DAS")
rf.pred1 <- predict(rf.fitPDaSdia_DAS, newdata = pdas_da_pc_ma_7)


# Mat
rf.fitMAT_DAS <- read_rds("D:/STW PDAS/Git/pdas_mapping/assign-attributes/PDaSmat_model DAS.RDS")
rf.predMAT <- predict(rf.fitMAT_DAS, newdata = pdas_da_pc_ma_7)


# join back to master data and prep for model 2
result_pred_1 <- cbind(Confidence = "A",DIAMETE = rf.pred1,MATERIA = rf.predMAT,pdas_da_pc_ma_7) # model 1 result
pred_2 <- pdas_da_pc_ma_5 %>%
  anti_join(result_pred_1, by = "rowguid") %>%
  distinct()

pred_2$n2_M[pred_2$n2_M == ""] <- NA
pred_2 <- pred_2 %>%
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

pred_2 %>%
  as_tibble() %>%
  sapply(function(x) sum(is.na(x)))

summary(pred_2)

# Load model 2--------------------------------------------------------------

rf.fitPDaSdia_County <- read_rds("D:/STW PDAS/Git/pdas_mapping/assign-attributes/PDaSdiam_model County.RDS")
rf.pred2 <- predict(rf.fitPDaSdia_County, newdata = pred_2)


# Mat
rf.fitMAT_County <- read_rds("D:/STW PDAS/Git/pdas_mapping/assign-attributes/PDaSmat_model County.RDS")
rf.predMAT2 <- predict(rf.fitMAT_County, newdata = pred_2)


# join back to master data and prep for model 3
result_pred_2 <- cbind(Confidence = "B",DIAMETE = rf.pred2,MATERIA = rf.predMAT2,pred_2) # model 2 result
pred_3 <- pdas_da_pc_ma_5 %>%
  anti_join(result_pred_1, by = "rowguid") %>%
  anti_join(result_pred_2, by = "rowguid") %>%
  distinct()

pred_3$n2_M[pred_3$n2_M == ""] <- NA
pred_3 <- pred_3 %>%
  mutate(n2_M = as.character(n2_M)) %>%
  as_tibble() %>%
  filter(!is.na(PURPOSE),!is.na(PROPERTY_TYPE),!is.na(n2_Y),!is.na(n2_M),n2_M != "",n2_D != 0) %>% # Remove any diameters with 0 counts
  mutate(n2_D = as.factor(n2_D),
         PURPOSE = as.factor(PURPOSE),
         n2_M = as.factor(n2_M),
         n2_Y = as.numeric(n2_Y),
         PROPERTY_TYPE = as.factor(PROPERTY_TYPE),
         County = as.factor(County),
         DAS = as.factor(ID_DA_CODE)) %>%
  select(-MaterialTable_BandedMaterial)

pred_3 %>%
  as_tibble() %>%
  sapply(function(x) sum(is.na(x)))

summary(pred_3)

rm(pdas_assetbase_sample)
rm(pdas_assetbase_fid_trimmed)
rm(pdas_da)
rm(rf.fitPDaSdia_DAS)
rm(rf.fitMAT_DAS)
rm(rf.fitPDaSdia_County)
rm(rf.fitMAT_County) # clear vars in env

# Load model 3--------------------------------------------------------------

rf.fitPDaSdia_County_noY <- read_rds("D:/STW PDAS/Git/pdas_mapping/assign-attributes/PDaSdiam_model County noY 3rd Model.RDS")
rf.pred3 <- predict(rf.fitPDaSdia_County_noY, newdata = pred_3)


# Mat
rf.fitMAT_County_noY <- read_rds("D:/STW PDAS/Git/pdas_mapping/assign-attributes/PDaSmat_model County.RDS")
rf.predMAT3 <- predict(rf.fitMAT_County_noY, newdata = pred_3)


# join back to master data and prep for model 4
result_pred_3 <- cbind(Confidence = "B",DIAMETE = rf.pred3,MATERIA = rf.predMAT3,pred_3) # model 3 result
pred_4 <- pdas_da_pc_ma_5 %>%
  anti_join(result_pred_1, by = "rowguid") %>%
  anti_join(result_pred_2, by = "rowguid") %>%
  anti_join(result_pred_3, by = "rowguid") %>%
  distinct()

pred_4$n2_M[pred_4$n2_M == ""] <- NA
pred_4 <- pred_4 %>%
  mutate(n2_M = as.character(n2_M)) %>%
  as_tibble() %>%
  filter(!is.na(PURPOSE),!is.na(PROPERTY_TYPE),Year != 0) %>% # Remove any diameters with 0 counts
  mutate(n2_D = as.factor(n2_D),
         PURPOSE = as.factor(PURPOSE),
         n2_M = as.factor(n2_M),
         n2_Y = as.numeric(n2_Y),
         PROPERTY_TYPE = as.factor(PROPERTY_TYPE),
         County = as.factor(County),
         DAS = as.factor(ID_DA_CODE)) %>%
  select(-MaterialTable_BandedMaterial)

pred_4 %>%
  as_tibble() %>%
  sapply(function(x) sum(is.na(x)))

summary(pred_4)


# save.image("D:/STW PDAS/Phase 2 datasets/pdas_d_m_y2.rdata")


# Load model 4--------------------------------------------------------------

rf.fit.pdas_4th_diam <- read_rds("D:/STW PDAS/Git/pdas_mapping/assign-attributes/pdasdiam_model_county_4th.RDS")
rf.pred4 <- predict(rf.fit.pdas_4th_diam, newdata = pred_4)


# Mat
rf.fit.pdas_4th_mat <- read_rds("D:/STW PDAS/Git/pdas_mapping/assign-attributes/pdasmat_model_county_4th.RDS")
rf.predMAT4 <- predict(rf.fit.pdas_4th_mat, newdata = pred_4)


# join back to master data and prep for model 5
result_pred_4 <- cbind(Confidence = "B",DIAMETE = rf.pred4,MATERIA = rf.predMAT4,pred_4) # model 4 result
pred_5 <- pdas_da_pc_ma_5 %>%
  anti_join(result_pred_1, by = "rowguid") %>%
  anti_join(result_pred_2, by = "rowguid") %>%
  anti_join(result_pred_3, by = "rowguid") %>%
  anti_join(result_pred_4, by = "rowguid") %>%
  distinct()

result_pred_5 <- pred_5 %>%
  mutate(DIAMETE = "100",MATERIA =  "VC") %>%
  mutate(Confidence = "D",
         n2_D = as.factor(n2_D),
         PURPOSE = as.factor(PURPOSE),
         n2_M = as.factor(n2_M),
         n2_Y = as.numeric(n2_Y),
         PROPERTY_TYPE = as.factor(PROPERTY_TYPE),
         County = as.factor(County),
         DAS = as.factor(ID_DA_CODE)) %>%
  select(-MaterialTable_BandedMaterial)



# merge all together    # changed all the way down

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
  left_join(pdas_assetbase[,c("pipe_uq_id")]) %>% 
  st_sf() %>% st_set_crs(27700) 

# infill the missing yearlaid rows and merge them back together -----------------------
results_withna <- results %>% filter(is.na(YEARLAI))
results_withoutna <- results %>% filter(!is.na(YEARLAI))

# infill assets with NA in YEARLAI using st_nearest_feature
results_withna_yearlaid <- results_withoutna[st_nearest_feature(results_withna,results_withoutna), "YEARLAI"]
results_withna$YEARLAI <- results_withna_yearlaid$YEARLAI

my_columns <- c("pipe_uq_id", "PROPERTY_TYPE", "PURPOSE", "Confidence", "MATERIA", "drwng__", 
                "DIAMETE", "County", "ID_DA_CODE", "propgrp", "fid", "YEARLAI", "geometry")
results_withna <- results_withna[,my_columns] %>% st_sf() %>% st_set_crs(27700)
results <- rbind(results_withoutna,results_withna)
results <- results[!duplicated(results$geometry),]

summary(results)
st_write(results,"D:/STW PDAS/pdas_phase_2/pdas_lines_with_attrs_14012021.shp")


# save.image("D:/STW PDAS/Phase 2 datasets/pdas_d_m_y_04122020.rdata")
# load("D:/STW PDAS/Phase 2 datasets/pdas_d_m_y_04122020.rdata")
