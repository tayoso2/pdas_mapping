
# Packages ---------------------------------------------------------------
library(data.table)
library(caret)
library(party)
library(mlbench)
library(doParallel)
library(stringr)
library(sf)
library(sp)
library(units)
library(tmap)
library(ggplot2)
library(tictoc)
library(dplyr)
library(tidyr)

# load data, rdata, rds  ----------------------------------------------------------------
link_one <- "D:/STW PDAS/Phase 2 datasets/updated_building_polygons/"
link_two <- "D:/STW PDAS/Phase 2 datasets/segmented_road_proxies_distances_FCS/"
link_h <- "D:/STW PDAS/ModalandNearestNeighbour/ModalAttributes/"
link_e <- "D:/STW PDAS/Phase 2 datasets/Postcode/"
link_nbp <- "D:/STW PDAS/Phase 2 datasets/sprint_4_props_with_atts/"
proxy_sewer <- st_read(paste0(link_two,"segmented_road_proxies_distances_FCS.shp"))
unband_band <- read.csv("D:/STW PDAS/old stuff/Assets - Rulesets - Material Banding Table v2.csv")
year_lookup <- read.csv(paste0(link_one,"year_lookup.csv"))
modal_atts <- st_read(paste0(link_h,"ModalAttributes_Merged.shp"))
the_postcode_2 <- st_read(paste0(link_e,"the_postcode_2.shp"))
phase_2_props_with_age <- st_read(paste0(link_nbp,"phase_2_props_with_age.shp"))

# massage the proxy sewer data before assigning county, type, postcode etc
# get the following Bnd_D_2 +  Bnd_M_2 + Type + res_buil_1 + Bnd_Length  + aprx_pstcd_2ch + aprx_pstcd_3ch



# Functions ---------------------------------------------------------------

rollmeTibble <- function(df1, values, col1, method) {
  
  df1 <- df1 %>% 
    mutate(merge = !!rlang::sym(col1)) %>% 
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
get_mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}
remove_anomalies_cat_pred <- function(x,y,col){
  # this removes categories/classes in y that fail to appear in x
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
  x$index <- nearest
  y2 <- y[,c("id", "feature", "rowguid")]
  st_geometry(y2) <- NULL
  x2 <- merge(x, as.data.frame(y2), by.x = "index", by.y = "rowguid")
  output <- x2
  
  # revert column names
  colnames(output)[which(names(output) == "id.x")] <- pipeid
  colnames(output)[which(names(output) == "id.y")] <- id
  colnames(output)[which(names(output) == "feature")] <- feature
  return(output)
} # needs improving but works
get_nearest_features_ycentroid <- function(x, y, pipeid, id, feature) {
  
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
  
  # convert y to centroid
  y <- st_centroid(y)
  
  # get nearest features
  nearest <- st_nearest_feature(x,y)
  x$index <- nearest
  y2 <- y[,c("id", "feature", "rowguid")]
  st_geometry(y2) <- NULL
  x2 <- merge(x, as.data.frame(y2), by.x = "index", by.y = "rowguid")
  output <- x2
  
  # revert column names
  colnames(output)[which(names(output) == "id.x")] <- pipeid
  colnames(output)[which(names(output) == "id.y")] <- id
  colnames(output)[which(names(output) == "feature")] <- feature
  return(output)
} # needs improving but works
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
  x$index <- nearest
  y2 <- y[,c("id", "feature", "rowguid")]
  st_geometry(y2) <- NULL
  x2 <- merge(x, as.data.frame(y2), by.x = "index", by.y = "rowguid")
  output <- x2
  
  # revert column names
  colnames(output)[which(names(output) == "id.x")] <- pipeid
  colnames(output)[which(names(output) == "id.y")] <- id
  colnames(output)[which(names(output) == "feature")] <- feature
  return(output)
} # needs improving but works
get_intersection_get_mode <- function(x,y,x_row.id=x_row.id,y_col.id=y_col.id,group.id = x_row.id,modal){
  # x = your x object
  # y = your y object
  # x_row.id = x unique id
  # y_col.id = y unique id
  # group.id = default is the x_row.id. This is what to group the object by and subsequently find the modal value for that group
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
  colnames(df_combine)[which(names(df_combine) == group.id)] <- "group.id"
  colnames(df_combine)[which(names(df_combine) == modal)] <- "modal"
  
  # get the modal age
  df_2 <- df_combine %>%
    select(-c(x_row.id,y_col.id)) %>%
    group_by(group.id) %>%
    distinct()
  
  df <- df_2 %>%
    mutate(modal_value = get_mode(modal)) %>%
    as.data.frame()

  df <- df %>%
    select(group.id,modal_value) %>%
    distinct()
  
  colnames(df)[which(names(df) == "group.id")] <- group.id
  colnames(df)[which(names(df) == "modal")] <- modal
  
  return(df)
} # needs improving but works

# LOAD PROXIES WITH SYSTEM TYPE IN HERE-----------------------------------------






# create unique ids for the loaded datasets
proxy_sewer <- proxy_sewer %>%
  filter(assmd_s != "close_to_F_and_S") %>% 
  st_sf() %>% 
  st_set_crs(27700)
proxy_sewer$id_0 <- 1:nrow(proxy_sewer)
st_crs(phase_2_props_with_age) <- 27700
phase_2_props_with_age$fid_1 <- 1:nrow(phase_2_props_with_age)
modal_atts$Tag_1 <- 1:nrow(modal_atts)

# testing.. ########################## DELETE #######################################
proxy_sewer <- proxy_sewer[1:100,]

# add nearest prop age as res_buil_1 using nearest  ------------------------------------------------
proxy_sewer_with_age <- get_nearest_features_ycentroid(proxy_sewer,phase_2_props_with_age[,c("fid_1","res_buil_1")],"id_0","fid_1","res_buil_1")

# OR add nearest prop age as res_buil_1 using a buffer around props.
proxy_sewer_buffer <- st_buffer(proxy_sewer[,"id_0"],30)
proxy_sewer_buffer <- proxy_sewer_buffer %>% rename(id_0_buffer = id_0)
proxy_sewer_with_age <-
  get_intersection_get_mode(proxy_sewer_buffer,
                            phase_2_props_with_age,
                            "id_0_buffer",
                            "fid_1",
                            "id_0_buffer",
                            "res_buil_1") # missing rows are due to x being too far from y

proxy_sewer_with_age <- proxy_sewer %>%
  left_join(proxy_sewer_with_age,c("id_0"="id_0_buffer")) %>% 
  st_sf() %>% 
  st_set_crs(27700)

# get the nearest n24 M and D
proxy_sewer_with_age_dm <- get_nearest_features(proxy_sewer_with_age,modal_atts[,c("Tag_1", "n2_D")],"id_0","Tag_1","n2_D")
modal_atts <- modal_atts %>% as.data.frame() %>% select(-geometry)
proxy_sewer_with_age_dm <- proxy_sewer_with_age_dm %>% 
  left_join(modal_atts[,c("Tag_1","n2_M")], by = "Tag_1")

# These are the diameter classes ------------------------------------------------------
diameterclasses <-c(0,225,750,1500)
proxy_sewer_with_age_dm$n2_D <- as.character(proxy_sewer_with_age_dm$n2_D)
proxy_sewer_with_age_dm$n2_D <- proxy_sewer_with_age_dm$n2_D %>% replace_na(0)
proxy_sewer_with_age_dm$n2_D <- as.integer(proxy_sewer_with_age_dm$n2_D)

proxy_sewer_with_age_dm_bd <- rollmeTibble(df1 = proxy_sewer_with_age_dm,
                                              values = diameterclasses,
                                              col1 = "n2_D",
                                              method = 'nearest') %>%
  dplyr::select(-band, -merge, -n2_D) %>% 
  dplyr::rename(Bnd_D_2 = value)

proxy_sewer_with_age_dm_bd %>% summary()

# band unbanded materials ---------------------------------------------------------------
proxy_sewer_with_age_dm_bd_bm <- proxy_sewer_with_age_dm_bd %>%
  left_join(unique(unband_band[,c(1,2)]),by = c("n2_M"="MaterialTable_UnbandedMaterial")) %>% 
  dplyr::mutate(Bnd_M_2 = as.factor(as.character(MaterialTable_BandedMaterial))) %>% 
  dplyr::select(-MaterialTable_BandedMaterial)

# get the postcode sector and postcode district ------------------------------------
the_postcode_2 <- the_postcode_2 %>% select(Tag,nrst_p_,Cnty_st) %>%
  mutate(left = as.character(sapply(nrst_p_, function(i) unlist(str_split(i, " "))[1])),
         right = as.character(sapply(nrst_p_, function(i) unlist(str_split(i, " "))[2])))
the_postcode_2 <- the_postcode_2 %>% st_set_crs(27700) %>% mutate(pcd_id = row_number())
proxy_sewer_with_age_dm_bd_bm <- proxy_sewer_with_age_dm_bd_bm %>% st_sf() %>% st_set_crs(27700)
proxy_sewer_with_b_d_pcd <- get_nearest_features(proxy_sewer_with_age_dm_bd_bm, the_postcode_2[, c("pcd_id", "nrst_p_")],
                                                 "nw_nq_d", "pcd_id", "nrst_p_") # the model was trained on this postcode data, 
                                                                              # if time permits update the pred. model using a larger postcode data
st_geometry(proxy_sewer_with_b_d_pcd) <- NULL
go_into_model <- proxy_sewer_with_b_d_pcd %>% 
  left_join(the_postcode_2, by = c("pcd_id" = "pcd_id"))
numbers = c(1:10)
go_into_model$aprx_pstcd_2ch <- ifelse(substring(go_into_model$left,2,2) %in% numbers,
                                       substring(go_into_model$left,1,1),
                                       substring(go_into_model$left,1,2))
go_into_model$aprx_pstcd_3ch <- go_into_model$left
go_into_model$aprx_pstcd_4ch <- paste0(go_into_model$left," ",substring(go_into_model$right,1,1))


# Machine Learning Bit: Set everything up into the right data format -------------------------
# get the following Bnd_D_2 +  Bnd_M_2 + Type + res_buil_1 + Bnd_Length  + aprx_pstcd_2ch + aprx_pstcd_3ch

if(length(go_into_model$Type) < 1) {
  go_into_model$Type <- "C"
}# DELETE ONCE YOU LOADED TYPE

mapped_ml_model <- go_into_model %>% 
  as_tibble() %>%
  rename(Type = fnl_sys) %>% 
  rename(res_buil_1 = bldng_g) %>% 
  select(Bnd_D_2,Type,res_buil_1,Bnd_M_2, aprx_pstcd_2ch, aprx_pstcd_3ch, Cnty_st) %>%
  filter(!is.na(Type),!is.na(res_buil_1),
         !is.na(aprx_pstcd_2ch),aprx_pstcd_2ch != 0,
         !is.na(Bnd_D_2), Bnd_D_2 != 0,
         !is.na(Bnd_M_2), Bnd_M_2 != "") %>% 
  distinct() %>% 
  mutate(Bnd_D_2 = as.factor(as.character(Bnd_D_2)),
         Bnd_M_2 = as.factor(Bnd_M_2),
         aprx_pstcd_2ch = as.factor(aprx_pstcd_2ch),
         aprx_pstcd_3ch = as.factor(aprx_pstcd_3ch),
         Cnty_st = as.factor(Cnty_st),
         res_buil_1 = as.factor(res_buil_1))

mapped_ml_model$Bnd_M_2 <- factor(mapped_ml_model$Bnd_M_2)
mapped_ml_model %>% summary()


# load the models
mapped_ml_model_rffit_diam <- readRDS("D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/ps_attrs_models/mapped_ml_model_rffit_diam_3.rds") # 83.4, 72 nir
mapped_ml_model_rffit_mat <- readRDS("D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/ps_attrs_models/mapped_ml_model_rffit_mat_3.rds") # 72, 49 nir

# Remove assets that don't match the model
mapped_ml_model_2 <- remove_anomalies_cat_pred(mapped_ml_model_rffit_diam$trainingData,mapped_ml_model,"aprx_pstcd_3ch")

# Predict and test the model
tic()
mapped_ml_model_pred2_diam <- predict(mapped_ml_model_rffit_diam, newdata = mapped_ml_model_2)
toc()

tic()
mapped_ml_model_pred2_mat <- predict(mapped_ml_model_rffit_mat, newdata = mapped_ml_model_2)
toc()

# join back to master data
proxyful <- cbind(DIAMETE = mapped_ml_model_pred2_diam,MATERIA = mapped_ml_model_pred2_mat,mapped_ml_model_2)

# infill leftover assets with the modes
# ....





