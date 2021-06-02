
# Packages ----------------------------------------------------------------

library(dplyr)
library(magrittr)
library(sf)
library(sp)
library(caret)
library(data.table)
library(rgdal)

library(party)
library(mlbench)

library(doParallel)
library(foreach)
library(tictoc)

# load r data

load("D:/STW PDAS/Git/pdas_mapping/predict_geom/predict_geometry_shape.rdata")

# Data Load ---------------------------------------------------------------

link_a <- "D:/STW PDAS/Phase 2 datasets/S24_pipegroups_geometry_labelled_v3/"
link_b <- "D:/STW PDAS/Phase 2 datasets/sprint_4_props_with_atts/"
link_c <- "D:/STW PDAS/Phase 2 datasets/OSIWWP-ID-only/"
link_e <- "D:/STW PDAS/Phase 2 datasets/Postcode/"
link_f <- "D:/STW PDAS/Phase 2 datasets/SOILS/"
link_g <- "D:/STW PDAS/Phase 2 datasets/S24_INFERRED_Sewer/"
link_one <- "D:/STW PDAS/Phase 2 datasets/updated_building_polygons/"
s24_pipegroups_geometry_labelled_v3_geom <- st_read(paste0(link_a,"S24_pipegroups_geometry_labelled_v3.shp"), stringsAsFactors = F)
intersected_s24_buildings_5m_buffer <- read.csv(paste0(link_a,"intersected_s24_buildings_5m_buffer.csv"))
sprint_4_props_with_atts_age_geom <- st_read(paste0(link_b,"sprint_4_props_with_atts_age.shp"), stringsAsFactors = F)
ospolygons <- st_read(paste0(link_c,"OSIWWP-ID-only.shp"), stringsAsFactors = F)
building_age <- st_read(paste0(link_one,"building_age.shp"), stringsAsFactors = F)
props_with_county_and_postcode <- read.csv(paste0(link_e,"props_with_county_and_postcode.csv"))
warwickshire_drawing_groups <- read.csv(paste0(link_e,"pred_system_warwick.csv"))
dlx <- readRDS(paste0(link_one,"postcode_age.rds"))
soils <- st_read(paste0(link_f,"SOILS.shp"), stringsAsFactors = F)
source <- st_read(paste0(link_g,"S24_INFERRED_Sewer.shp"), stringsAsFactors = F)


# intersection works ......................................................

st_crs(ospolygons) <- 27700
st_crs(building_age) <- 27700
st_intersects(ospolygons,building_age)

intersect <- st_intersects(ospolygons,building_age)
intersect <- as.data.frame(intersect)
ospolygons$row.id <- seq.int(nrow(ospolygons))
building_age$col.id <- seq.int(nrow(building_age))

intersect_2 <- intersect %>% group_by(row.id) %>% tally(name = "count_ppts")

# Get the id of the buildings in buffer

ospolygons_col_id <- left_join(intersect_2, as.data.frame(ospolygons), by = "row.id") %>% select(-geometry)
# combine <- left_join(ospolygons_col_id, as.data.frame(building_age), by = "col.id") %>% select(-geometry)







# join the above ------------------------------------------------------------

#  clean dlx
dlx$Postcod <- gsub(" ","",dlx$Postcod,fixed = TRUE)
dlx <- dlx %>% select(Postcod, Year)

# check for uid
s24_pipegroups_geometry_labelled_v3$pipegrp %>% unique()
s24_pipegroups_geometry_labelled_v3$gmtry_l %>% unique()

s24_pipegroups_geometry_labelled_v3 <- s24_pipegroups_geometry_labelled_v3_geom
st_geometry(s24_pipegroups_geometry_labelled_v3) <- NULL

sprint_4_props_with_atts_age <- sprint_4_props_with_atts_age_geom
st_geometry(sprint_4_props_with_atts_age) <- NULL
s24_pipegroups_geometry_labelled_v3$pipegrp <- as.character(s24_pipegroups_geometry_labelled_v3$pipegrp)
intersected_s24_buildings_5m_buffer$pipegroup <- as.character(intersected_s24_buildings_5m_buffer$pipegroup)

# load functions and add source (using nearest pipes) and soils
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
get_one_to_one_distance_xycentroid <- function(x, y, x.id, y.id) {
  
  # x.id is id of x
  # y.id is id of y
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
  
  # pre-process x and y
  x2 <- x
  st_geometry(x) <- NULL
  x$x_index <- seq.int(nrow(x))
  y2 <- as.data.frame(x[,c(y.id,"x_index")]) %>% inner_join(y[,y.id], by = y.id)
  # y2 <- merge(as.data.frame(x[,c(y.id,"x_index")]), y[,y.id], by = y.id,all.x=FALSE, all.y=FALSE)
  y2 <- st_as_sf(y2, crs = 27700)
  
  # convert x and y to centroids
  x2 <- st_centroid(x2)
  y2 <- st_centroid(y2)
  
  # Compute x_index:x_index distance matrix
  for (i in 1:nrow(x2)){
    x2$length_to_centroid[[i]] <- st_distance(x2[i, x.id], y2[i, y.id])
  }
  x3 <- x2[,c(x.id,y.id,"length_to_centroid")]
  return(x3)
}
get_intersection_get_mode <- function(x,y,x_row.id=x_row.id,y_col.id=y_col.id,group.id,modal){
  
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
  colnames(df_combine)[which(names(df_combine) == group.id)] <- "group.id"
  colnames(df_combine)[which(names(df_combine) == modal)] <- "modal"
  
  # get the modal age
  
  mode <- function(x) {
    ux <- unique(x)
    ux[which.max(tabulate(match(x, ux)))]
  }
  
  df_2 <- df_combine %>%
    select(-c(x_row.id,y_col.id)) %>%
    group_by(group.id) %>%
    distinct()
  
  df <- df_2 %>%
    mutate(modal_value = mode(modal)) %>%
    as.data.frame()
  
  colnames(df)[which(names(df) == "group.id")] <- group.id
  colnames(df)[which(names(df) == "modal")] <- modal
  
  return(df)
}
st_crs(soils) <- 27700
soils$id <- seq.int(nrow(soils))
soils %>% mutate(SIMPLE_DES = as.factor(SIMPLE_DES),cg_soil = as.factor(cg_soil)) %>% summary()
st_crs(source) <- 27700
st_crs(s24_pipegroups_geometry_labelled_v3_geom) <- 27700
source_s24_pipegroups_geometry <- get_nearest_features(s24_pipegroups_geometry_labelled_v3_geom,source[,c("OBJECTID","SOURCE_MET")],
                                                       "index","OBJECTID","SOURCE_MET")
tic()
dist_xy_source <- get_one_to_one_distance_xycentroid(source_s24_pipegroups_geometry,source[,c("OBJECTID","SOURCE_MET")],"index","OBJECTID")
toc()

dist_xy_source_2 <- dist_xy_source[,c("index","length_to_centroid")] %>% 
  left_join(as.data.frame(source_s24_pipegroups_geometry), by = "index") %>% 
  filter(length_to_centroid <= 5) %>%
  rename(geometry = geometry.x) %>% 
  select(-geometry.y)

source_s24_pipegroups_geometry_1 <- get_intersection_get_mode(dist_xy_source_2,
                                                              soils[,c("id","SIMPLE_DES")],x_row.id,y_col.id,"index","SIMPLE_DES")

# source_s24_pipegroups_geometry_2 <- get_nearest_features(dist_xy_source_2[,1:3],soils[,c("id","SIMPLE_DES")],
#                                                          "index","id","SIMPLE_DES")


pref_geoms <- c("L-shape","unbranched-curved","T-shape","straight")

# join by pipegrp and add the ff: age from sprint 4 props with age + Postcod from dlx

get_gmtry_label <- intersected_s24_buildings_5m_buffer %>% 
  left_join(source_s24_pipegroups_geometry_1[,c("gmtry_l","pipegrp","SOURCE_MET","modal_value")], by = c("pipegroup"="pipegrp")) %>% 
  filter(gmtry_l %in% pref_geoms) %>% 
  mutate(gmtry_l = ifelse(gmtry_l == "unbranched-curved","L-shape",gmtry_l)) %>% 
  left_join(sprint_4_props_with_atts_age[,c("fid","res_buil_1")], by = "fid") %>% 
  left_join(dlx, by = c("aprx_pstcd"="Postcod")) %>% 
  rename(SIMPLE_DES = modal_value)

# These are the decided mdn_postcode_y
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
postcodeclasses <-c(0,1900,1909,1924,1935,1947,1960,1969,1978,1988,1996,2004)

get_gmtry_label <- rollmeTibble(df1 = get_gmtry_label,
                                values = postcodeclasses,
                                col1 = "mdn_pst_y",
                                method = 'nearest') %>%
  dplyr::select(-band, -merge,-mdn_pst_y) %>% 
  rename(mdn_pst_y = value)

# if 2nd char is not between 1-9, select the first 3 characters
numbers = c(1:10)
get_gmtry_label$aprx_pstcd_2ch <- ifelse(substring(get_gmtry_label$aprx_pstcd,2,2) %in% numbers,
                                         substring(get_gmtry_label$aprx_pstcd,1,1),
                                         substring(get_gmtry_label$aprx_pstcd,1,2))
get_gmtry_label$aprx_pstcd_3ch <- ifelse(substring(get_gmtry_label$aprx_pstcd,2,2) %in% numbers,
                                         substring(get_gmtry_label$aprx_pstcd,1,2),
                                         substring(get_gmtry_label$aprx_pstcd,1,3))
get_gmtry_label$aprx_pstcd_4ch <- ifelse(substring(get_gmtry_label$aprx_pstcd,2,2) %in% numbers,
                                         substring(get_gmtry_label$aprx_pstcd,1,3),
                                         substring(get_gmtry_label$aprx_pstcd,1,4))
unique(get_gmtry_label$aprx_pstcd_2ch)
unique(get_gmtry_label$aprx_pstcd_3ch)
unique(get_gmtry_label$aprx_pstcd_4ch)

# get_gmtry_label$gmtry_l %>% unique()
get_gmtry_label$my_index <- seq.int(nrow(get_gmtry_label))
fwrite(get_gmtry_label[,c("my_index","aprx_pstcd","aprx_pstcd_2ch", "aprx_pstcd_3ch","pipegroup")],"D:/STW PDAS/Phase 2 datasets/Postcode/get_gmtry_label.csv",row.names = T)


# add no of os props

st_crs(ospolygons) <- 27700
st_crs(sprint_4_props_with_atts_age_geom) <- 27700

ppt_int <- st_intersects(ospolygons,sprint_4_props_with_atts_age_geom)
ppt_int <- as.data.frame(ppt_int)

ppt_int <- ppt_int %>% group_by(row.id) %>% 
  add_count(name = "os_no_of_props") %>% 
  ungroup()


# Get the no of props added
sprint_4_props_with_atts_age_geom$col.id <- seq.int(nrow(sprint_4_props_with_atts_age_geom))
ppt_int_2 <- sprint_4_props_with_atts_age_geom %>% left_join(ppt_int, by = "col.id") %>% select(-row.id)

# merge with get_geom_label
got_gmtry_label <- get_gmtry_label %>% left_join(ppt_int_2[,c("fid","os_no_of_props")], by = "fid")



# Set everything up into the right data formats and remove rows that will make the model infeasible

for_ml_model <- got_gmtry_label %>% 
  as_tibble() %>%
  select(P_Type, gmtry_l, res_buil_1, County, Tp_prps,mdn_pst_y,near, Year, aprx_pstcd_2ch, aprx_pstcd_3ch,
         aprx_pstcd_4ch,SOURCE_MET,SIMPLE_DES,os_no_of_props) %>% 
  filter(!is.na(res_buil_1),!is.na(mdn_pst_y),!is.na(near),!is.na(Year), mdn_pst_y != 0, Year != 0,
         !is.na(aprx_pstcd_2ch),!is.na(aprx_pstcd_3ch),!is.na(SOURCE_MET),!is.na(SIMPLE_DES),!is.na(os_no_of_props)) %>% 
  mutate(P_Type = as.factor(P_Type),
         aprx_pstcd_2ch = as.factor(aprx_pstcd_2ch),
         aprx_pstcd_3ch = as.factor(aprx_pstcd_3ch),
         aprx_pstcd_4ch = as.factor(aprx_pstcd_4ch),
         gmtry_l = as.factor(gmtry_l),
         res_buil_1 = as.factor(res_buil_1),
         County = as.factor(County),
         Tp_prps = as.factor(Tp_prps),
         mdn_pst_y  = as.factor(as.character(mdn_pst_y)),
         Year = as.factor(as.character(Year)),
         SOURCE_MET  = as.factor(as.character(SOURCE_MET)),
         SIMPLE_DES = as.factor(as.character(SIMPLE_DES)),
         near = as.integer(as.character(near))) %>% 
  distinct()

for_ml_model %>% 
  as_tibble() %>% 
  sapply(function(x) sum(is.na(x)))

for_ml_model %>% summary()
# Dataset 
# Predictors:
#   Ppt type
# Building Age
# Postcode age
# Mdn_pst_y
# County




#split data for ML

set.seed(34)
train.base <- sample(1:nrow(for_ml_model),(nrow(for_ml_model)*0.75),replace = FALSE)
traindata.base <- for_ml_model[train.base, ]
testdata.base <- for_ml_model[-train.base, ]

# If you want to process in parallel do the following
cl <- makePSOCKcluster(3)
registerDoParallel(cl)

# Train the model
tic()
for_ml_model_rffit <- cforest(gmtry_l ~ P_Type + res_buil_1 + County + Tp_prps + Year + aprx_pstcd_2ch + aprx_pstcd_3ch + os_no_of_props + SIMPLE_DES,
                              data = traindata.base, controls = cforest_unbiased(ntree = 100, mtry = 4))
toc()

tic()
for_ml_model_rffit_2 <- train(gmtry_l ~ P_Type + res_buil_1 + County + Tp_prps + Year + aprx_pstcd_2ch + aprx_pstcd_3ch + aprx_pstcd_4ch + os_no_of_props + SIMPLE_DES,
                              tuneLength = 1,
                              data = traindata.base,
                              method = "ranger",
                              trControl = trainControl(
                                method = "cv",
                                allowParallel = TRUE,
                                number = 5,
                                verboseIter = T
                              ))
toc()

rev(sort(varimp(for_ml_model_rffit)))
saveRDS(for_ml_model_rffit, "D:/STW PDAS/Git/pdas_mapping/predict_geom/for_ml_model_rffit_5.rds")
saveRDS(for_ml_model_rffit_2, "D:/STW PDAS/Git/pdas_mapping/predict_geom/for_ml_model_rffit_6.rds")
saveRDS(for_ml_model_rffit, "D:/STW PDAS/Git/pdas_mapping/predict_geom/for_ml_model_rffit_7.rds") #P_Type + res_buil_1 + County + Tp_prps + Year + aprx_pstcd_2ch + aprx_pstcd_3ch + os_no_of_props + SIMPLE_DES

# Predict and test the model
tic()
for_ml_model_pred2 <- predict(for_ml_model_rffit, newdata = testdata.base)
toc()
caret::confusionMatrix(for_ml_model_pred2, testdata.base$gmtry_l)

tic()
for_ml_model_pred2_2 <- predict(for_ml_model_rffit_2, newdata = testdata.base)
toc()
caret::confusionMatrix(for_ml_model_pred2_2, testdata.base$gmtry_l)



# first run P_Type + res_buil_1 + County + Tp_prps - 0.17, 0.42 NIR

# second 54, 51 NIR # for_ml_model_rffit.rds

# third 55.5,51 NIR # for_ml_model_rffit_2.rds
#        near  res_buil_1      County   mdn_pst_y     Tp_prps 
# 0.072325899 0.003408284 0.002141102 0.001693218 0.000243969

# third 58,51 NIR # for_ml_model_rffit_3.rds

# fourth 61,51 NIR # for_ml_model_rffit_4.rds # res_buil_1 + County + Tp_prps + mdn_pst_y + near + Year

# fifth 54,50 NIR # for_ml_model_rffit_5.rds # res_buil_1 + County + Tp_prps + mdn_pst_y + Year + aprx_pstcd_2ch

# round mdn_y to postcode year


#######################################

# ones for warwickshire .......................................................................................

sprint_4_props_with_atts_age_wwshr <- sprint_4_props_with_atts_age %>% 
  left_join(props_with_county_and_postcode, by = "fid") %>% 
  filter(County == "Warwickshire")

# remove dups
sprint_4_props_with_atts_age_wwshr <- sprint_4_props_with_atts_age_wwshr %>% 
  as_tibble() %>% 
  #select(-geometry) %>% # remove if you run all above
  distinct()

# add tp_purp
sprint_4_props_with_atts_age_wwshr_2 <- sprint_4_props_with_atts_age_wwshr %>% 
  left_join(warwickshire_drawing_groups[,c("fid","system_pred")], by = "fid") %>% 
  rename(Tp_prps = system_pred) %>% 
  mutate(Tp_prps = ifelse(Tp_prps == "combined","C",
                          ifelse(Tp_prps == "separate","FS",Tp_prps)))

# split the FS into F and S
sprint_4_props_with_atts_age_wwshr_2_F <- sprint_4_props_with_atts_age_wwshr_2 %>% filter(Tp_prps == "FS") %>% 
  mutate(Tp_prps = "F")
sprint_4_props_with_atts_age_wwshr_2_S <- sprint_4_props_with_atts_age_wwshr_2 %>% filter(Tp_prps == "FS") %>% 
  mutate(Tp_prps = "S")
sprint_4_props_with_atts_age_wwshr_2_FS <- rbind(sprint_4_props_with_atts_age_wwshr_2_F,sprint_4_props_with_atts_age_wwshr_2_S)
sprint_4_props_with_atts_age_wwshr_2_C <- sprint_4_props_with_atts_age_wwshr_2 %>% filter(Tp_prps == "C")
sprint_4_props_with_atts_age_wwshr_3 <- rbind(sprint_4_props_with_atts_age_wwshr_2_C,sprint_4_props_with_atts_age_wwshr_2_FS)
sprint_4_props_with_atts_age_wwshr_3$aprx_pstcd_2ch <- ifelse(substring(sprint_4_props_with_atts_age_wwshr_3$nearest_postcode_centroid,2,2) %in% numbers,
                                                              substring(sprint_4_props_with_atts_age_wwshr_3$nearest_postcode_centroid,1,1),
                                                              substring(sprint_4_props_with_atts_age_wwshr_3$nearest_postcode_centroid,1,2))
sprint_4_props_with_atts_age_wwshr_3$aprx_pstcd_3ch <- ifelse(substring(sprint_4_props_with_atts_age_wwshr_3$nearest_postcode_centroid,2,2) %in% numbers,
                                                              substring(sprint_4_props_with_atts_age_wwshr_3$nearest_postcode_centroid,1,2),
                                                              substring(sprint_4_props_with_atts_age_wwshr_3$nearest_postcode_centroid,1,3))

# load functions and add source (using nearest pipes) and soils

st_crs(sprint_4_props_with_atts_age_geom) <- 27700
sprint_4_props_with_atts_age_wwshr_3_geom <- sprint_4_props_with_atts_age_wwshr_3 %>% 
  left_join(sprint_4_props_with_atts_age_geom[,"fid"], by = "fid") %>% 
  as.data.frame()
sprint_4_props_with_atts_age_wwshr_3_geom <- st_as_sf(sprint_4_props_with_atts_age_wwshr_3_geom,crs=27700)
sprint_4_props_with_atts_age_wwshr_3_geom$id_0 <- seq.int(nrow(sprint_4_props_with_atts_age_wwshr_3_geom))

tic()
sprint_4_props_with_atts_age_wwshr_4 <- get_intersection_get_mode(sprint_4_props_with_atts_age_wwshr_3_geom,
                                                                  soils[,c("id","SIMPLE_DES")],x_row.id,y_col.id,"id_0","SIMPLE_DES")
toc()

dist_xy_source_2 <- dist_xy_source[,c("index","length_to_centroid")] %>% 
  left_join(as.data.frame(source_s24_pipegroups_geometry), by = "index") %>% 
  filter(length_to_centroid <= 5) %>%
  rename(geometry = geometry.x) %>% 
  select(-geometry.y)


# rename and refactor vars 
sprint_4_props_with_atts_age_wwshr_5 <- sprint_4_props_with_atts_age_wwshr_4 %>% 
  select(res_buil_1,County, Tp_prps, nearest_postcode_centroid_year, aprx_pstcd_2ch, aprx_pstcd_3ch, SIMPLE_DES) %>% 
  filter(!is.na(nearest_postcode_centroid_year),!is.na(SIMPLE_DES)) %>% 
  rename(Year = nearest_postcode_centroid_year) %>% 
  mutate(mdn_pst_y = as.factor(as.character(Year)),
         aprx_pstcd_2ch = as.factor(aprx_pstcd_2ch),
         aprx_pstcd_3ch = as.factor(aprx_pstcd_3ch),
         res_buil_1 = as.factor(res_buil_1),
         Tp_prps = as.factor(Tp_prps),
         SIMPLE_DES = as.factor(SIMPLE_DES),
         Year = as.factor(as.character(Year))) %>% 
  distinct()

sprint_4_props_with_atts_age_wwshr_5 <- sprint_4_props_with_atts_age_wwshr_5 %>% filter(aprx_pstcd_2ch != "NN", aprx_pstcd_2ch !="OX", SIMPLE_DES != "shallow clay over limestone")

# predict for wwshr
for_ml_model_rffit_old <- readRDS("D:/STW PDAS/Git/pdas_mapping/predict_geom/for_ml_model_rffit_6.rds")
for_ml_model_rffit_old <- readRDS("D:/STW PDAS/Git/pdas_mapping/predict_geom/for_ml_model_rffit_7.rds")
tic()
for_ml_model_pred2 <- predict(for_ml_model_rffit_old, newdata = sprint_4_props_with_atts_age_wwshr_5)
toc()

# join back to fid and write
result_pred <- cbind(for_ml_model_pred2,sprint_4_props_with_atts_age_wwshr_5)
result_pred_2 <- sprint_4_props_with_atts_age_wwshr_4 %>% 
  rename(Year = nearest_postcode_centroid_year) %>% 
  mutate(Year = as.factor(as.character(Year))) %>% 
  left_join(result_pred) %>% 
  distinct()

#sprint_4_props_with_atts_age_wwshr_4 and result_pred_2 must have same row numbers
dim(sprint_4_props_with_atts_age_wwshr_4)
dim(result_pred_2)

write.csv(result_pred_2,"D:/STW PDAS/Phase 2 datasets/Postcode/wwshr_preds_2.csv")



# infill missing postcode age ................................................................

sprint_4_props_with_atts_age_geom <- st_read(paste0(link_b,"sprint_4_props_with_atts_age.shp"), stringsAsFactors = F)
sprint_4_props_with_atts_age_geom <- as.data.frame(sprint_4_props_with_atts_age_geom)
props_with_county_and_postcode_geom <- merge(sprint_4_props_with_atts_age_geom[,c("fid","geometry")],props_with_county_and_postcode, by = "fid")
props_with_county_and_postcode_geom <- st_as_sf(props_with_county_and_postcode_geom)
st_crs(props_with_county_and_postcode_geom) <- 27700

# subset age categ. with na and those without na
props_with_county_and_postcode_geom_nona <- props_with_county_and_postcode_geom %>% filter(!is.na(nearest_postcode_centroid_year))
props_with_county_and_postcode_geom_na <- props_with_county_and_postcode_geom %>% filter(is.na(nearest_postcode_centroid_year)|nearest_postcode_centroid_year == 0) %>% 
  rename(fid.x=fid)

# assign age attribute from subset with nona to that with na using nearest futures function
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
  x2 <- left_join(x, as.data.frame(y2), by = c("index"= "rowguid"))
  output <- x2
  
  # revert column names
  colnames(output)[which(names(output) == "id.x")] <- pipeid
  colnames(output)[which(names(output) == "id.y")] <- id
  colnames(output)[which(names(output) == "feature")] <- feature
  return(output)
}
props_with_county_and_postcode_geom_age_infill <- get_nearest_features_xycentroid(props_with_county_and_postcode_geom_na[,c("fid.x")],
                                                                                  props_with_county_and_postcode_geom_nona[,c("fid","nearest_postcode_centroid_year")],"fid.x","fid","nearest_postcode_centroid_year")
postcode_age_na_infill <- left_join(props_with_county_and_postcode_geom_age_infill,
                                    as.data.frame(props_with_county_and_postcode_geom_na),
                                    by = "fid.x") %>% 
  select(fid.x,County,nearest_postcode_centroid,nearest_postcode_centroid_year.x,geometry.y) %>% 
  rename(fid = fid.x) %>% 
  rename(nearest_postcode_centroid_year=nearest_postcode_centroid_year.x) %>% 
  rename(geometry = geometry.y) %>% 
  select(-geometry.x)
infilled_postcode_age <- rbind(postcode_age_na_infill,props_with_county_and_postcode_geom_nona) %>% distinct()
infilled_postcode_age %>% distinct() %>% dim()
props_with_county_and_postcode_geom %>% distinct() %>% dim()

# set wd to the current r file location and save the rdata
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
save.image("D:/STW PDAS/Git/pdas_mapping/predict_geom/predict_geometry_shape.rdata")

# the end