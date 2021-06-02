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


link_a <- "G:/Desktop/STW PDaS/Phase 2 datasets/S24_pipegroups_geometry_labelled_v3/"
link_b <- "G:/Desktop/STW PDaS/Phase 2 datasets/sprint_4_props_with_atts/"
link_c <- "G:/Desktop/STW PDaS/Phase 2 datasets/OSIWWP-ID-only/"
link_e <- "G:/Desktop/STW PDaS/Phase 2 datasets/Postcode/"
link_f <- "G:/Desktop/STW PDaS/Phase 2 datasets/SOILS/"
link_g <- "G:/Desktop/STW PDaS/Phase 2 datasets/S24_INFERRED_Sewer/"
link_one <- "G:/Desktop/STW PDaS/Phase 2 datasets/updated_building_polygons/"
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




# infill missing postcode age ................................................................

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
infilled_postcode_age <- rbind(postcode_age_na_infill,props_with_county_and_postcode_geom_nona)
infilled_postcode_age %>% filter(fid == "osgb1000033839033")
#saveRDS(infilled_postcode_age,"infilled_postcode_age.rds")


# predict on the new data

s24_cluster <- fread("G:/Desktop/STW PDaS/Phase 2 datasets/s24_cluster_preds/s24_cluster_preds.csv")
s24_cluster %>% 
  as_tibble() %>% 
  sapply(function(x) sum(is.na(x)))
s24_cluster %>% filter(fid == "osgb1000033839033")
s24_cluster_na <- s24_cluster %>% filter(is.na(pc_year))
s24_cluster_nona <- s24_cluster %>% filter(!is.na(pc_year))
st_geometry(infilled_postcode_age) <- NULL
s24_cluster_na <- s24_cluster_na %>% 
  left_join(infilled_postcode_age[,c("fid","nearest_postcode_centroid_year")], by = c("fid"="fid")) %>% 
  mutate(pc_year = nearest_postcode_centroid_year) %>% 
  select(-nearest_postcode_centroid_year)
s24_clusters_1 <- rbind(s24_cluster_na,s24_cluster_nona) 
s24_clusters_1 <- s24_clusters_1 %>% 
  distinct() %>% 
  left_join(sprint_4_props_with_atts_age_geom[,c("fid","P_Type","geometry")], by = "fid")
# s24_clusters_2 <- s24_clusters_1 %>% 
#   left_join(infilled_postcode_age[,c("fid","County","nearest_postcode_centroid")], by = c("fid"="fid"))

# add soil, ppt tpe
tic()
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
s24_clusters_1 <- st_as_sf(s24_clusters_1)
st_crs(s24_clusters_1) <- 27700
st_crs(soils) <- 27700
soils$id <- seq.int(nrow(soils))
s24_clusters_1$id_0 <- seq.int(nrow(s24_clusters_1))
s24_clusters_3 <- get_intersection_get_mode(s24_clusters_1,soils[,c("id","SIMPLE_DES")],x_row.id,y_col.id,"id_0","SIMPLE_DES")
toc()

# add county and postcode districts
s24_clusters_4 <- s24_clusters_3 %>% 
  left_join(props_with_county_and_postcode, by = "fid")
numbers = c(1:10)
s24_clusters_4$aprx_pstcd_2ch <- ifelse(substring(s24_clusters_4$nearest_postcode_centroid,2,2) %in% numbers,
                                                              substring(s24_clusters_4$nearest_postcode_centroid,1,1),
                                                              substring(s24_clusters_4$nearest_postcode_centroid,1,2))
s24_clusters_4$aprx_pstcd_3ch <- ifelse(substring(s24_clusters_4$nearest_postcode_centroid,2,2) %in% numbers,
                                                              substring(s24_clusters_4$nearest_postcode_centroid,1,2),
                                                              substring(s24_clusters_4$nearest_postcode_centroid,1,3))
s24_clusters_4$aprx_pstcd_4ch <- ifelse(substring(s24_clusters_4$nearest_postcode_centroid,2,2) %in% numbers,
                                        substring(s24_clusters_4$nearest_postcode_centroid,1,3),
                                        substring(s24_clusters_4$nearest_postcode_centroid,1,4))

# rename and refactor vars 
s24_clusters_5 <- s24_clusters_4 %>% 
  select(age,County,CnNghTy,pc_year,PC_City,PC_Distric,SIMPLE_DES, aprx_pstcd_2ch, aprx_pstcd_3ch, aprx_pstcd_4ch, count, P_Type) %>%
  rename(res_buil_1 = age) %>%
  rename(Tp_prps = CnNghTy) %>%
  rename(os_no_of_props = count) %>% 
  rename(Year = pc_year) %>%
  select(res_buil_1,County, Tp_prps, Year, aprx_pstcd_2ch, aprx_pstcd_3ch, aprx_pstcd_4ch, SIMPLE_DES, P_Type, os_no_of_props) %>% 
  filter(!is.na(Year),!is.na(SIMPLE_DES),aprx_pstcd_2ch != "",Tp_prps != "") %>% 
  mutate(P_Type = ifelse(P_Type == "Detatched","Detached",ifelse(P_Type == "Terraced","over-8-Terrace",P_Type)),
         County = ifelse(County ==  "Worcestershire/Gloucestershire","Worcestershire Gloucestershire",as.character(County))) %>% 
  distinct()


s24_clusters_5 %>% 
  as_tibble() %>% 
  sapply(function(x) sum(is.na(x)))

s24_clusters_5 %>% summary()

# find the factors which are stopping the prediction
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

original_list_2ch <- traindata.base %>% select(aprx_pstcd_2ch) %>% unlist()
original_list_3ch <- traindata.base %>% select(aprx_pstcd_3ch) %>% unlist()
original_list_4ch <- traindata.base %>% select(aprx_pstcd_4ch) %>% unlist()
original_list_os <- traindata.base %>% select(os_no_of_props) %>% unlist()
original_list_soil <- traindata.base %>% select(SIMPLE_DES) %>% unlist()

list_2ch <- s24_clusters_5 %>% select(aprx_pstcd_2ch) %>% unlist()
list_3ch <- s24_clusters_5 %>% select(aprx_pstcd_2ch) %>% unlist()
list_4ch <- s24_clusters_5 %>% select(aprx_pstcd_4ch) %>% unlist()
list_os <- s24_clusters_5 %>% select(os_no_of_props) %>% unlist()
list_soil <- s24_clusters_5 %>% select(SIMPLE_DES) %>% unlist()

"%ni%" <- Negate("%in%")
ni_2ch_s24_clusters_5 <- s24_clusters_5 %>% filter(aprx_pstcd_2ch %ni% original_list_2ch)
ni_3ch_s24_clusters_5 <- s24_clusters_5 %>% filter(aprx_pstcd_3ch %ni% original_list_3ch)
ni_4ch_s24_clusters_5 <- s24_clusters_5 %>% filter(aprx_pstcd_4ch %ni% original_list_4ch)
ni_os_s24_clusters_5 <- s24_clusters_5 %>% filter(os_no_of_props %ni% original_list_os)
ni_soil_s24_clusters_5 <- s24_clusters_5 %>% filter(SIMPLE_DES %ni% original_list_soil)

# list_2ch <- x %>% anti_join(p, by = "a") %>% select(aprx_pstcd_2ch) %>% unlist()
# list_soil <- x %>% anti_join(p, by = "a") %>% select(SIMPLE_DES) %>% unlist()

s24_clusters_6 <- s24_clusters_5 %>% anti_join(ni_2ch_s24_clusters_5, by = "aprx_pstcd_2ch") %>% 
  anti_join(ni_3ch_s24_clusters_5, by = "aprx_pstcd_3ch") %>% 
  anti_join(ni_4ch_s24_clusters_5, by = "aprx_pstcd_4ch") %>% 
  anti_join(ni_os_s24_clusters_5, by = "os_no_of_props") %>% 
  anti_join(ni_soil_s24_clusters_5, by = "SIMPLE_DES") %>% 
  as_tibble() %>% 
  mutate(mdn_pst_y = as.factor(as.character(Year)),
         aprx_pstcd_2ch = as.factor(aprx_pstcd_2ch),
         aprx_pstcd_3ch = as.factor(aprx_pstcd_3ch),
         res_buil_1 = as.factor(res_buil_1),
         Tp_prps = as.factor(Tp_prps),
         SIMPLE_DES = as.factor(SIMPLE_DES),
         County = as.factor(County),
         Tp_prps = as.factor(Tp_prps),
         Year = as.factor(as.character(Year)),
         P_Type = as.factor(P_Type)) %>% 
  distinct()



# check levels
levels(traindata.base$res_buil_1)
levels(s24_clusters_6$res_buil_1)
levels(traindata.base$aprx_pstcd_2ch)
levels(s24_clusters_6$aprx_pstcd_2ch)
levels(traindata.base$aprx_pstcd_3ch)
levels(s24_clusters_6$aprx_pstcd_3ch)
traindata.base$os_no_of_props %>% unique()
s24_clusters_6$os_no_of_props %>% unique()
levels(traindata.base$Year)
levels(s24_clusters_6$Year)
levels(traindata.base$County)
levels(s24_clusters_6$County)
levels(traindata.base$P_Type)
levels(s24_clusters_6$P_Type)
levels(traindata.base$SIMPLE_DES)
levels(s24_clusters_6$SIMPLE_DES)
levels(traindata.base$Tp_prps)
s24_clusters_6$Tp_prps <- factor(s24_clusters_6$Tp_prps)
levels(s24_clusters_6$Tp_prps)





# s24_clusters_7 <- s24_clusters_5 %>% filter(aprx_pstcd_2ch != "NN", aprx_pstcd_2ch !="OX", SIMPLE_DES != "shallow clay over limestone")

# predict

for_ml_model_rffit_old <- readRDS("G:/Desktop/STW PDaS/Git/pdas_mapping/predict_geom/for_ml_model_rffit_7.rds")
for_ml_model_rffit_old <- readRDS("G:/Desktop/STW PDaS/Git/pdas_mapping/predict_geom/for_ml_model_rffit_6.rds")
tic()
for_ml_model_pred2 <- predict(for_ml_model_rffit_old, newdata = s24_clusters_6)
toc()


# join back to fid and write
result_pred <- cbind(for_ml_model_pred2,s24_clusters_6)
result_pred_2 <- s24_clusters_4 %>% 
  distinct() %>% 
  rename(res_buil_1 = age) %>%
  rename(Tp_prps = CnNghTy) %>%
  rename(os_no_of_props = count) %>% 
  rename(Year = pc_year) %>% 
  mutate(Year = as.factor(as.character(Year))) %>% 
  left_join(result_pred) %>% 
  select(-id_0,-id,-mdn_pst_y) %>% 
  rename(gmtry_l = for_ml_model_pred2) %>% 
  mutate(gmtry_l = as.character(gmtry_l)) %>% 
  distinct()

result_pred_2 <- result_pred_2 %>% 
  mutate(gmtry_l = ifelse(is.na(gmtry_l),"straight",gmtry_l)) %>% 
  mutate(gmtry_l = as.factor(gmtry_l))
result_pred_2$gmtry_l %>% unique()
result_pred_2 %>% summary()

#sprint_4_props_with_atts_age_wwshr_4 and result_pred_2 must have same row numbers
s24_clusters_4 %>% distinct() %>% dim()
result_pred_2 %>% distinct() %>% dim()
s24_cluster %>% distinct() %>% dim() # 1889018
s24_cluster %>% dim() # 1889018

s24_cluster_gmtry <- s24_cluster %>% left_join(distinct(result_pred_2[,c("fid","Tp_prps","gmtry_l")]), by = c("fid","CnNghTy"="Tp_prps")) %>% 
  # filter(CnNghTy != "") %>% 
  mutate(gmtry_l = as.factor(gmtry_l)) %>% 
  distinct()
# s24_cluster_gmtry <- s24_cluster %>% left_join(distinct(result_pred_2[,c("fid","gmtry_l")]), by = c("fid")) %>% 
  # distinct()
s24_cluster_gmtry %>% summary()
s24_cluster_gmtry %>% distinct() %>% dim() # 1890163
s24_cluster_gmtry %>% group_by()
s24_cluster %>% filter(CnNghTy == "")
s24_cluster_gmtry %>% anti_join(s24_cluster, by = "fid") # 1890163


write.csv(s24_cluster_gmtry,"G:/Desktop/STW PDaS/Phase 2 datasets/Postcode/s24_cluster_2.csv")
save.image("apply geometry shape prediciton.rdata")
