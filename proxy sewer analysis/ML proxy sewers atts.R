
# Packages ----------------------------------------------------------------

library(caret)
library(party)
library(mlbench)
library(doParallel)
library(stringr)
library(data.table)
library(dplyr)
library(sf)
library(sp)
library(units)
library(tmap)
library(ggplot2)
library(rgdal)
library(tictoc)
library(tidyr)
library(gbm)



# Predict attributes of proxy-road-sewer
# Look at the attributes of the nearest neighbour sewer main


# load data, rdata, rds  ----------------------------------------------------------------
link_one <- "D:/STW PDAS/Phase 2 datasets/updated_building_polygons/"
link_two <- "D:/STW PDAS/Phase 2 datasets/sprint_5_sewers/"
link_three <- "D:/STW PDAS/Phase 2 datasets/sprint_4_props_with_atts/"
link_d <- "D:/STW PDAS/Phase 2 datasets/All_Sewerln_With_Attributes/"
link_e <- "D:/STW PDAS/Phase 2 datasets/Postcode/"
link_f <- "D:/STW PDAS/Phase 2 datasets/SOILS/"
link_h <- "D:/STW PDAS/ModalandNearestNeighbour/ModalAttributes/"
# link_g <- "D:/STW PDAS/Phase 2 datasets/S24_INFERRED_Sewer/"
sewer <- st_read(paste0(link_two,"sprint_5_sewers.shp"))
unband_band <- read.csv("D:/STW PDAS/old stuff/Assets - Rulesets - Material Banding Table v2.csv")
year_lookup <- read.csv(paste0(link_one,"year_lookup.csv"))
merged_iba_s4_geom <- st_read(paste0(link_three,"sprint_4_props_with_atts_age.shp")) # building poly with age
modal_atts <- st_read(paste0(link_h,"ModalAttributes_Merged.shp"))
# props_with_county_and_postcode <- read.csv(paste0(link_e,"props_with_county_and_postcode.csv"))
# source <- st_read(paste0(link_g,"S24_INFERRED_Sewer.shp"), stringsAsFactors = F)
all_sewer <- readRDS(paste0(link_d,"All_Sewerln_With_Attributes.rds"))
soils <- st_read(paste0(link_f,"SOILS.shp"), stringsAsFactors = F)
the_postcode_2 <- st_read(paste0(link_e,"the_postcode_2.shp"))

# rdata load ?
# load("D:/STW PDAS/RDATA/MLproxysewersatts.rdata")



# data pre-processing
proxy_sewer <- sewer %>% filter(rod_flg == "1")
mapped_sewer <- sewer %>% filter(rod_flg == "0") %>% 
  mutate(Unbnd_D = as.integer(as.character(Unbnd_D)))

# add the n2_m and n2_D
st_geometry(modal_atts) <- NULL
mapped_sewer_n2 <- mapped_sewer %>% 
  left_join(as.data.frame(modal_atts[,c("Tag","n2_D", "n2_M","n2_Y")]), by = "Tag") %>% 
  mutate(n2_D = as.integer(as.character(n2_D)),
         n2_Y = as.integer(as.character(n2_Y)))
  


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
get_mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}
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
  df_2 <- df_combine %>%
    select(-c(x_row.id,y_col.id)) %>%
    group_by(group.id) %>%
    distinct()
  
  df <- df_2 %>%
    mutate(modal_value = get_mode(modal)) %>%
    as.data.frame()
  
  colnames(df)[which(names(df) == "group.id")] <- group.id
  colnames(df)[which(names(df) == "modal")] <- modal
  
  return(df)
}

# These are the decided classes
diameterclasses <-c(0,225,750,1500)

mapped_sewer_bd_1 <- rollmeTibble(df1 = mapped_sewer_n2,
                                values = diameterclasses,
                                col1 = "Unbnd_D",
                                method = 'nearest') %>%
  select(-band, -merge) %>% 
  rename(Bnd_D = value)
mapped_sewer_bd <- rollmeTibble(df1 = mapped_sewer_bd_1,
                                values = diameterclasses,
                                col1 = "n2_D",
                                method = 'nearest') %>%
  select(-band, -merge, -n2_D) %>% 
  rename(n2_D = value)

# Check 0 counts
mapped_sewer_bd %>% 
  as_tibble() %>% 
  count(Bnd_D)

# check if tag is the uid and merge the geom back

mapped_sewer_bd <- mapped_sewer_bd %>% 
  left_join(mapped_sewer[,"Tag"], by = "Tag")


# get the modal age of the nearest properties by getting the tag of the mapped_sewer_pipe -------------------------------------
# many ways - you can use a buffer around the pipes and the properties within the buffer is used /

# add a new rowid called uid
merged_iba_s4_geom$uid <- seq.int(nrow(merged_iba_s4_geom))

# set the crs to 27700
st_crs(merged_iba_s4_geom) <- 27700
mapped_sewer_bd <- st_as_sf(mapped_sewer_bd,crs = 27700)


# create a 5m buffer for mapped_sewer_bd and extract only the merged_iba_s4_geom within that buffer --------------------
mapped_sewer_bd_buffer <- st_buffer(mapped_sewer_bd[,"Tag"],30)
mapped_sewer_bd_buffer <- mapped_sewer_bd_buffer %>% rename(Tag_buffer = Tag)

# Calculate_Intersect
intersect <- st_intersects(mapped_sewer_bd_buffer,merged_iba_s4_geom)
intersect <- as.data.frame(intersect)
mapped_sewer_bd_buffer$row.id <- seq.int(nrow(mapped_sewer_bd_buffer))
merged_iba_s4_geom$col.id <- seq.int(nrow(merged_iba_s4_geom))

# Get the id of the buildings in buffer
mapped_sewer_bd_buffer_col_id <- left_join(intersect, as.data.frame(mapped_sewer_bd_buffer), by = "row.id") %>% select(-geometry)
combine <- left_join(mapped_sewer_bd_buffer_col_id, as.data.frame(merged_iba_s4_geom), by = "col.id") %>% select(-geometry)

# get the modal age
year_lookup <- year_lookup %>% 
  mutate(res_buil_1_fact = as.integer(res_buil_1))

combine_two_0 <- combine %>% 
  select(-c(row.id,col.id)) %>% 
  # as_tibble() %>% 
  group_by(Tag_buffer) %>% 
  left_join(year_lookup[,c("res_buil_1", "res_buil_1_fact")], by = "res_buil_1") %>%
  distinct()

combine_two <- combine_two_0 %>%
  #arrange(year_index) %>%
  select(Tag_buffer,P_Type, res_buil_1,res_buil_1_fact) %>% 
  mutate(rowindex = get_mode(res_buil_1)) %>% 
  as.data.frame()
  

combine_three <- combine_two %>%
  select(Tag_buffer,rowindex) %>%
  distinct() 

combine_three$rowindex %>% as.factor() %>% summary()
combine_two %>% filter(Tag_buffer == "3000469661")
combine_two[,c("Tag_buffer","res_buil_1")] %>% unique()


# will come back to you later -------------------------------------------------------------------------------
# band unbanded materials
unband_band <- read.csv("G:\\Desktop\\STW PDaS\\old stuff\\Assets - Rulesets - Material Banding Table v2.csv")
mapped_sewer_bd_bm <- mapped_sewer_bd %>%
  left_join(unique(unband_band[,c(1,2)]),by = c("Unbnd_M"="MaterialTable_UnbandedMaterial")) %>% 
  dplyr::mutate(Bnd_M = as.factor(as.character(MaterialTable_BandedMaterial))) %>% 
  dplyr::select(-MaterialTable_BandedMaterial) %>%  
  left_join(unique(unband_band[,c(1,2)]),by = c("n2_M"="MaterialTable_UnbandedMaterial")) %>% 
  dplyr::mutate(n2_M = as.factor(as.character(MaterialTable_BandedMaterial))) %>% 
  dplyr::select(-MaterialTable_BandedMaterial)

mapped_sewer_bd_bm %>% summary()

# add the max age back to the proxy pipes
mapped_sewer_bd_age <- mapped_sewer_bd_bm %>% 
  left_join(combine_three, by = c("Tag"="Tag_buffer")) %>% 
  rename(res_buil_1 = rowindex) %>% 
  mutate(Length = as.integer(as.character(Length)))

# These are the decided length bands
lengthclasses <-c(0,10,20,50,100,200,300,500)

mapped_sewer_bd_age %>% summary()
mapped_sewer_bd_age_length <- rollmeTibble(df1 = mapped_sewer_bd_age,
                                values = lengthclasses,
                                col1 = "Length",
                                method = 'nearest') %>%
  dplyr::select(-band,-merge,-geometry) %>% 
  rename(Bnd_Length = value)

# use the inferred sewer base to get the f and t junction node ---------------------------------------------
st_geometry(all_sewer) <- NULL
me_1 <- all_sewer[,c("Tag","Fjunction","Tjunction")] %>% 
  as.data.frame()  %>% 
  mutate(Tag = as.character(Tag)) %>% 
  right_join(mapped_sewer_bd_age_length, by = "Tag")
me_2 <- me_1 %>% filter(!is.na(Fjunction))

# get the nearest tag based on f junctions - T junction connect
me_3 <- me_2 %>% select(Tag,Fjunction, Tjunction) %>% distinct()
me_4 <- me_3[,c("Fjunction","Tag")] %>% filter(Fjunction != 0) %>% distinct() %>% 
  left_join(distinct(me_3[,c("Tjunction","Tag")]), by = c("Fjunction"="Tjunction")) %>% 
  filter(!is.na(Tag.y))

me_5 <- mapped_sewer_bd_age_length %>% 
  left_join(me_4, by = c("Tag"="Tag.x")) %>% 
  left_join(mapped_sewer_bd_age_length[,c("Tag","Bnd_D","Bnd_M")], by = c("Tag.y"="Tag"))
  
me_5 %>% summary()

go_into_model <- me_5 %>% select(Bnd_Length,Bnd_D.x,Tag,Length,GrpdTyp,Tp_prps, Type,  
                                  Cnty_st,Bnd_M.x,,Fjunction,Tag.y,Bnd_D.y,Bnd_M.y, res_buil_1) %>% 
  rename(Bnd_D = Bnd_D.x) %>% 
  rename(Bnd_D_2 = Bnd_D.y) %>% 
  rename(Bnd_M = Bnd_M.x) %>% 
  rename(Bnd_M_2 = Bnd_M.y)

# get the postcode sector and postcode district
the_postcode_2 <- the_postcode_2 %>% select(Tag,nrst_p_) %>%
  mutate(left = as.character(sapply(nrst_p_, function(i) unlist(str_split(i, " "))[1])), # split the X Y
         right = as.character(sapply(nrst_p_, function(i) unlist(str_split(i, " "))[2])))
st_geometry(the_postcode_2) <- NULL
go_into_model <- go_into_model %>% left_join(the_postcode_2, by = "Tag")
numbers = c(1:10)
go_into_model$aprx_pstcd_2ch <- ifelse(substring(go_into_model$left,2,2) %in% numbers,
                                      substring(go_into_model$left,1,1),
                                      substring(go_into_model$left,1,2))
go_into_model$aprx_pstcd_3ch <- go_into_model$left
go_into_model$aprx_pstcd_4ch <- paste0(go_into_model$left," ",substring(go_into_model$right,1,1))

# add soils atts
st_crs(soils) <- 27700
soils$id <- seq.int(nrow(soils))
soils %>% mutate(SIMPLE_DES = as.factor(SIMPLE_DES),cg_soil = as.factor(cg_soil)) %>% summary()
go_into_model_2 <- go_into_model %>% left_join(mapped_sewer[,c("Tag")], by = "Tag")
go_into_model_2 <- st_as_sf(go_into_model_2)
st_crs(go_into_model_2) <- 27700
go_into_model_2$id_0 <- seq.int(nrow(go_into_model_2))
go_into_model_3 <- get_intersection_get_mode(go_into_model_2, soils[,c("id","SIMPLE_DES")],x_row.id,y_col.id,"id_0","SIMPLE_DES")


# Machine Learning Bit ----------------------------------------------------------------------------------

# Set everything up into the right data formats
mapped_ml_model <- go_into_model_3 %>% 
  as_tibble() %>%
  select(Bnd_D,Bnd_D_2,Type,Cnty_st,res_buil_1,Bnd_M,Bnd_M_2,Bnd_Length, aprx_pstcd_2ch, aprx_pstcd_3ch,aprx_pstcd_4ch, SIMPLE_DES) %>% # Add length band later
  filter(!is.na(Type),!is.na(res_buil_1),!is.na(Cnty_st),!is.na(Bnd_D), 
         Bnd_D != 0,!is.na(Bnd_M), Bnd_M != "", !is.na(aprx_pstcd_2ch),aprx_pstcd_2ch != 0,
         !is.na(Bnd_D_2), Bnd_D_2 != 0,!is.na(Bnd_M_2), Bnd_M_2 != "",
         !is.na(Bnd_Length),Bnd_Length != 0) %>% 
  distinct() %>% 
  mutate(Bnd_D = as.factor(as.character(Bnd_D)),
         Bnd_M = as.factor(Bnd_M),
         Bnd_D_2 = as.factor(as.character(Bnd_D_2)),
         Bnd_M_2 = as.factor(Bnd_M_2),
         aprx_pstcd_2ch = as.factor(aprx_pstcd_2ch),
         aprx_pstcd_3ch = as.factor(aprx_pstcd_3ch),
         aprx_pstcd_4ch = as.factor(aprx_pstcd_4ch),
         SIMPLE_DES = as.factor(SIMPLE_DES),
         Bnd_Length = as.factor(as.character(Bnd_Length)),
         res_buil_1 = as.factor(res_buil_1))

mapped_ml_model <- mapped_ml_model %>% mutate(Bnd_M = as.character(Bnd_M)) %>% 
  filter(Bnd_M != "",Bnd_M_2 != "") %>% 
  select(Bnd_M, everything())
mapped_ml_model$Bnd_M <- factor(mapped_ml_model$Bnd_M)
mapped_ml_model$Bnd_M_2 <- factor(mapped_ml_model$Bnd_M_2)
mapped_ml_model %>% summary()


mapped_ml_model %>% 
  as_tibble() %>% 
  sapply(function(x) sum(is.na(x)))

mapped_ml_model %>% summary()

# save.image("MLproxysewersatts.RData",version = 2, compress = TRUE)

# Add new vars like age, -----------------------------------------------------------------
rm(intersect)
rm(combine)
rm(mapped_sewer)
rm(mapped_sewer_bd)
rm(mapped_sewer_bd_1)
rm(mapped_sewer_bd_age)
rm(mapped_sewer_bd_age_length)
rm(mapped_sewer_bd_bm)
rm(mapped_sewer_bd_buffer)
rm(mapped_sewer_bd_buffer_col_id)


#split data for ML

set.seed(14)
train.base <- sample(1:nrow(mapped_ml_model),(nrow(mapped_ml_model)*0.75),replace = FALSE)
traindata.base <- mapped_ml_model[train.base, ]
testdata.base <- mapped_ml_model[-train.base, ]

# If you want to process in parallel do the following
cl <- makePSOCKcluster(3)
registerDoParallel(cl)

# Train the model for diam


tic()
mapped_ml_model_rffit_diam_2 <- train(Bnd_D ~ Type + Cnty_st + res_buil_1 + Bnd_D_2 + Bnd_M_2 + aprx_pstcd_2ch,
                                    tuneLength = 1,
                                    data = traindata.base[1:70000,],
                                    method = "ranger",
                                    trControl = trainControl(
                                      method = "cv",
                                      allowParallel = TRUE,
                                      number = 5,
                                      verboseIter = T
                                    ))
toc()

# rev(sort(varimp(mapped_ml_model_rffit_diam_2)))
saveRDS(mapped_ml_model_rffit_diam_2, "D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/ps_attrs_models/mapped_ml_model_rffit_diam_4.rds") # 85% 72 NIR
mapped_ml_model_rffit_diam_2 <- readRDS("D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/ps_attrs_models/mapped_ml_model_rffit_diam_4.rds") 

# Predict and test the model
tic()
mapped_ml_model_pred2_diam_2 <- predict(mapped_ml_model_rffit_diam_2, newdata = testdata.base)
toc()
caret::confusionMatrix(mapped_ml_model_pred2_diam_2, testdata.base$Bnd_D)

# Train the model for mat -------------------------------------------
traindata.base <- traindata.base %>% mutate(Bnd_M = as.character(Bnd_M)) %>% 
  filter(Bnd_M != "",Bnd_M_2 != "") %>% 
  select(Bnd_M, everything())
traindata.base$Bnd_M <- factor(traindata.base$Bnd_M)
traindata.base$Bnd_M_2 <- factor(traindata.base$Bnd_M_2)
traindata.base %>% summary()



tic()
mapped_ml_model_rffit_mat_2 <- train(Bnd_M ~ Bnd_D_2 + Cnty_st + Type + res_buil_1 +  Bnd_M_2 + aprx_pstcd_2ch,
                                   tuneLength = 1,
                                   data = traindata.base[1:70000,],
                                   method = "ranger",
                                   trControl = trainControl(
                                     method = "cv",
                                     allowParallel = F,
                                     number = 5,
                                     verboseIter = T
                                   ))
toc()


testdata.base <- testdata.base %>% mutate(Bnd_M = as.character(Bnd_M)) %>% 
  filter(Bnd_M != "",Bnd_M_2 != "") %>% 
  select(Bnd_M, everything())
testdata.base$Bnd_M <- factor(testdata.base$Bnd_M)
testdata.base$Bnd_M_2 <- factor(testdata.base$Bnd_M_2)
testdata.base %>% summary()

rev(sort(varimp(mapped_ml_model_rffit_mat)))
saveRDS(mapped_ml_model_rffit_mat_2, "D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/ps_attrs_models/mapped_ml_model_rffit_mat_4.rds") #  74 49 nir
mapped_ml_model_rffit_mat_2 <- readRDS("D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/ps_attrs_models/mapped_ml_model_rffit_mat_4.rds") 

# Predict and test the model
tic()
mapped_ml_model_pred2_mat_2 <- predict(mapped_ml_model_rffit_mat_2, newdata = testdata.base)
toc()
caret::confusionMatrix(mapped_ml_model_pred2_mat_2, testdata.base$Bnd_M)


