library(tidyr)
library(data.table)
library(dplyr)
library(plyr)
library(sp)
library(sf)
library(raster)
library(maptools)
library(units)
library(tmap)
library(ggplot2)
library(rgdal)
library(gbm)
library(DescTools)
library(party)
library(caret)
library(tictoc)
library(purrr)


#s24
setwd("I:\\Projects\\Client\\1883\\Analytics")
# dl <- readOGR(dsn=".", layer="\\2020-02-14 TW Data\\STW Client Transfer\\DL0584ValOfficeAges501-nnn99AMP5SMSL1")
# str(dl)
# writeOGR(dl, dsn=".", layer="DL0584ValOfficeAges501-nnn99AMP5SMSL1", driver="ESRI Shapefile")
dlx <- st_read(".\\2020-02-14 TW Data\\STW Client Transfer\\DL0584ValOfficeAges501-nnn99AMP5SMSL1.shp")
ex_s24 <- st_read(".\\2020-02-14 JG Data\\Jan 20 PDaS\\EX_Section_24_Indicator\\EX_Section_24_Indicator.shp")

# dissimilar columns check
dlx_uneq <- dlx[!as.character(dlx$Postcod)==as.character(dlx$AddpnPC),]

# remove spaces in postcode string
dlx$Postcod <- gsub("[[:blank:]]", "", dlx$Postcod)
summary(dlx)

#select important attributes
dlx2 <- dlx %>% dplyr::select(Bllng_A,Postcod,Length,Year) %>% 
  dplyr::rename(Postcode = Postcod)

#add rowguid column to ex_s24 and select important columns
ex_s24$rowguid <- seq.int(nrow(ex_s24))
ex_s24x <- ex_s24[,c("rowguid")]

# set crs
st_crs(dlx2) <- 27700 #272k
st_crs(ex_s24x) <- 27700 #110k

# find nearest line to centroid
my_split <- rep(1:50,each = 1, length = nrow(dlx2))
my_split2 <- mutate(dlx2, my_split = my_split)
my_split3 <- nest(my_split2, - my_split) 
## use find nearest function
FindNearest <- function(x, y, y.name = "y") {
  # Accepts two sf objects (x and y) and determines the nearest feature in y for
  # every feature in x. 
  #
  # Args:
  #   x: An sf object containing the features for which you want find the 
  #      nearest features in y
  #   y: An sf object containing features to be assigned to x
  #   y.name: Characters prepended to the y features in the returned datatable
  #
  # Returns:
  #   A datatable containing all rows from x with the corresponding nearest 
  #   feature from y. A column representing the distance between the features is
  #   also included. Note that this object contains no geometry.
  
  #browser()
  
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
  
  # Compute distance matrix
  dist.matrix <- st_distance(x, y)
  
  # Select y features which are shortest distance
  nearest.rows <- apply(dist.matrix, 1, which.min)
  # Determine shortest distances
  nearest.distance <-
    dist.matrix[cbind(seq(nearest.rows), nearest.rows)]
  
  # Report distance units
  distance.units <- deparse_unit(nearest.distance)
  message(paste0("Distance unit is: ", distance.units))
  
  # Build data table of nearest features
  nearest.features <- y[nearest.rows,]
  nearest.features$distance <- nearest.distance
  nearest.features$Postcode <- x$Postcode
  nearest.features$Year <- x$Year
  # Remove geometries
  st_geometry(x) <- NULL
  st_geometry(nearest.features) <- NULL
  
  # Prepend names to y columns
  names(nearest.features) <- paste0(y.name, ".", names(nearest.features))
  
  # Bind datatables and return
  #output <- cbind(x, nearest.features)
  output <- nearest.features
  return(output)
}

# tic()
# models <- map(my_split3$data, ~ FindNearest(., ex_s24x)) #lengthy processing time - 11.75 hrs
# toc()
# nearest_centroids <- rbindlist(models, use.names = TRUE, fill = TRUE)
# write.csv(nearest_centroids, "nearest_centroids.csv")
setwd("G:\\Desktop\\STW PDaS")
nearest_centroids <- read.csv("nearest_centroids_year.csv")
nearest_centroids2 <- nearest_centroids %>% mutate(y.distance = as.numeric(y.distance)) %>% 
  filter(y.distance <= 15) %>% # use 15m buffer
  mutate(Year2 = ifelse(is.na(y.Year),"before 1937", as.numeric(y.Year)))

#test number of nearest_centroids2 in the right age category
nearest_centroids3 <- nearest_centroids2 %>% dplyr::rename(rowguid = y.rowguid) %>% 
  left_join(ex_s24[,c("TAG","FJUNCTIONI","TJUNCTIONI","rowguid")], by = "rowguid")

#check why these are in s24
s24s_wrongage <- filter(nearest_centroids3, y.Year > "1937") %>%
  #dplyr::rename(rowguid = y.rowguid) %>% 
  left_join(ex_s24[,c("TAG","FJUNCTIONI","TJUNCTIONI","rowguid")], by = "rowguid")

s24s_wrongage2 <- s24s_wrongage %>% dplyr::rename(distance_to_pcd_centroid = y.distance) %>% 
  dplyr::rename(geometry = geometry.x) %>% 
  dplyr::rename(Year = y.Year) %>% 
  dplyr::rename(Postcode = y.Postcode) %>% 
  dplyr::rename(TAG = TAG.y) %>% dplyr::select(TAG, distance_to_pcd_centroid, Postcode, Year)
write.csv(s24s_wrongage2,"s24s_mismatch_postcode_age.csv")
#st_write(s24s_wrongage2,"s24s_mismatch_postcode_age.shp")


# 100% confidence between the s24 near the postcode & 
summary(nearest_centroids3)
nearest_centroids_test <- nearest_centroids3[!as.character(nearest_centroids3$y.Year)==as.character(nearest_centroids3$y.Year2),]
filter(nearest_centroids3, y.Year == Year2)




# check confidence between public sewers and postcode age
setwd("I:\\Projects\\Client\\1883\\Analytics\\2020-02-14 TW Data\\STW Client Transfer\\")
sl <- readOGR(dsn=".", layer="All_Sewerln_With_Attributes")
# str(dl)
writeOGR(sl, dsn=".", layer="All_Sewerln_With_Attributes", driver="ESRI Shapefile")
slx <- st_read("All_Sewerln_With_Attributes.shp")
slx_nogeom <- slx
st_geometry(slx_nogeom) <- NULL
write.csv(slx_nogeom,"All_Sewerln_With_Attributes.csv")





# nearest <- st_nearest_feature(dlx2,ex_s24)
# nearest <- nearest %>% dplyr::rename(col.id = nearest)
# nearest$row.id <- seq.int(nrow(nearest))

# tmap_mode("view")
# plot1 <- tm_shape(pdas_4_sfc[1:10,]) + tm_basemap("Esri.WorldStreetMap") 
# plot2 <- tm_shape(county_sf) + tm_polygons() + tm_basemap("Esri.WorldStreetMap")
# tmap_arrange(plot2,plot1)

pdas_4_sf$row.id <- seq.int(nrow(pdas_4_sf))
county_sf$col.id <- seq.int(nrow(county_sf))
# Get the names of the names of neighborhoods in buffer
pdas4sf_col.id <- left_join(nearest, as.data.frame(pdas_4_sf), by = "row.id")
combine <- left_join(pdas4sf_col.id, as.data.frame(county_sf[,c("county","col.id")]), by = "col.id")
unique(county_sf$county)
















#create a DF without wkt_geom
s24_ <- s24 %>% filter(GroupedType == 'S24' )#& County == 'Shropshire')
#s24_ <- s24 %>% filter(GroupedType == 'S24' & County == 'Shropshire')
s24_$Current_year <- as.numeric(format(Sys.Date(), "%Y"))
s24_$Age <- (s24_$Current_year - s24_$Banded_Yearlaid); s24_$Current_year <- NULL; s24_$Banded_Yearlaid <- NULL
tail(s24_)
s24_x <- data.table(s24_)
#convert levels to numeric values
s24_x$Tp_purpose <- revalue(s24_x$Tp_purpose,c("C"="1","F"="2","S"="3"))
s24_x$Banded_Material <- revalue(s24_x$Banded_Material,c("BR"="1","CI"="2","CO"="3","GRP_L"="4","PVC"="5","VC"="6"))
s24_x$Tp_purpose <- as.integer(s24_x$Tp_purpose)
s24_x$Banded_Material <- as.integer(s24_x$Banded_Material)
summary(s24_)
describe(s24_)

#S24
setwd("G:\\Desktop\\STW PDaS")
# start here
s24 <- st_read("G:\\Desktop\\STW PDaS\\S24 PDaS Filtered GIS\\STW_S24_Pipes.shp")
str(s24)  
summary(s24)
describe(s24)
s24_1 <- s24 %>% dplyr::select(Length, County, Tp_prps, Postcod,Bndd_Dm,Bndd_Mt,Bndd_Yr) %>% 
  mutate(Bndd_Yr = as.numeric(Bndd_Yr))%>%
  mutate(Age = 2020 - Bndd_Yr)%>%
  dplyr::rename(pipe_typ = Tp_prps) %>%
  dplyr::rename(Postcode = Postcod) %>%
  dplyr::rename(pipe_diam = Bndd_Dm) %>%
  dplyr::rename(pipe_mat = Bndd_Mt) %>% 
  dplyr::rename(Age = Bndd_Yr)












#PDaS
cctv_survey4 <- st_read("PDaS AMP6 Yr4 CORRECT Joined Data (all except PDaS WS53UP) at 03052019/CCTV survey.shp")
pipe4 <- st_read("PDaS AMP6 Yr4 CORRECT Joined Data (all except PDaS WS53UP) at 03052019/Pipe.shp")
cctv_survey3 <- st_read("PDaS AMP6 Yr3 CORRECT Joined Data/CCTV survey.shp")
pipe3 <- st_read("PDaS AMP6 Yr3 CORRECT Joined Data/Pipe.shp")
cctv_survey1 <- st_read("PDaS AMP6 Yr1 JOINED DATA v3/CCTV survey.shp")
pipe1 <- st_read("PDaS AMP6 Yr1 JOINED DATA v3/Pipe.shp")
cctv_survey_yr345 <- st_read("PDaS AMP5 Yr3&Yr4&Yr5(1site) JOINED DATA - butNOTcleansed/CCTV survey.shp")
pipe_yr345 <- st_read("PDaS AMP5 Yr3&Yr4&Yr5(1site) JOINED DATA - butNOTcleansed/Pipe.shp")
cctv_survey_yr2 <- st_read("PDaS AMP6 Yr2 JOINED DATA complete (NOTE some now FullyCleansed&UPLOADED to GISSTdb)/CCTV survey.shp")
pipe_yr2 <- st_read("PDaS AMP6 Yr2 JOINED DATA complete (NOTE some now FullyCleansed&UPLOADED to GISSTdb)/Pipe.shp")
pipe_all <- st_as_sf(rbind.fill(pipe1,pipe_yr2,pipe3,pipe4,pipe_yr345))
nrow(pipe_all)
pipe_x <- pipe_all[!duplicated(pipe_all$id),]
nrow(pipe_x)
cctv_survey_all <- st_as_sf(rbind.fill(cctv_survey1,cctv_survey_yr2,cctv_survey3,cctv_survey4,cctv_survey_yr345))
nrow(cctv_survey_all)
cctv_survey_x <- cctv_survey_all[!duplicated(cctv_survey_all$id),]
nrow(cctv_survey_x)
pdas_age <- read.csv("G:\\Desktop\\STW PDaS\\markdown\\pipe_sf.csv")

#only 3% - 3749 can be mapped to 129k pdas using 50m tolerance, so we ignore below and use the age of the nearest public sewer no matter the age
######ignore###
# pdas_distance <- read.csv("G:\\Desktop\\STW PDaS\\markdown\\nearest_blockage1.csv")
# pdas_age <- pdas_distance %>% dplyr::select(id,distance) %>% 
#   filter(distance < 50) %>% 
#   group_by(id) %>% 
#   mutate(distance = min(distance)) %>% 
#   distinct() %>%
#   ungroup() %>% 
#   left_join(pdas_age, by = "id")
# summary(pdas_age)
##ignore###

pdas_age1 <- pdas_age %>% dplyr::select(id, pipe_mat.x,	pipe_diam.x,	pipe_typ.x,	length.x,	County,	Age)

pdas <- st_read("G:\\Desktop\\STW PDaS\\S24 PDaS Filtered GIS\\STW_PDaS_All_Pipes.shp")
pdas <- pdas[!duplicated(pdas$id),]
summary(pdas)
pdas_1 <- as.data.frame(pdas) %>% left_join(as.data.frame(pipe_x[,c("id","systemtyp","geometry")]), by = "id") %>% 
  dplyr::select(id,asset_id,length,pipe_mat,ds_height,systemtyp,geometry.x) %>% 
  dplyr::rename(pipe_typ = systemtyp) %>% 
  dplyr::rename(pipe_diam = ds_height) %>% 
  dplyr::rename(geometry = geometry.x)


pdas_2 <- pdas_1 %>% left_join(pdas_age1[,c("id","Age","County")], by = "id")
unband_band <- read.csv("Assets - Rulesets - Material Banding Table v2.csv")
pdas_3 <- pdas_2 %>%
  left_join(unique(unband_band[,c(1,2)]),by = c("pipe_mat"="MaterialTable_UnbandedMaterial")) %>% 
  dplyr::mutate(pipe_mat = (pipe_mat = as.character(MaterialTable_BandedMaterial)))
unique(pdas_3$pipe_mat)
summary(pdas_3)
pdas_3$pipe_diam <- as.numeric(pdas_3$pipe_diam)
pdas_4 <- pdas_3 %>% 
  mutate(pipe_diam = ifelse(pipe_diam <= 75 & pipe_diam > 0,"<=75",
                            (ifelse(pipe_diam <= 150 & pipe_diam > 75,"80-150",
                                    (ifelse(pipe_diam <= 500 & pipe_diam > 150,">150",
                                            (ifelse(pipe_diam <= 0,"",
                                                    (ifelse(pipe_diam > 500,"",pipe_diam)))))))))) %>% 
  dplyr::select(-c(MaterialTable_BandedMaterial)) %>% 
  mutate(Category = ifelse(Age > 82,"S24","PDaS")) %>% 
  dplyr::rename(public_sewer_id = asset_id)

#infill county
#county <- st_read("G:\\Desktop\\STW PDaS\\Counties_and_Unitary_Authorities_December_2016_Full_Extent_Boundaries_in_England_and_Wales\\Counties_and_Unitary_Authorities_December_2016_Full_Extent_Boundaries_in_England_and_Wales.shp")
#st_write(county, "Counties_and_Unitary_Authorities_December_2016_Full_Extent_Boundaries_in_England_and_Wales.shp.csv")
#county <- county %>% dplyr::rename(county = ctyua16nm)

county <- st_read("G:\\Desktop\\STW PDaS\\CountyShapefile\\ST_HD_Counties.shp")
county <- county %>% dplyr::rename(county = SAP_LOCA_1)
county_sf <- st_as_sf(county, crs = 27700)
st_crs(county_sf) <- 27700
# county_sf <- county_sf %>% filter(county == "Shropshire"|
#                                   county == "Central East"|
#                                   county == "Warwickshire"|
#                                   county == "Central West"|
#                                   county == "Staffordshire"|
#                                   county == "Leicestershire"|
#                                   county == "Nottinghamshire N"|
#                                   county == "Nottinghamshire S"|
#                                   county == "Worcestershire"|
#                                   county == "Gloucestershire")
unique(county_sf$county)
pdas_4_sf <- st_as_sf(pdas_4,crs = 27700)
# Calculate_nearest
pdas_4_sfc <- st_centroid(pdas_4_sf)
nearest <- st_nearest_feature(pdas_4_sfc,county_sf)
nearest <- nearest %>% dplyr::rename(col.id = nearest)
nearest$row.id <- seq.int(nrow(nearest))

# tmap_mode("view")
# plot1 <- tm_shape(pdas_4_sfc[1:10,]) + tm_basemap("Esri.WorldStreetMap") 
# plot2 <- tm_shape(county_sf) + tm_polygons() + tm_basemap("Esri.WorldStreetMap")
# tmap_arrange(plot2,plot1)

pdas_4_sf$row.id <- seq.int(nrow(pdas_4_sf))
county_sf$col.id <- seq.int(nrow(county_sf))
# Get the names of the names of neighborhoods in buffer
pdas4sf_col.id <- left_join(nearest, as.data.frame(pdas_4_sf), by = "row.id")
combine <- left_join(pdas4sf_col.id, as.data.frame(county_sf[,c("county","col.id")]), by = "col.id")
unique(county_sf$county)

#filter to certain pipe types and materials
combine2 <- combine %>% dplyr::select(id,public_sewer_id,length,pipe_mat,pipe_diam,pipe_typ,Age,County,county,Category,geometry.x) %>% 
  dplyr::rename(geometry = geometry.x) %>% 
  mutate(County = ifelse(!is.na(County),as.character(County),as.character(county))) %>%
  mutate(pipe_typ = ifelse(pipe_typ == "F",as.character(pipe_typ),
                           (ifelse(pipe_typ == "S",as.character(pipe_typ),
                                   (ifelse(pipe_typ == "C", as.character(pipe_typ), NA)))))) %>% 
  mutate(pipe_mat = ifelse(pipe_mat == "BR", "", as.character(pipe_mat))) %>% 
  mutate(County = ifelse(is.na(County),as.character(county) , as.character(County))) %>%
  mutate(County = ifelse(County == "Worcestershire Gloucestershire", "Worcestershire/Gloucestershire",as.character(County))) %>%
  mutate(County = ifelse(County == "Nottinghamshire S" | County == "Nottinghamshire N", "Nottinghamshire",as.character(County))) %>%
  mutate(County = ifelse(County == "Central East" | County == "Central West", "Central",as.character(County))) %>%
  dplyr::select(-c(county,public_sewer_id)) 
unique(combine2$County)

combine2 <- combine2 %>% 
  mutate_at(c(3:5,7:8),as.character)
#deselct geomerty and transform length to factor
combine3 <- combine2[,1:8] %>% 
  mutate(lengthband = as.factor(paste("band",round(length/50)))) %>% 
  mutate(Age = as.factor(Age)) %>% 
  dplyr::select(-length)

combine3[combine3 == ""] <- NA
summary(combine3)
# 
# test <- na.omit(pdas_4[,-c(2,7])
# test <- test %>%
#   mutate_all(as.character)
# cor(test[complete.cases(test), ], method = "pearson")
#combine3$pipe_mat <- revalue(combine3$pipe_mat,c("CI"="2","CO"="3","PVC"="4","VC"="5"))
#combine3$pipe_diam <- revalue(combine3$pipe_diam,c("<=75"="1","80-150"="2",">150"="3"))
combine3$County <- revalue(combine3$County,  c(
  "Shropshire" = "1",
  "Powys" = "2",
  "Warwickshire" = "3",
  "Central" = "4",
  "Staffordshire" = "5",
  "Leicestershire" = "6",
  "Nottinghamshire" = "7",
  "Derbyshire" = "8",
  "Worcestershire/Gloucestershire" = "9"
)
)
calculate_mode <- function(x) {
  uniqx <- unique(na.omit(x))
  uniqx[which.max(tabulate(match(x, uniqx)))]
}

combination <- combine3[,-4] %>% 
  mutate(Age = ifelse(is.na(Age),calculate_mode(as.character(Age)),as.character(Age))) %>% #mode of age to infill
  mutate(Category = ifelse(is.na(Category),calculate_mode(as.character(Category)),as.character(Category))) %>% #mode of age to infill
  mutate_at(c(1:6),as.factor) %>% 
  mutate(Age = as.factor(Age)) %>%
  na.omit()  #%>% 
#dplyr::select(-Age)#remove age
# filter(pipe_mat == "1" | "2"| "3" | "4" | "5") %>% 
# filter(pipe_diam == "1" | "2" | "3")
summary(combination)

#split data for ML
set.seed(1)
train.base <- sample(1:nrow(combination),(nrow(combination)*0.85),replace = FALSE)
traindata.base <- combination[train.base, ]
testdata.base <- combination[-train.base, ]

# ################find the predictive value of material################################
# fitControl <- trainControl(method = "repeatedcv", number = 4, repeats = 4, search = "random")
# hyperparams <- expand.grid(n.trees = 20, 
#                            interaction.depth = seq(from = 1, to = 10,length.out = 10), 
#                            shrinkage = 0.1, 
#                            n.minobsinnode = 10)
# gbm.mod.base <- train(pipe_mat ~ ., data = traindata.base, method = "gbm",trControl = fitControl,verbose = FALSE,
#                       tuneGrid = hyperparams)
# gbm.pred.base <- predict(gbm.mod.base,newdata = testdata.base, type = "raw")
# print(gbm.pred.base)
# cm <-  confusionMatrix(gbm.pred.base, testdata.base$pipe_mat)
# print(cm)
# 
# 
# ################find the predictive value of diameter################################
# 
# set.seed(53)
# gbm.mod.base2 <- train(pipe_diam ~ ., data = traindata.base, method = "gbm",trControl = fitControl,verbose = FALSE,
#                       tuneGrid = hyperparams)
# gbm.mod.base2
# gbm.pred.base2 <- predict(gbm.mod.base2,newdata = testdata.base, type = "raw")
# print(gbm.pred.base2)
# summary(gbm.pred.base2)
# cm2 <-  confusionMatrix(gbm.pred.base2, testdata.base$pipe_diam)
# print(cm2)
# 
# 

#RF diameter######
set.seed(2)
rf.fit2 <- cforest(pipe_diam ~ Age + County + lengthband, data = traindata.base, controls = cforest_unbiased(ntree = 50, mtry = 4))
rf.fit2
cforestStats(rf.fit2)
#sort important variables
rev(sort(varimp(rf.fit2)))
rev(sort(varimp(rf.fit2, pre1.0_0 = TRUE)))
rf.pred2 <- predict(rf.fit2,newdata = testdata.base, type = 'response')
cm2.2 <-  confusionMatrix(rf.pred2, testdata.base$pipe_diam)
caret::confusionMatrix(rf.pred2, testdata.base$pipe_diam)
print(cm2.2)

summary(validation3)

#RF material
set.seed(2)
rf.fit3 <- cforest(pipe_mat ~ County + lengthband + Age, data = traindata.base, controls = cforest_unbiased(ntree = 50, mtry = 4))
rf.fit3
cforestStats(rf.fit3)
#sort important variables
rev(sort(varimp(rf.fit3)))
rev(sort(varimp(rf.fit3, pre1.0_0 = TRUE)))
rf.pred3 <- predict(rf.fit3,newdata = testdata.base, type = 'response')
cm2.3 <-  confusionMatrix(rf.pred3, testdata.base$pipe_mat)
caret::confusionMatrix(rf.pred3, testdata.base$pipe_mat)
print(cm2.3)

summary(validation3)

################validation for the other na containing rows################################

validation <- combine3[,-4] %>% 
  mutate(Age = ifelse(is.na(Age),calculate_mode(as.character(Age)),as.character(Age))) %>% #mode of age to infill
  mutate(Category = ifelse(is.na(Category),calculate_mode(as.character(Category)),as.character(Category))) %>% #mode of age to infill
  mutate_at(c(1:6),as.factor) %>% 
  mutate(Age = as.factor(Age)) %>% 
  anti_join(combination,by = "id")

filter(combine3, County == "2")
filter(combination, County == "2")
filter(validation, County == "2")
unique(validation$County)
unique(combination$County)

# predict diameter
library(tictoc)
tic()
split1 <-  split(validation,validation$County)

## predicting new diameter
predict_diameter <- function(x){
  for(j in 1:length(x)){
    output.data <- NULL
    test.output <- NULL
    test <- data.frame(x[[j]])
    test.output <-
      data.table(diameter = (predict(rf.fit2, newdata = test, type = "response")))
    test.output$County <- test$County
    test.output$id <- test$id
    if (j == 1) {
      output.data.test <- test.output
    } else
    {
      output.data.test <-
        rbind(output.data.test, test.output)
    }
  }
  return(output.data.test)
}

diameterx <- predict_diameter(split1)
# diameterx <- output.data.test
toc()

## predict new material
predict_material <- function(x) {
  for(j in 1:length(x)){
    output.data2 <- NULL
    test.output2 <- NULL
    test <- data.frame(x[[j]])
    test.output2 <-
      data.table(material = (predict(rf.fit3, newdata = test, type = "response")))
    test.output2$County <- test$County
    test.output2$id <- test$id
    if (j == 1) {
      output.data.test2 <- test.output2
    } else
    {
      output.data.test2 <-
        rbind(output.data.test2, test.output2)
      
    }
  }
  return(output.data.test2)
}

materialx <- predict_material(split1)
# materialx <- output.data.test2

## extracting the asset base
output.data2 <- rbindlist(split1, use.names = TRUE, fill = TRUE)

# merge results with ids
predicted <- output.data2 %>% left_join(diameterx, by = c("id","County")) %>% left_join(materialx, by = c("id","County"))
#predicted <- cbind(output.data2, diameter, material)

## add pipe_typ and ifelse material and diameter
validation2 <- validation %>% left_join(predicted[,c("id","material","diameter")], by = "id") %>% 
  mutate(pipe_mat = ifelse(is.na(pipe_mat),as.character(material),as.character(pipe_mat))) %>% 
  mutate(pipe_diam = ifelse(is.na(pipe_diam),as.character(diameter),as.character(pipe_diam))) %>% 
  dplyr::select(-c(diameter, material)) %>% 
  left_join(combine[,c("pipe_typ","id")], by = "id")

## bring back the data we cleaned but did not predict
non_predicted <- combine3[,-4] %>% 
  mutate(Age = ifelse(is.na(Age),calculate_mode(as.character(Age)),as.character(Age))) %>% #mode of age to infill
  mutate(Category = ifelse(is.na(Category),calculate_mode(as.character(Category)),as.character(Category))) %>% #mode of age to infill
  mutate_at(c(1:6),as.factor) %>% 
  mutate(Age = as.factor(Age)) %>%
  na.omit() %>% 
  left_join(combine3[,c("id","pipe_typ")],by = "id") # adds the pipe type

validation3 <- rbind.fill(validation2,non_predicted) %>% mutate_at(c(2:3,8),as.factor)
validation3$County <- revalue(validation3$County,c(
  "1" = "Shropshire",
  "2" = "Powys",
  "3" = "Warwickshire",
  "4" = "Central",
  "5" = "Staffordshire",
  "6" = "Leicestershire",
  "7" = "Nottinghamshire",
  "8" = "Derbyshire",
  "9" = "Worcestershire/Gloucestershire")
)
write.csv(validation3,"total assets.csv")

summary(validation3)


library(mlbench)
library(randomForest)
library(doMC)

# save the model to disk
saveRDS(rf.fit2, "./rf.fit2.rds")
saveRDS(rf.fit3, "./rf.fit3.rds")

# load the model
super_model <- readRDS("./final_model.rds")











