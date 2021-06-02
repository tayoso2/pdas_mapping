
# Packages ----------------------------------------------------------------

library(caret)
library(party)
library(mlbench)
library(doParallel)
library(foreach)
library(data.table)
library(dplyr)
library(sf)
library(units)
library(tmap)
library(ggplot2)
library(rgdal)
library(tictoc)
library(tidyr)
library(rgdal)


# So yeah the aim is to remove the proxy-road-sewers from the sprint_5_sewers 
# that are close to anything in the all_sewerln_with_attributes dataset. 
# I would go about this by first doing st_nearest_feature() to find the nearest mapped sewer 
# to the proxy sewer, and then doing st_distance() between those two vectors



# read building age data, year_lookup and rdata ----------------------------------------------------------------


link_one <- "G:/Desktop/STW PDaS/Phase 2 datasets/updated_building_polygons/"
link_two <- "G:/Desktop/STW PDaS/Phase 2 datasets/sprint_5_sewers/"
sewer <- st_read(paste0(link_two,"sprint_5_sewers.shp"))
unband_band <- read.csv("G:\\Desktop\\STW PDaS\\old stuff\\Assets - Rulesets - Material Banding Table v2.csv")
year_lookup <- read.csv(paste0(link_one,"year_lookup.csv"))

# convert to shpfile

# setwd("G:/Desktop/STW PDaS/Phase 2 datasets/All_Sewerln_With_Attributes/")
# dl <- readOGR(dsn=".", layer="All_Sewerln_With_Attributes")
# dl_shp <- dl
# dl_shp <- st_as_sf(dl_shp)

# st_write(dl_shp, "All_Sewerln_With_Attributes.shp")
saveRDS(dl_shp,"All_Sewerln_With_Attributes.rds")


# read in the shpfile

link_three <- "G:/Desktop/STW PDaS/Phase 2 datasets/All_Sewerln_With_Attributes/"
sewerln_shp <- readRDS(paste0(link_three,"All_Sewerln_With_Attributes.rds"))

sewerln_shp <- sewerln_shp[,c(1:8,64:70)]

# data processing

proxy_sewer <- sewer %>% filter(rod_flg == "1")
mapped_sewer <- sewer %>% filter(rod_flg == "0") %>% 
  mutate(Unbnd_D = as.integer(as.character(Unbnd_D)))

mapped_sewer$Unbnd_D %>% summary()
mapped_sewer$Unbnd_D %>% str()
mapped_sewer$Unbnd_M %>% summary()

# Functions ---------------------------------------------------------------

# rollmeTibble <- function(df1, values, col1, method) {
#   
#   df1 <- df1 %>% 
#     mutate(merge = !!sym(col1)) %>% 
#     as.data.table()
#   
#   values <- as_tibble(values) %>% 
#     mutate(band = row_number()) %>% 
#     mutate(merge = as.numeric(value)) %>% 
#     as.data.table()
#   
#   setkeyv(df1, c('merge'))
#   setkeyv(values, c('merge'))
#   
#   Merged = values[df1, roll = method]
#   
#   Merged
# }
# 
# # These are the decided classes
# diameterclasses <-c(0, 100, 150, 225, 300, 375, 450 ,525, 600, 675, 750, 825,
#                     900, 975, 1050, 1200, 1350, 1500)
# 
# mapped_sewer_bd <- rollmeTibble(df1 = mapped_sewer,
#                                 values = diameterclasses,
#                                 col1 = "Unbnd_D",
#                                 method = 'nearest') %>%
#   select(-band, -merge, -geometry) %>% 
#   rename(Bnd_D = value)
# 
# # Check 0 counts
# mapped_sewer_bd %>% 
#   as_tibble() %>% 
#   count(Bnd_D)
# 
# # check if tag is the uid and merge the geom back
# 
# mapped_sewer_bd$Tag %>% unique()
# 
# mapped_sewer_bd <- mapped_sewer_bd %>% 
#   left_join(mapped_sewer[,"Tag"], by = "Tag")


# get the nearest mapped sewer mains to sewerln -----------------------------------------------

st_crs(proxy_sewer) <- 27700
st_crs(sewerln_shp) <- 27700
sewerln_shp <- sewerln_shp %>% rename(Tag_sewerln = Tag)

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
}
get_one_to_one_distance <- function(x, y, x.id, y.id) {
  
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
  x$index <- seq.int(nrow(x))
  y2 <- as.data.frame(x[,c(y.id,"index")]) %>% inner_join(y[,y.id], by = y.id)
  # y2 <- merge(as.data.frame(x[,c(y.id,"index")]), y[,y.id], by = y.id,all.x=FALSE, all.y=FALSE)
  y2 <- st_as_sf(y2, crs = 27700)
  
  # Compute index:index distance matrix
  for (i in 1:nrow(x2)){
    x2$length_to_centroid[[i]] <- st_distance(x2[i, x.id], y2[i, y.id])
  }
  x3 <- x2[,c(x.id,y.id,"length_to_centroid")]
  return(x3)
}


# apply above fxns to find the distance between the nearest proxy sewer and the sewerln_with_attributes.shp

tic()
proxy_near_sewerln <- get_nearest_features(proxy_sewer,sewerln_shp,"Tag","Tag_sewerln","Fjunction")
toc()

# remove the tag ids from the "sewer" data above and save the new sewer dataset.

trunc_sewer <- sewer %>% anti_join(as.data.frame(proxy_near_sewerln_three), by = "Tag")

tic()
dist_xy <- get_one_to_one_distance(proxy_near_sewerln,sewerln_shp,"Tag","Tag_sewerln")
toc()

dist_xy_two <- dist_xy %>% filter(length_to_centroid <= 5)

# remove the tag ids from the "sewer" data above and save the new sewer dataset.

trunc_sewer_two <- sewer %>% anti_join(as.data.frame(dist_xy_two), by = "Tag")

st_write(trunc_sewer_two,"updated_sprint_5_sewers_2.shp")

# test check

proxy_near_sewerln %>% filter(Tag == "28992245")
proxy_sewer %>% filter(Tag == "119334846")
proxy_near_sewerln %>% filter(Tag == "1181681935")

proxy_near_sewerln_two %>% filter(Tag == "28992245")
proxy_near_sewerln_three %>% filter(Tag == "28992245")
proxy_near_sewerln_three %>% filter(Tag == "29017466")


# get the attributes of the nearest sewers to the proxy sewers and plot ........................................................

dist_xy # these are my proxy sewers


# get the attributes summary for the Tag_sewerln by joining to the original dataset

nearest_sewers_to_proxy <- sewerln_shp %>% inner_join(as.data.frame(dist_xy), by = "Tag_sewerln") %>% 
  rename(geometry = geometry.x) %>% 
  rename(proxy_Tag = Tag) %>%
  select(-geometry.y)

# band unbanded materials

unband_band <- read.csv("G:\\Desktop\\STW PDaS\\old stuff\\Assets - Rulesets - Material Banding Table v2.csv")
nearest_sewers_to_proxy <- nearest_sewers_to_proxy %>%
  left_join(unique(unband_band[,c(1,2)]),by = c("Tp_materia" = "MaterialTable_UnbandedMaterial")) %>% 
  dplyr::mutate(Tp_materia = (Tp_materia = as.character(MaterialTable_BandedMaterial))) %>% 
  select(-MaterialTable_BandedMaterial) %>% 
  mutate(Tp_materia = as.factor(Tp_materia))

unique(nearest_sewers_to_proxy$Tp_materia)

# clean the diameter var

nearest_sewers_to_proxy$Complex_la <- gsub('\\D',"",nearest_sewers_to_proxy$Complex_la)
nearest_sewers_to_proxy$Complex_la <- as.numeric(nearest_sewers_to_proxy$Complex_la)
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

# These are the decided classes
diameterclasses <-c(0, 50, 100, 150, 225, 300, 375, 450 ,525, 600, 675, 750, 825,
                    900, 975, 1050, 1200, 1350, 1500)

nearest_sewers_to_proxy_2 <- rollmeTibble(df1 = nearest_sewers_to_proxy,
                                values = diameterclasses,
                                col1 = "Complex_la",
                                method = 'nearest') %>%
  select(-band, -merge, -geometry) %>% 
  rename(Bnd_D = value)

summary(nearest_sewers_to_proxy_2)

# get count

nearest_sewers_to_proxy_2 %>% 
  count(Tp_purpose) %>% 
  mutate(prop = 100* (n/sum(n)))

nearest_sewers_to_proxy_2 %>% 
  count(Bnd_D)%>% 
  mutate(prop = 100* (n/sum(n)))

nearest_sewers_to_proxy_2 %>% 
  count(Tp_materia)%>% 
  mutate(prop = 100* (n/sum(n)))

# plot the diameter

nearest_sewers_to_proxy_2 %>% 
  count(Tp_purpose) %>% 
  mutate(prop = 100* (n/sum(n))) %>% 
  ggplot(aes(x=Tp_purpose, y=n)) +
  geom_bar(stat="identity", fill="#eb5e34")+
  theme_minimal()


nearest_sewers_to_proxy_2 %>% 
  count(Bnd_D)%>% 
  mutate(prop = 100* (n/sum(n))) %>% 
  ggplot(aes(x=Bnd_D, y=n)) +
  geom_bar(stat="identity", fill="#eb5e34")+
  theme_minimal()


nearest_sewers_to_proxy_2 %>% 
  count(Tp_materia)%>% 
  mutate(prop = 100* (n/sum(n))) %>% 
  ggplot(aes(x=Tp_materia, y=n)) +
  geom_bar(stat="identity", fill="#eb5e34")+
  theme_minimal()




