
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
library(tictoc)
library(pbapply)


# data load function ------------------------------------------------------

sx_read <- function(shape) {
        
        # Converts to EPSG 27700 and prevents strings as factors.
        
        st_read(shape,
                stringsAsFactors = FALSE) %>% 
                st_transform(27700)
        
}

# load data, rdata, rds  ----------------------------------------------------------------
link_one <- "D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/for will/updated_building_polygons\\"
link_two <- "D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/for will/segmented_road_proxies_distances_FCS\\"
link_h <- "D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/for will/ModalAttributes\\"
link_e <- "D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/for will/Postcode\\"
#link_nbp <- "D:/STW PDAS/Phase 2 datasets/sprint_4_props_with_atts/"

proxy_sewer <- sx_read(paste0(link_two,"segmented_road_proxies_distances_FCS.shp"))
unband_band <- read.csv("D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/for will/old stuff\\Assets - Rulesets - Material Banding Table v2.csv")
year_lookup <- read.csv(paste0(link_one,"year_lookup.csv"))
modal_atts <- sx_read(paste0(link_h,"ModalAttributes_Merged.shp"))
the_postcode_2 <- st_read(paste0(link_e,"the_postcode_2.shp"))
#phase_2_props_with_age <- st_read(paste0(link_nbp,"phase_2_props_with_age.shp"))

propgroups <- sx_read("D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/Will_proxy_attribute_assigning_progress/new_polys_propgroups_whole_postcode/new_polys_propgroups_whole_postcode.shp")

fids_all <- fread("D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/Will_proxy_attribute_assigning_progress/new_polys_stw_wide_system_preds/new_polys_stw_wide_system_preds.csv")

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
# proxy_sewer <- proxy_sewer[1:5,]


# match up sewers to nearest sewer + propgroup ----------------------------

proxy_sewers_filtered <- proxy_sewer %>% 
        filter(assmd_s != "close_to_F_and_S")

modal_atts_no_roads <- modal_atts %>% 
        filter(road_flag == 0)

# Can get from nearest mapped sewer main:
# 1. Neighbour diameter
# 2. Neighbour material

tictoc::tic()
nearest_sewers_no_roads <- st_nearest_feature(proxy_sewers_filtered,
                                              modal_atts_no_roads)
tictoc::toc()


# Can get from propgroup:
# 1. Postcode
# 2. System type prediction
# 3. Building age

tictoc::tic()
nearest_propgroup <- st_nearest_feature(proxy_sewers_filtered,
                                        propgroups) 
toctoc::toc()

proxies_nearest <- proxy_sewers_filtered %>% 
        mutate(nearest_sewers = nearest_sewers_no_roads,
               nearest_propgroup = nearest_propgroup) 

st_drop_geometry(proxies_nearest) %>% 
        fwrite("D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/proxies_nearest_features.csv")

proxies_nearest_csv <- fread("D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/proxies_nearest_features.csv")

proxy_sewers_filtered_nrst <- proxy_sewers_filtered %>% 
        left_join(proxies_nearest_csv %>% 
                          select(uniqu_d,
                                 nearest_sewers,
                                 nearest_propgroup),
                  by = "uniqu_d")

proxies_nearest_joined <- proxy_sewers_filtered_nrst %>% 
        select(uniqu_d,
               assmd_s,
               nearest_sewers,
               nearest_propgroup) %>% 
        left_join(modal_atts %>%
                          st_drop_geometry() %>% 
                          mutate(row_num = row_number()) %>% 
                          select(Tag,
                                 n2_D,
                                 n2_M,
                                 row_num),
                  by = c("nearest_sewers" = "row_num")) %>% 
        left_join(propgroups %>% 
                          st_drop_geometry() %>% 
                          select(propgroup) %>% 
                          mutate(row_num = row_number()),
                  by = c("nearest_propgroup" = "row_num")) %>% 
        select(-nearest_sewers,
               -nearest_propgroup)

# get propgroup attributes from fids

propgroups_attributes <- fids_all %>% 
        group_by(propgroup) %>% 
        summarise(building_age = get_mode(age),
                  system_pred = get_mode(system_pred))

# join propgroup attributes on to proxies

proxies_age_system <- proxies_nearest_joined %>% 
        left_join(propgroups_attributes,
                  by = "propgroup")

# lines to duplicate & offset...

to_duplicate <- proxies_age_system %>% 
        filter(assmd_s == "both",
               system_pred == "separate")


# offset lines ------------------------------------------------------------

to_duplicate <- to_duplicate[1:20000, ]

to_duplicate$grouping <- rep(1:72, each = 8000)[1:572926]

to_duplicate_split <- to_duplicate %>% 
        group_split(grouping)

tic()
dup_geom <- pblapply(to_duplicate_split,
                   function(x) {
        
                x$sewer_index <- 1:nrow(x)
                           
           for (i in x$sewer_index) {
                   # add 2 to all
                   x$geom[[i]][[1]] <- 
                           x[["geometry"]][[i]][[1]]+(-2)
           }
                
                x
                   })
toc() # completes in under 4 minutes

# tic()
# for (i in to_duplicate$sewer_index) {
#         # add 2 to all
#         to_duplicate$geom[[i]][[1]] <- to_duplicate[["geometry"]][[i]][[1]]+(-2)
# }
# toc() # incomplete after 200minutes

dup_geom_bound <- bind_rows(dup_geom)

print(dup_geom_bound) # check if the duplication occurred, new geometry columns is "geom"

# make all duplicates surface
dup_geom_bound <- st_transform(dup_geom_bound, 27700) %>% 
        mutate(final_system = "S")


# label duplicated lines --------------------------------------------------


to_duplicate <- to_duplicate %>% 
        mutate(final_system = "F")

separate_pairs <- bind_rows(to_duplicate,
                           dup_geom_bound) %>% 
        rename(old_unique_id = uniqu_d)


# join to rest of proxies -------------------------------------------------

new_proxies <- proxies_age_system %>% 
        filter(!(assmd_s == "both" & system_pred == "separate") |
               is.na(system_pred)) %>% 
        mutate(final_system = assmd_s,
               final_system = case_when(
                       final_system == "both" ~ "C",
                       TRUE ~ final_system)) %>% 
        rename(old_unique_id = uniqu_d) %>% 
        bind_rows(separate_pairs) %>% 
        mutate(new_unique_id = str_pad(
                as.character(
                        row_number()), 
                width = 8,
                side = "left",
                pad = "0"))

# new_proxies %>% 
#         st_write("D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/offset_full_proxies\\full_offset_proxies_working.shp")

new_proxies <- sx_read("D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/Will_proxy_attribute_assigning_progress/full_offset_proxies_working\\full_offset_proxies_working.shp")

new_proxies <- new_proxies %>% 
        left_join(st_drop_geometry(propgroups) %>% 
                          select(propgrp = propgroup,
                                 postcode = PC_SingleS))

new_proxies %>% as.data.frame() %>% mutate(fnl_sys = as.factor(fnl_sys)) %>% select(fnl_sys) %>% summary()

# new_proxies_pre_tayo <- new_proxies

# write out for line drawing ----------------------------------------------


# new_proxies %>% 
#         select(new_unique_id,
#                old_unique_id,
#                final_system) %>% 
#         st_write("D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/offset_full_proxies\\full_offset_proxy_geometries.shp")


# add nearest prop age as res_buil_1 using nearest  ------------------------------------------------


# proxy_sewer_with_age <- get_nearest_features_ycentroid(proxy_sewer,phase_2_props_with_age[,c("fid_1","res_buil_1")],"id_0","fid_1","res_buil_1")
# 
# # OR add nearest prop age as res_buil_1 using a buffer around props.
# proxy_sewer_buffer <- st_buffer(proxy_sewer[,"id_0"],30)
# proxy_sewer_buffer <- proxy_sewer_buffer %>% rename(id_0_buffer = id_0)
# proxy_sewer_with_age <-
#         get_intersection_get_mode(proxy_sewer_buffer,
#                                   phase_2_props_with_age,
#                                   "id_0_buffer",
#                                   "fid_1",
#                                   "id_0_buffer",
#                                   "res_buil_1") # missing rows are due to x being too far from y
# 
# proxy_sewer_with_age <- proxy_sewer %>%
#         left_join(proxy_sewer_with_age,c("id_0"="id_0_buffer")) %>% 
#         st_sf() %>% 
#         st_set_crs(27700)
# 
# # get the nearest n24 M and D
# proxy_sewer_with_age_dm <- get_nearest_features(proxy_sewer_with_age,modal_atts[,c("Tag_1", "n2_D")],"id_0","Tag_1","n2_D")
# modal_atts <- modal_atts %>% as.data.frame() %>% select(-geometry)
# proxy_sewer_with_age_dm <- proxy_sewer_with_age_dm %>% 
#         left_join(modal_atts[,c("Tag_1","n2_M")], by = "Tag_1")

# These are the diameter classes ------------------------------------------------------
diameterclasses <-c(0,225,750,1500)
new_proxies$n2_D <- as.character(new_proxies$n2_D)
new_proxies$n2_D <- new_proxies$n2_D %>% replace_na(0)
new_proxies$n2_D <- as.integer(new_proxies$n2_D)

proxy_sewer_with_age_dm_bd <- rollmeTibble(df1 = new_proxies,
                                           values = diameterclasses,
                                           col1 = "n2_D",
                                           method = 'nearest') %>%
        dplyr::select(-band, -merge, -n2_D) %>% 
        dplyr::rename(Bnd_D_2 = value)

proxy_sewer_with_age_dm_bd %>% summary()

# band unbanded materials ---------------------------------------------------------------
proxy_sewer_with_age_dm_bd_bm <- proxy_sewer_with_age_dm_bd %>%
        left_join(unique(unband_band[,c(1,2)]),
                  by = c("n2_M" = "MaterialTable_UnbandedMaterial")) %>% 
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

mapped_ml_model <- go_into_model %>% 
        as_tibble() %>%
        rename(Type = fnl_sys) %>% 
        rename(res_buil_1 = bldng_g) %>% 
  # select(Bnd_D_2,Type,res_buil_1,Bnd_M_2, aprx_pstcd_2ch, aprx_pstcd_3ch, Cnty_st) %>%
  filter(!is.na(Type),!is.na(res_buil_1),
         !is.na(aprx_pstcd_2ch),aprx_pstcd_2ch != 0,
         !is.na(Bnd_D_2), Bnd_D_2 != 0,
         !is.na(Bnd_M_2), Bnd_M_2 != "") %>% 
  mutate(Bnd_D_2 = as.factor(as.character(Bnd_D_2)),
         Bnd_M_2 = as.factor(Bnd_M_2),
         aprx_pstcd_2ch = as.factor(aprx_pstcd_2ch),
         aprx_pstcd_3ch = as.factor(aprx_pstcd_3ch),
         Cnty_st = as.factor(Cnty_st),
         res_buil_1 = as.factor(res_buil_1))

mapped_ml_model$Bnd_M_2 <- factor(mapped_ml_model$Bnd_M_2)
mapped_ml_model %>% summary()



# load the models --- 1st prediction
mapped_ml_model_rffit_diam <- readRDS("D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/ps_attrs_models/mapped_ml_model_rffit_diam_4.rds") # 
mapped_ml_model_rffit_mat <- readRDS("D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/ps_attrs_models/mapped_ml_model_rffit_mat_4.rds") #


# Remove assets that don't match the model
mapped_ml_model_2 <- remove_anomalies_cat_pred(mapped_ml_model_rffit_diam$trainingData,
                                               mapped_ml_model,
                                               "aprx_pstcd_2ch")

# Predict and test the model
mapped_ml_model_pred2_diam <- predict(mapped_ml_model_rffit_diam, 
                                      newdata = mapped_ml_model_2)

mapped_ml_model_pred2_mat <- predict(mapped_ml_model_rffit_mat, 
                                     newdata = mapped_ml_model_2)

# join back to master data
result_pred_1 <- cbind(CONFIDENCE = "A",
                       DIAMETE = mapped_ml_model_pred2_diam,
                      MATERIA = mapped_ml_model_pred2_mat,
                      mapped_ml_model_2)
pred_2 <- go_into_model %>% # master data
  anti_join(result_pred_1, by = "nw_nq_d") %>% 
  distinct()

# save.image("D:/STW PDAS/RDATA/offset_proxies.rdata")
# load("D:/STW PDAS/RDATA/offset_proxies.rdata")

# infill mat and diam for the remaining assets
pred_2_md <- pred_2 %>% 
  mutate(CONFIDENCE = "D",
         Bnd_M_2 = as.factor(Bnd_M_2),
         MATERIA = as.character(Bnd_M_2),
         DIAMETE = as.integer(as.character(Bnd_D_2)),
         MATERIA = ifelse(nchar(MATERIA) >= 2,as.character(Bnd_M_2),"VC"),
         # MATERIA = ifelse(MATERIA == "NA","VC",as.character(MATERIA)),
         DIAMETE = ifelse(is.na(Bnd_D_2) | Bnd_D_2 == 0,225,as.integer(Bnd_D_2)),
         MATERIA = as.factor(MATERIA),
         DIAMETE = as.factor(as.character(DIAMETE))) %>% 
  as_tibble() %>%
  rename(Type = fnl_sys) %>% 
  rename(res_buil_1 = bldng_g) %>% 
  select(CONFIDENCE,MATERIA,DIAMETE, everything())

pred_2_md$MATERIA <- pred_2_md$MATERIA %>% replace_na("VC")
pred_2_md %>% summary()

# merge it all up
pred_2_md %>% as.data.frame() %>% mutate(Type = as.factor(Type)) %>% select(Type) %>% summary()
result_pred_1 %>% as.data.frame() %>% mutate(Type = as.factor(Type)) %>% select(Type) %>% summary()
proxyful <- plyr::rbind.fill(pred_2_md,result_pred_1) %>% 
  select(-c(Tag.y,nrst_p_.y,geometry,Bnd_M_2,Bnd_D_2,n2_M)) %>% 
  rename(Tag = Tag.x) %>% 
  rename(nrst_p_ = nrst_p_.x) %>% 
  left_join(new_proxies[,c("nw_nq_d")], by = "nw_nq_d")  %>% 
  mutate(Type = as.factor(Type)) %>% 
  st_sf() %>% st_set_crs(27700)


summary(proxyful)
proxyful %>% saveRDS("D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/proxy_asset_base_atts.rds")
proxyful %>% st_write("D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/proxy_asset_base_atts.shp")

# # Load model 2--------------------------------------------------------------
# 
# mapped_ml_model_rffit_diam2 <- readRDS("D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/ps_attrs_models/mapped_ml_model_rffit_diam_4.rds") # 
# mapped_ml_model_rffit_mat2 <- readRDS("D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/ps_attrs_models/mapped_ml_model_rffit_mat_4.rds") #
# 
# # Diam
# rf.pred2 <- predict(mapped_ml_model_rffit_diam2, newdata = pred_2)
# 
# # Mat
# rf.predMAT2 <- predict(mapped_ml_model_rffit_mat2, newdata = pred_2)
# 
# 
# # join back to master data and prep for model 3
# result_pred_2 <- cbind(Confidence = "B",DIAMETE = rf.pred2,MATERIA = rf.predMAT2,pred_2) # model 2 result
# pred_3 <- pdas_da_pc_ma_5 %>% 
#   anti_join(result_pred_1, by = "rowguid") %>%
#   anti_join(result_pred_2, by = "rowguid") %>%
#   distinct()
# 
# pred_3 <- pred_3 %>% 
#   mutate(n2_M = as.character(n2_M)) %>% 
#   as_tibble() %>% 
#   filter(!is.na(PURPOSE),!is.na(n2_Y),!is.na(n2_M),n2_M != "",n2_D != 0) %>% # Remove any diameters with 0 counts
#   mutate(n2_D = as.factor(n2_D),
#          PURPOSE = as.factor(PURPOSE),
#          n2_M = as.factor(n2_M),
#          n2_Y = as.numeric(n2_Y),
#          PROPERTY_TYPE = as.factor(PROPERTY_TYPE),
#          County = as.factor(County),
#          DAS = as.factor(ID_DA_CODE)) %>% 
#   select(-MaterialTable_BandedMaterial) 
# 
# pred_3 %>% 
#   as_tibble() %>% 
#   sapply(function(x) sum(is.na(x)))
# 
# summary(pred_3)
# 
# 
# # infill leftover assets with the modes
# # ....
# 
# 
# 
# 
# 
