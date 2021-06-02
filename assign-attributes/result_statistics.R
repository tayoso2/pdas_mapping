

library(sf)
library(dplyr)
library(magrittr)
library(ggplot2)

link_f_o <- "D:/STW PDAS/Phase 2 datasets/Flag overlap/"
pdas <- st_read(paste0(link_f_o,"stw_region_pdas_flagged.shp"))
s24 <- st_read(paste0(link_f_o,"stw_region_s24_flagged.shp"))

pdas$length <- st_length(pdas$geometry)
s24$length <- st_length(s24$geometry)

pdas %>% summary() # 100 VC
s24 %>% summary() # 150 VC
pdas$length %>% sum() # 26700 km
s24$length %>% sum() # 45000 km

# pdas %>% mutate(length = as.numeric(length)) %>%  filter(length < 0.1)

# plot the predicted assets ###############

## pdas
## Materials
for(purposes in c("C","F","S")) {
  print(ggplot(pdas %>% 
                 filter(PURPOSE == !!purposes)) +
          geom_bar(aes(x = MATERIA),fill = "#eb5e34") +
          scale_y_continuous(
            limits = c(0,750000),
            labels = function(x) {
              paste0(x / 10 ^ 3, 'K')
            }
          )  + 
          ggtitle(paste0("Type of pipe: ",purposes)) +
          ylab("Number of pipes") + 
          xlab("Pipe Material"))
}

## Diameter
for(purposes in c("C","F","S")) {
  print(ggplot(pdas %>% 
                 filter(PURPOSE == !!purposes)) +
          geom_histogram(aes(x = DIAMETE),fill = "#eb5e34") +
          scale_y_continuous(
            limits = c(0,500000),
            labels = function(x) {
              paste0(x / 10 ^ 3, 'K')
            }
          )  + 
          ggtitle(paste0("Type of pipe: ",purposes)) + 
          xlim(1,250))
}



##s24

## Materials
for(purposes in c("C","F","S")) {
  print(ggplot(s24 %>% 
                 filter(PURPOSE == !!purposes)) +
          geom_bar(aes(x = MATERIA),fill = "#eb5e34") +
          scale_y_continuous(
            limits = c(0,500000),
            labels = function(x) {
              paste0(x / 10 ^ 3, 'K')
            }
          )  + 
          ggtitle(paste0("Type of pipe: ",purposes)) +
          ylab("Number of pipes") + 
          xlab("Pipe Material")) 
}   

## Diameter
for(purposes in c("C","F","S")) {
  print(ggplot(s24 %>% 
                 filter(PURPOSE == !!purposes)) +
          geom_histogram(aes(x = DIAMETE),fill = "#eb5e34") +
          scale_y_continuous(
            limits = c(0,500000),
            labels = function(x) {
              paste0(x / 10 ^ 3, 'K')
            }
          )  + gg +
          ggtitle(paste0("Type of pipe: ",purposes)) + 
          xlim(1,250))
}






##################################### -----------------------------------------------------------------------

save_me_2 <- readRDS("D:/STW PDAS/Phase 2 datasets/proxy_bearing_and_attrs.rds")
save_me_2 <- mutate(save_me_2, length = st_length(geometry))
save_me_2 %>% summary()

# diameter stat
save_me_2 %>% filter(DIAMETE == "225") %>% nrow() /nrow(save_me_2)

# material stat
save_me_2 %>% filter(MATERIA == "VC")  %>% nrow() /nrow(save_me_2)

# system type stat
save_me_2 %>% as.data.frame() %>% group_by(sys_type) %>% summarise(length = sum(as.integer(length)))


# plot the predicted assets ###############

## pdas
## Materials

# stacked bar plot
save_me_2 %>% as.data.frame() %>% 
  group_by(sys_type,MATERIA) %>% 
  tally() %>% 
  ggplot(aes(x = sys_type, y = n)) +
  geom_col(aes(fill = MATERIA)) +
  scale_y_continuous(
    # limits = c(0,1000000),
    labels = function(x) {
      paste0(x / 10 ^ 3, 'K')
    }
  )  +
  # ggtitle(paste0("Type of sewer: ", purposes)) +
  ylab("Number of proxy sewers") +
  xlab("Sewer Material")


# individual plots
for(purposes in c("C","F","S")) {
  print(ggplot(save_me_2 %>% 
                 filter(sys_type == !!purposes)) +
          geom_bar(aes(x = MATERIA),fill = "#eb5e34") +
          scale_y_continuous(
            limits = c(0,1000000),
            labels = function(x) {
              paste0(x / 10 ^ 3, 'K')
            }
          )  + 
          ggtitle(paste0("Type of sewer: ",purposes)) +
          ylab("Number of proxy sewers") + 
          xlab("Sewer Type"))
}

## Diameter

# stacked bar plot
save_me_2 %>% as.data.frame() %>% 
  group_by(sys_type,DIAMETE) %>% 
  tally() %>% 
  ggplot(aes(x = sys_type, y = n)) +
  geom_col(aes(fill = DIAMETE)) +
  scale_y_continuous(
    # limits = c(0,1000000),
    labels = function(x) {
      paste0(x / 10 ^ 3, 'K')
    }
  )  +
  # ggtitle(paste0("Type of sewer: ", purposes)) +
  ylab("Number of proxy sewers") +
  xlab("Sewer Type")

for(purposes in c("C","F","S")) {
  print(ggplot(pdas %>% 
                 filter(PURPOSE == !!purposes)) +
          geom_histogram(aes(x = DIAMETE),fill = "#eb5e34") +
          scale_y_continuous(
            limits = c(0,500000),
            labels = function(x) {
              paste0(x / 10 ^ 3, 'K')
            }
          )  + 
          ggtitle(paste0("Type of pipe: ",purposes)) + 
          xlim(1,250))
}


# get property group number

to = stw_region_s24 %>% as.data.frame() %>% group_by(drwng__) %>%
  mutate(rown = row_number()) %>% 
  arrange(desc(rown)) %>% 
  select(drwng__, rown)

s24$YEARLAI %>% as.data.frame() %>% summary()
pdas$YEARLAI %>% as.data.frame() %>% summary()


# test pipe_q_d (UNIQUE ID)
## load shapeiles

# pdas/s24*
pdas_final <- st_read("D:/STW PDAS/Phase 2 datasets/Flag overlap/stw_region_pdas_flagged.shp")
st_geometry(pdas_final) <- NULL
pdas_final %>% arrange(pip_q_d)
pdas_final %>% arrange(desc(pip_q_d))
s24_final <- st_read("D:/STW PDAS/Phase 2 datasets/Flag overlap/stw_region_s24_flagged.shp")
st_geometry(s24_final) <- NULL
s24_final %>% arrange(pip_q_d)
s24_final %>% arrange(desc(pip_q_d))

# proxies* 
proxies <- st_read("D:/STW PDAS/Phase 2 datasets/sewer_with_proxy/updated_proxy_bearing_atts_14122020.shp")
st_geometry(proxies) <- NULL
proxies %>% arrange(pip_q_d)
proxies %>% arrange(desc(pip_q_d))