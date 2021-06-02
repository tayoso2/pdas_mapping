library(readxl)
library(dplyr)
library(magrittr)
library(sf)
library(reshape2)
library(data.table)


# Data Load ------------------------------------------------------------------->

## data with the ppt type
PdasPath1 <- "C:/Users/TOsosanya/Desktop/STW PDAS 2/ModalandNearestNeighbour/PDaS_with_property_type.csv"
pdas1 <- read.csv(PdasPath1)
pdas <- pdas1 %>% select(PIPEID,LENGTH,PURPOSE,County,dom_prop100) %>% #there is pipegroup
  dplyr::rename(PROPERTY_TYPE = dom_prop100)
pdas$PIPETYP <- "pdas"

s24Path1 <- "C:/Users/TOsosanya/Desktop/STW PDAS 2/ModalandNearestNeighbour/s24_with_property_type.csv"
s241 <- read.csv(s24Path1)
s24 <- s241 %>% select(Tag,Length,System,County,dom_prop100) %>% 
  dplyr::rename(PROPERTY_TYPE = dom_prop100) %>% # no pipegroup
  dplyr::rename(PIPEID = Tag) %>% 
  dplyr::rename(LENGTH = Length) %>% 
  dplyr::rename(PURPOSE = System)
s24$PIPETYP <- "s24"

# remove anomalous length
survey <- plyr::rbind.fill(pdas,s24)
summary(survey)
survey_ <- survey %>% filter(LENGTH > 1,PROPERTY_TYPE != "") %>% 
  filter(LENGTH < 5*mean(LENGTH), !is.na(PROPERTY_TYPE)) %>% 
  mutate(PURPOSE = ifelse(PURPOSE == "F", "C","S")) %>% 
  #filter(PROPERTY_TYPE != "") %>% 
  na.omit() %>% as.data.table()

survey_$County <- gsub("Worcestershire/Gloucestershire","Worcestershire Gloucestershire",survey_$County)
summary(survey_)



## load in pdas and s24 geometry for surveyed assets
PdasPath1 <- "C:/Users/TOsosanya/Desktop/STW PDAS 2/ModalandNearestNeighbour/PDASwithnn/PDASwithnn_merged.shp"
pdas1 <- st_read(PdasPath1, stringsAsFactors = F)
pdas1$PIPETYP <- "pdas"
pdas1 <- pdas1 %>% select(PIPEID)

s24Path1 <- "C:/Users/TOsosanya/Desktop/STW PDAS 2/ModalandNearestNeighbour/s24withnn/s24withnn_merged.shp"
s241 <- st_read(s24Path1, stringsAsFactors = F)
s241$PIPETYP <- "s24"
s241 <- s241 %>% select(PIPEID) 
pdas_s24 <- rbind(pdas1,s241)
survey_x <- left_join(survey_, pdas_s24, by = "PIPEID") # merge ppt_type and geom

# convert to epsg 27700 in preparation for finding nearest notional asset "length"
survey_x <- st_as_sf(survey_x, crs = 27700)



# find the nearest survey pipes to the notional survey pipes ------------------------->
## combine notional s24 and pdas 
path2 = "C:/Users/TOsosanya/Desktop/STW PDAS 2/ModalandNearestNeighbour/DataflowOutputs/DataflowOutputs/shapefiles"
building_grp_sf <- st_read(paste0(path2,"/test_full.shp"))

PdasPath2 <- "C:/Users/TOsosanya/Desktop/STW PDAS 2/ModalandNearestNeighbour/Notionals_of_PDaS_areas.csv"
s24Path2 <- "C:/Users/TOsosanya/Desktop/STW PDAS 2/ModalandNearestNeighbour/Notionals_of_S24_areas.csv"
notional_pdas <- read.csv(PdasPath2)
notional_s24 <- read.csv(s24Path2)
notionals <- rbind(notional_s24, notional_pdas) %>% dplyr::select(fid, Length)
notional_assets <- notionals %>% left_join(building_grp_sf, by = "fid")
notional_assets <- st_as_sf(notional_assets, crs = 27700)

# find nearest features - surveyed and notional assets
Get_Nearest_Features <- function(x, y, pipeid, id, feature) {
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
  y2 <- dplyr::select(y, id, feature, rowguid)
  st_geometry(y2) <- NULL
  x2 <- x %>% left_join(as.data.frame(y2), by = "rowguid")
  output <- x2
  
  # revert column names
  colnames(output)[which(names(output) == "id")] <- pipeid
  colnames(output)[which(names(output) == "feature")] <- feature
  print(output)
}
pair <- Get_Nearest_Features(survey_x,notional_assets,"PIPEID","fid","Length")



# Get calibration factor ------------------------------------------------------------------>
st_geometry(pair) <- NULL
cf <- pair %>% dplyr::rename(notional_length = Length) %>%
  dplyr::rename(surveyed_length = LENGTH) %>%
  dplyr::select(id.x,surveyed_length, notional_length, PURPOSE, County, PROPERTY_TYPE, PIPETYP) %>% 
  dplyr::mutate(calibration = as.numeric(surveyed_length/notional_length)) %>% #surveyed/notional
  dplyr::filter(calibration < (3 * median(calibration)))
cf2 <- cf %>%  
  dplyr::select(id.x,notional_length,PURPOSE,County,PROPERTY_TYPE,PIPETYP) %>% 
  dplyr::group_by(PURPOSE,County,PROPERTY_TYPE,PIPETYP) %>% 
  dplyr::tally() %>% 
  dplyr::ungroup() #%>% 
  #dplyr::select(-n)
cf2$groupid <- seq.int(nrow(cf2))
cf_group <- cf2 %>% left_join(cf, by = c("PURPOSE","County","PROPERTY_TYPE","PIPETYP"))

cf.split <- split(cf_group,cf_group$`groupid`) # use this in the MC simulation



### (1)
factor <- cf$calibration
summary(factor)
hist(factor)
# apply mean and sd
m <- mean(factor)
std <- sqrt(var(factor))
hist(factor, prob = T)
curve(dnorm(x,mean = m, sd = std), add = TRUE)


### (2)
# get ratio of number of pipeids for s24/pdas in each county------------------------>
ratio <- pair %>% 
  dplyr::group_by(County, PIPETYP) %>% 
  dplyr::tally()

ratio2 <- ratio %>% dplyr::ungroup() %>% 
  dplyr::group_by(County) %>% 
  dplyr::mutate(sumtot.n = sum(n)) %>% 
  dplyr::mutate(PIPETYP_RATIO = n/sumtot.n) %>%
  dplyr::ungroup() %>% 
  dplyr::select(-sumtot.n) #%>% 
#pivot_wider(names_from = PIPETYP,values_from = c(tot.length,PIPETYP_RATIO))


# try 3 ---------------------------------------------------------------------------------------------------------------------->
# latin hyper cube

test <- cf.split[[1]]
#result = integer(nrow(test))

# starts here
Calculate_LHC <- function(y) {
  # y is dataframe that has been split
  for (x in 1:length(y)) {
    test <- data.frame(y[[x]])
    #1.
    n = 30000
    total.length2 = integer(n)
    mean.totallength2 = integer(n)
    #2.
    #limits for unif
    lower.lim <- seq(0, 1 - (1 / nrow(test)), 1 / nrow(test))
    upper.lim <- seq(0 + (1 / nrow(test)), 1, 1 / nrow(test))
    
    for (i in 1:n) {
      #3.
      # bring in group data
      group1.mean = mean(test$calibration)
      group1.std = sqrt(var(test$calibration))
      notional_length = test$notional_length
      
      #lhs bit
      #sample from unif
      unif.rv <- runif(nrow(test), lower.lim, upper.lim)
      
      #jumble up to generate orthogonal latin hypercube style samples
      lhs.jumble <- sample(unif.rv, 10, replace = FALSE)
      
      #evalualte unif with nromal cdf to turn into normal samples
      sampled.cf.rv <-
        qnorm(lhs.jumble, group1.mean, group1.std)
      
      #calculate total length
      total.length2[i] = sum(sampled.cf.rv * notional_length)
      #record mean total length for convergence
      mean.totallength2[i] = mean(total.length2[1:i])
    }
    sumresult <- as.data.table(sum(mean.totallength2[i]))
    sumresult$desc <-
      paste0((sumresult - 3 * group1.std),
             " < length < ",
             (sumresult + 3 * group1.std),
             " with 95% confidence"
      )
    sumresult$group <- x
    if (x == 1) {
      sumresult_all <- sumresult
    } else{
      sumresult_all <- rbind(sumresult_all, sumresult)
    }#delete from if and here
  }
  return(sumresult_all)
}
Calculate_LHC(cf.split[2])
lc.result <- Calculate_LHC(cf.split)



# get the cf for each group ----------------------------------->

table <- cf %>%  
  dplyr::select(id.x,surveyed_length, PURPOSE,County,PROPERTY_TYPE,PIPETYP) %>% 
  dplyr::group_by(PURPOSE,County,PROPERTY_TYPE,PIPETYP) %>% 
  dplyr::summarise(sum.surveyed_length = sum(surveyed_length)) %>% 
  dplyr::ungroup()
  
  
table2 <- lc.result %>% left_join(cf2, by = c("group"="groupid")) %>% 
  dplyr::rename(mean.total.length = V1) %>% 
  dplyr::rename(no.of.pipes.in.group = n) 
  
table.full <- cbind(table2,table[,c("sum.surveyed_length")])

# using the surveyed length

table.full %>% ungroup() %>% 
  dplyr::group_by(County) %>% 
  dplyr::summarise(total.sum.surveyed = sum(sum.surveyed_length), 
                   total.sum.meanlength = sum(mean.total.length)) %>% 
  dplyr::mutate(cf = total.sum.surveyed/total.sum.meanlength)


table.full %>% ungroup() %>% 
  dplyr::group_by(PURPOSE) %>% 
  dplyr::summarise(total.sum.surveyed = sum(sum.surveyed_length), 
                   total.sum.meanlength = sum(mean.total.length)) %>% 
  dplyr::mutate(cf = total.sum.surveyed/total.sum.meanlength)


# using the notional length

table2 <- cf %>%  
  dplyr::select(id.x,notional_length, PURPOSE,County,PROPERTY_TYPE,PIPETYP) %>% 
  dplyr::group_by(PURPOSE,County,PROPERTY_TYPE,PIPETYP) %>% 
  dplyr::summarise(sum.notional_length = sum(notional_length)) %>% 
  dplyr::ungroup()


table22 <- lc.result %>% left_join(cf2, by = c("group"="groupid")) %>% 
  dplyr::rename(mean.total.length = V1) %>% 
  dplyr::rename(no.of.pipes.in.group = n) 

table.full2 <- cbind(table22,table2[,c("sum.notional_length")])

table.full2 %>% ungroup() %>% 
  dplyr::group_by(County) %>% 
  dplyr::summarise(total.sum.notional = sum(sum.notional_length), 
                   total.sum.meanlength = sum(mean.total.length)) %>% 
  dplyr::mutate(cf = total.sum.notional/total.sum.meanlength)


table.full2 %>% ungroup() %>% 
  dplyr::group_by(PURPOSE) %>% 
  dplyr::summarise(total.sum.notional = sum(sum.notional_length), 
                   total.sum.meanlength = sum(mean.total.length)) %>% 
  dplyr::mutate(cf = total.sum.notional/total.sum.meanlength)



# distribution of C and F -----------------------------------------------------------------------

cf.split.purpose <- split(cf_group,cf_group$`PURPOSE`) # use this in the MC simulation
cf.purpose.test.F <- cf.split.purpose[[1]]
cf.purpose.test.S <- cf.split.purpose[[2]]

# latin hyper cube
n = 40000
total.length3 = integer(n)
mean.totallength3 = integer(n)


#limits for unif
lower.lim <- seq(0,1-(1/nrow(cf.purpose.test.F)),1/nrow(cf.purpose.test.F))
upper.lim <- seq(0+(1/nrow(cf.purpose.test.F)),1,1/nrow(cf.purpose.test.F))

for(i in 1:n){
  #3.
  # bring in group data
  group1.mean = mean(cf.purpose.test.F$calibration)
  group1.std = sqrt(var(cf.purpose.test.F$calibration))
  notional_length = cf.purpose.test.F$notional_length

  #lhs bit
  #sample from unif
  unif.rv <- runif(nrow(cf.purpose.test.F),lower.lim,upper.lim)

  #jumble up to generate orthogonal latin hypercube style samples
  lhs.jumble <- sample(unif.rv,10,replace = FALSE)

  #evalualte unif with nromal cdf to turn into normal samples
  sampled.cf.rv <- qnorm(lhs.jumble,group1.mean,group1.std)

  #calculate total length
  total.length3[i] = sum(sampled.cf.rv*notional_length)
  #record mean total length for convergance
  mean.totallength3[i] = mean(total.length3[1:i])
}

#display distribution and probability
# m = mean(cf.purpose.test.F$calibration)
# std = sqrt(var(cf.purpose.test.F$calibration))
quantile(total.length3, c(.025, .5, .975))
m = mean(total.length3)
std = sqrt(var(total.length3))
mean(total.length3)
paste0(m - (3 * std), " < Total length < ", m + (3 * std), " with 95% confidence")
hist(total.length3, prob = T, main = "Histogram of Total Length - Foul/Combined Sewers", xlab = "Total Length")
abline(v=mean(total.length3), col=2)
lines(density(total.length3), col="blue", lwd=2)
#curve(dnorm(x,mean = m, sd = std), add = TRUE)

#display convergence
plot(1:n,mean.total.length3, col = "red")


plot(1:n,mean.totallength3,type="l",col="red")
lines(1:n,mean.totallength3,col="green")


# distribution of surface ---------------------------------------------------------

cf.purpose.test.S <- cf.split.purpose[[2]]

# latin hyper cube
n = 40000
total.length4 = integer(n)
mean.totallength4 = integer(n)


#limits for unif
lower.lim <- seq(0,1-(1/nrow(cf.purpose.test.F)),1/nrow(cf.purpose.test.F))
upper.lim <- seq(0+(1/nrow(cf.purpose.test.F)),1,1/nrow(cf.purpose.test.F))

for(i in 1:n){
  #3.
  # bring in group data
  group1.mean = mean(cf.purpose.test.F$calibration)
  group1.std = sqrt(var(cf.purpose.test.F$calibration))
  notional_length = cf.purpose.test.F$notional_length
  
  #lhs bit
  #sample from unif
  unif.rv <- runif(nrow(cf.purpose.test.F),lower.lim,upper.lim)
  
  #jumble up to generate orthogonal latin hypercube style samples
  lhs.jumble <- sample(unif.rv,10,replace = FALSE)
  
  #evalualte unif with nromal cdf to turn into normal samples
  sampled.cf.rv <- qnorm(lhs.jumble,group1.mean,group1.std)
  
  #calculate total length
  total.length4[i] = sum(sampled.cf.rv*notional_length)
  #record mean total length for convergance
  mean.totallength4[i] = mean(total.length4[1:i])
}

#display distribution and probability
# m = mean(cf.purpose.test.F$calibration)
# std = sqrt(var(cf.purpose.test.F$calibration))
quantile(total.length4, c(.025, .5, .975))
m2 = mean(total.length4)
std2= sqrt(var(total.length4))
mean(total.length4)
paste0(m2 - (3 * std2), " < Total length < ", m2 + (3 * std2), " with 95% confidence")
hist(total.length4, prob = T, main = "Histogram of Total Length - Surface Sewers", xlab = "Total Length")
abline(v=mean(total.length4), col=2)
lines(density(total.length4), col="blue", lwd=2)
#curve(dnorm(x,mean = m, sd = std), add = TRUE)

#display convergence
plot(1:n,mean.total.length4, col = "red")



# distribution of PDAS ---------------------------------------------------------

cf.split.PIPETYP <- split(cf_group,cf_group$`PIPETYP`) # use this in the MC simulation
cf.PIPETYP.test.PDAS <- cf.split.PIPETYP[[1]]

# latin hyper cube
n = 40000
total.length5 = integer(n)
mean.totallength5 = integer(n)


#limits for unif
lower.lim <- seq(0,1-(1/nrow(cf.PIPETYP.test.PDAS)),1/nrow(cf.PIPETYP.test.PDAS))
upper.lim <- seq(0+(1/nrow(cf.PIPETYP.test.PDAS)),1,1/nrow(cf.PIPETYP.test.PDAS))

for(i in 1:n){
  #3.
  # bring in group data
  group1.mean = mean(cf.PIPETYP.test.PDAS$calibration)
  group1.std = sqrt(var(cf.PIPETYP.test.PDAS$calibration))
  notional_length = cf.PIPETYP.test.PDAS$notional_length
  
  #lhs bit
  #sample from unif
  unif.rv <- runif(nrow(cf.PIPETYP.test.PDAS),lower.lim,upper.lim)
  
  #jumble up to generate orthogonal latin hypercube style samples
  lhs.jumble <- sample(unif.rv,10,replace = FALSE)
  
  #evalualte unif with nromal cdf to turn into normal samples
  sampled.cf.rv <- qnorm(lhs.jumble,group1.mean,group1.std)
  
  #calculate total length
  total.length5[i] = sum(sampled.cf.rv*notional_length)
  #record mean total length for convergance
  mean.totallength5[i] = mean(total.length5[1:i])
}

#display distribution and probability
# m = mean(cf.PIPETYP.test.PDAS$calibration)
# std = sqrt(var(cf.PIPETYP.test.PDAS$calibration))
quantile(total.length5, c(.025, .5, .975))
m3 = mean(total.length5)
std3= sqrt(var(total.length5))
mean(total.length5)
paste0(m3 - (3 * std3), " < Total length < ", m3 + (3 * std3), " with 95% confidence")
hist(total.length5, prob = T, main = "Histogram of Total Length - Total PDaS Network", xlab = "Total Length")
abline(v=mean(total.length5), col=2)
lines(density(total.length5), col="blue", lwd=2)
#curve(dnorm(x,mean = m, sd = std), add = TRUE)

#display convergence
plot(1:n,mean.total.length5, col = "red")

# done -------------------------------------------------------------------------------------


# # monte carlo sample
# n = 40000
# total.length = integer(n)
# mean.totallength = integer(n)
# 
# for(i in 1:n){ # for each pipeid
#   #2.
#   group1.mean = mean(test$calibration)
#   group1.std = sqrt(var(test$calibration))
#   notional_length = test$notional_length
#   #3.
#   cf = rnorm(nrow(test),group1.mean,group1.std)
#   #4.
#   total.length[i] = sum(cf*notional_length)
#   mean.totallength[i] = mean(total.length[1:i])
# }
# 
# hist(total.length)
# plot(1:n,mean.totallength, col = "blue")
# # 
# # total.length %>% mean()
# # total.length %>% sd()
# # 
# # # -- For group 1, the expected length of pipe is 10400m, the length is within 
# # ## paste0((mean - 3*std)," < length < ", (mean + 3*std))  with 95% confidence
# # 
# # 
# 
# # latin hyper cube
# n = 40000
# total.length2 = integer(n)
# mean.totallength2 = integer(n)
# 
# 
# #limits for unif
# lower.lim <- seq(0,1-(1/nrow(test)),1/nrow(test))
# upper.lim <- seq(0+(1/nrow(test)),1,1/nrow(test))
# 
# for(i in 1:n){
#   #3.
#   # bring in group data
#   group1.mean = mean(test$calibration)
#   group1.std = sqrt(var(test$calibration))
#   notional_length = test$notional_length
# 
#   #lhs bit
#   #sample from unif
#   unif.rv <- runif(nrow(test),lower.lim,upper.lim)
# 
#   #jumble up to generate orthogonal latin hypercube style samples
#   lhs.jumble <- sample(unif.rv,10,replace = FALSE)
# 
#   #evalualte unif with nromal cdf to turn into normal samples
#   sampled.cf.rv <- qnorm(lhs.jumble,group1.mean,group1.std)
# 
#   #calculate total length
#   total.length2[i] = sum(sampled.cf.rv*notional_length)
#   #record mean total length for convergance
#   mean.totallength2[i] = mean(total.length2[1:i])
# }
# 
# #display distribution
# hist(total.length2)
# 
# #display convergence
# plot(1:n,mean.totallength2, col = "red")
# 
# 
# plot(1:n,mean.totallength,type="l",col="red")
# lines(1:n,mean.totallength2,col="green")
# 
# # 
# 
# 
# # try 2 ------------------------------------>
# # cftest <- cf.split[[1]]
# # result = integer(nrow(cftest))
# # Calculate_Monte_Carlo <- function(i) {
# #   for(i in 1:nrow(cftest)){ # for each pipeid
# #     #2.
# #     group1.mean = mean(cftest$calibration)
# #     group1.std = sqrt(var(cftest$calibration))
# #     notional_length = cftest$notional_length
# #     #3.
# #     cf = rnorm(1,group1.mean,group1.std)
# #     #4.
# #     result[i] = cf*notional_length
# #   }
# #   {
# #     sumresult <- as.data.table(sum(result[i]))
# #     sumresult$group <- 1
# #     print(sumresult)
# #   }
# # }
# 
# Calculate_Monte_Carlo(cf.split[[2]])
# 
# sumresult$desc <- paste0((group1.mean - 3*group1.std)," < length < ", (group1.mean + 3*group1.std),  " with 95% confidence")
