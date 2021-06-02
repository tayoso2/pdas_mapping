# Introduction 
Script reads LiDAR data from raster images (ASC) and assigns one to each record in a pipe data set. The LiDAR data is used to determine pipe depth at the start and end point in order to find flow directio assuming the pipe is gravity fed.

# Getting Started
Project code is stored on GitLab at [PDaS1](https://gitlab.com/pdas1/pipe-depth-from-lidar-images). The repo contains an RStudio project with scripts, sample data and other components, but a complete pipe asset data set is required once a local clone has been made. Current script package requirements are:
- sf
- raster
- lwgeom
- tmap
- pbapply
- dplyr

# Build and Test
The master branch will contain the current state of the script and can be tested with the project open in RStudio using the 'Source' button at the top-right of the 'Source' panel.

# Contribute
To contribute to this project contact [James Brown](James.Brown2@arcadisgen.com)