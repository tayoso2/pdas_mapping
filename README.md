# PDaS and S24 Spatial Mapping Project

## Project Description

The PDaS and S24 Spatial Mapping project was designed to utilise advanced spatial analytics in  improving STW’s understanding of the location of S24 shared drains and PDaS assets and their key  attributes, i.e., length, material, diameter and yearlaid. Arcadis Gen was tasked with generating a set of  inferred PDaS and S24 assets and their neighbouring proxy sewer mains.

- **Phase 1:** This involved generating property polygons and notional connections from the centroid of the property to the nearest sewer mains. This also involved building custom machine learning models used to assign attributes to notional assets as well as applying Monte Carlo and Latin hyper cube simulations to recalibrate notional asset coefficients. The scripts and repeatable processes were written in R and QGIS.
- **Phase 2:** This involved spatial refinement of the notional length from Phase 1. Using rules and assumptions provided by subject matter experts, the notional connections were upgraded to actual pipe polygons to take into account a wider variety of externalities and engineering constraints. 



## Resources

**Final Report**

https://arcadiso365.sharepoint.com/teams/PDaSAssetBaseCreation/Shared%20Documents/General/Presentations/Phase%202/Final%20Report/PDaS%20and%20S24%20Spatial%20Mapping%20Final%20Report%20v2.pdf

**Phase 2 Decision Log**

https://arcadiso365.sharepoint.com/teams/PDaSAssetBaseCreation/_layouts/15/Doc.aspx?OR=teams&action=edit&sourcedoc={AEFC1F8B-5C09-4B58-A119-C08EA4BBEAE1}





## Role

- Tayo Ososanya - Technical Lead
- Will Bailey - Spatial Analyst
- Shuyao Chen - Spatial Analyst
- Nik Humphries - Spatial Analyst
- Simone Croft - Project Manager
- Carl Takamizawa - Project Manager

 

## Contacts 

- Will Bailey - Analytics Consultant
- Shuyao Chen - Analytics Consultant
- Carl Takamizawa - Associate Director of Analytics



**File-location**

It is located on the SSD 

> D:\STW PDAS\



## Scripts

The scripts used are on www.github.com/tayoso2/pdas_mapping. 

**Apologies for the poorly written scripts as there was no time to tidy them up after project closure.**

### Proxy mains

**Attribution**

1. ./proxy sewer analysis/Will_proxy_attribute_assigning_progress/offset proxies_wb_edit.r

2. ./proxy sewer analysis/assign proxy sewer atts.r

*Assign proxy sewer atts was created using the prediction models from ML proxy sewer atts.*

**Flow Direction** 

1. Data from Attribution
2. ./pipe-depth-from-lidar-images/scripts/pipe_lidar.r
3. ./pipe-depth-from-lidar-images/scripts/pipe_direction.r

### **PDaS line mapping & attribution**

**Drawing, splitting, attributing, flagging lines**

1. Received lines from Shuyao 
2. ./segment_pdas_s24_assetbases_VM/Segment_PDAS_S24/Segment_PDaS_Final/Dataflow Folder/2-R Script Chunker/3 - Splitter.r
3. ./segment_pdas_s24_assetbases_VM/Segment_PDAS_S24/Segment_S24_Final/Dataflow Folder/2-R Script Chunker/3 - Splitter.r
4. ./assign-attributes/predict d_m for inferred pdas.r
5. ./assign-attributes/predict d_m for inferred s24_no_fid.r
6. ./flag_overlap/flag_overlap.r
7. ./assign-attributes/result_statistics.r

