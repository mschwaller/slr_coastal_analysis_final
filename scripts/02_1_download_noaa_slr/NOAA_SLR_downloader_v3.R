# Download files from the NOAA "Sea Level Rise Viewer Data Download" page:
# https://coast.noaa.gov/slrdata/Ancillary/index.html and 
# https://coast.noaa.gov/slrdata/Ancillary/NOAA_OCM_SLR_MergedPolys_Shapefiles_0225/index.html
# The dowwnloading is run from a YAML configuration file: config_filename, see just below the library() calls

library(yaml)
library(httr)
library(fs) # For path-safe file handling
library(stringr)
# not sure I actually use these:
library(dplyr)
library(magrittr)
library(beepr)

# the filename of the YAML configuration file
config_filename <- "~/!Essentials/Nora_SLR/NOAA_downloads_config_v1.yaml"
# quit if the config file can't be found
if (!file.exists(config_filename)) {
  cat("can't find the configuration file!\n")
  cat(config_filename, "\n")
  stop("Quitting!")
}

# a function to trap errors when GET-ing the NOAA files based on the urls that are built below
download_file <- function(url, dest) {
  tryCatch({
    res <- GET(url, write_disk(dest, overwrite = TRUE))
    if (res$status_code == 200) {
      message("Downloaded to: ", dest)
      # the else block runs if there's a HTTP error
    } else {
      warning("Failed with status ", res$status_code, ": ", url)
    }
    # this block runs if there's an R-level failure (like GET can't parse the string)
  }, error = function(e) {
    warning("Download error for ", url, ":\n", conditionMessage(e))
  })
}

# Load configuration parameters from the YAML file
config_yaml <- yaml::yaml.load_file(config_filename)
cat("read the YAML configuration file\n")
NOAA_base_url      <- config_yaml$NOAA_base_url
# extension names, e.g. cpg, dbf, prj, sbn, sbx, shp, xml, shx
extension_names    <- config_yaml$extension_names
# location names, e.g., TX, MS_AL, Atlantic, ...
location_names     <- config_yaml$location_names
# vector type may be slr or low
vector_type        <- config_yaml$vector_type
# slr height is an integer number of feet of SLR rise from 0 to 10
slr_height         <- config_yaml$slr_height
download_directory <- config_yaml$download_directory

# verify that the vector type is valid
if (!(vector_type == "slr" | vector_type == "low")) {
  message("Vector type is not valid, expecting 'slr' or 'low' but found ", vector_type)
  message("Quitting!")
  stop()
}

# verify that SLR height is an integer between 0 and 10
if (!(slr_height %% 1 == 0 && slr_height >= 0 && slr_height <= 10)) {
  message("SLR height is not valid, expecting an integer between 0 and 10 but found ", slr_height)
  message("Quitting!")
  stop()
}

# verify that the download directory exists, and if not then create it
if (dir.exists(download_directory) == FALSE) {
  dir_create(download_directory)
}

# the slr and low files have slightly different naming conventions, so treat each one separately
if (vector_type == "slr") {
  for (l_name in location_names) {
    for (extension in extension_names) {
      
      # TX an LA regions have a source naming convention like this:
      # TX_merged_slr_3ft.shp               NOTE: slr_3ft, or more generally:
      # <location_name>_merged_<polygon_class>_<sea_level_height>ft.<extension>
      if (l_name == "TX" || l_name == "LA") {
        url_part <- paste0(l_name, "_merged_", vector_type, "_", slr_height, "ft.", extension)
        # add the missing _0 to the dest_path filename to make it consistent with the naming convention for the other regions
        url_dest_part <- str_replace(url_part, "ft.", "_0ft.")
        dest_path <- path(download_directory, url_dest_part)
      } else {
        # But all other regions have a naming convention like this (why?):
        # Atlantic_merged_slr_3_0ft.shp     NOTE: slr_3_0ft, or more generally:
        # <location_name>_merged_<polygon_class>_<sea_level_height>_0ft.<extension>
        url_part <- paste0(l_name, "_merged_", vector_type, "_", slr_height, "_0ft.", extension)
        dest_path <- path(download_directory, url_part)
      }
      url_full <- paste0(NOAA_base_url, url_part)
      cat(url_full, "\n")
      
      
      # if the file already exists, then skip to the next one
      if (file.exists(dest_path)) {
        cat("   ", url_part, "already exists on disk, skipping\n")
        next 
      }
      
      download_file(url_full, dest_path)
      
      
      
    }
  }
} else {
  cat("Downloader for 'low' file types not implemented yet...\n")
}

beep(3)
cat("Done!\n")
