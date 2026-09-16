# NOAA_SLR_downloader_v4.R
#
# Download files from the NOAA "Sea Level Rise Viewer Data Download" page:
# https://coast.noaa.gov/slrdata/Ancillary/index.html and
# https://coast.noaa.gov/slrdata/Ancillary/NOAA_OCM_SLR_MergedPolys_Shapefiles_0225/index.html
#
# Downloading is driven by a YAML configuration file: config_filename, below.
#
# ---------------------------------------------------------------------------
# v4 changes, prompted by a silently truncated download
#
# LA_merged_slr_1_0ft.shp was downloaded 26% short (1,086,590,712 bytes against
# a declared 1,472,614,452) and the failure was invisible: the HTTP status was
# 200, the file was written, and every subsequent run skipped it because it
# existed on disk. The incomplete layer propagated through ingest into the
# tract intersections and structure flags, and presented as a systematic
# Louisiana-specific SLR anomaly at 1 ft.
#
#   1. Content-Length is compared against the bytes actually written, and a
#      short file is deleted rather than left on disk.
#   2. .shp files are additionally checked against the file length declared in
#      their own 100-byte header, which is independent of the HTTP layer.
#   3. Failed downloads are retried, with the partial file removed first.
#   4. A final integrity sweep re-checks every .shp in the download directory
#      and prints a summary, so a bad file cannot pass unnoticed.
#   5. The skip-if-exists rule now verifies the existing file before skipping.
#   6. Errors and non-200 responses delete the partial file, so the skip rule
#      can never preserve damage.
#   7. beep() is wrapped in try() so a headless run does not fail on audio.
#   8. str_replace uses fixed() -- "ft." was being treated as a regex where the
#      "." matched any character.
# ---------------------------------------------------------------------------

library(yaml)
library(httr)
library(fs)
library(stringr)
library(beepr)

# the filename of the YAML configuration file
config_filename <- "~/Science/Nora_SLR/NOAA_downloads_config_v2_LA_1ft_only.yaml"

if (!file.exists(config_filename)) {
  cat("can't find the configuration file!\n")
  cat(config_filename, "\n")
  stop("Quitting!")
}

# --- integrity helpers ------------------------------------------------------

# A shapefile's .shp header stores the total file length at bytes 25-28,
# big-endian, in 16-bit words. Comparing that against the size on disk detects
# truncation independently of anything the HTTP layer reports.
shp_declared_length <- function(path) {
  if (!file.exists(path) || file.info(path)$size < 100) return(NA_real_)
  con <- file(path, "rb"); on.exit(close(con))
  hdr <- readBin(con, "raw", 100)
  sum(as.numeric(hdr[25:28]) * c(256^3, 256^2, 256, 1)) * 2
}

check_shp <- function(path, quiet = FALSE) {
  if (!grepl("\\.shp$", path, ignore.case = TRUE)) return(TRUE)
  declared <- shp_declared_length(path)
  actual   <- file.info(path)$size
  if (is.na(declared)) {
    if (!quiet) warning("cannot read .shp header: ", basename(path))
    return(FALSE)
  }
  ok <- isTRUE(declared == actual)
  if (!ok && !quiet)
    warning("SHP LENGTH MISMATCH: ", basename(path),
            " -- header declares ", format(declared, big.mark = ","),
            " bytes, file is ", format(actual, big.mark = ","),
            " (short by ", format(declared - actual, big.mark = ","), ")")
  ok
}

# --- download with verification ---------------------------------------------

download_file <- function(url, dest, max_attempts = 3) {

  for (attempt in seq_len(max_attempts)) {

    ok <- tryCatch({
      res <- GET(url, write_disk(dest, overwrite = TRUE), timeout(7200))

      if (res$status_code != 200) {
        warning("Failed with status ", res$status_code, ": ", url)
        if (file.exists(dest)) file.remove(dest)
        FALSE

      } else {
        actual   <- file.info(dest)$size
        expected <- suppressWarnings(as.numeric(res$headers$`content-length`))

        if (!is.na(expected) && expected != actual) {
          warning("TRUNCATED (attempt ", attempt, "): ", basename(dest),
                  " -- expected ", format(expected, big.mark = ","),
                  " bytes, wrote ", format(actual, big.mark = ","))
          file.remove(dest)
          FALSE

        } else if (!check_shp(dest)) {
          # header check failed; already warned inside check_shp
          file.remove(dest)
          FALSE

        } else {
          message("Downloaded to: ", dest, "  (",
                  format(actual, big.mark = ","), " bytes",
                  if (is.na(expected)) ", no Content-Length header" else "", ")")
          TRUE
        }
      }
    }, error = function(e) {
      warning("Download error (attempt ", attempt, ") for ", url, ":\n",
              conditionMessage(e))
      if (file.exists(dest)) file.remove(dest)
      FALSE
    })

    if (isTRUE(ok)) return(invisible(TRUE))
    if (attempt < max_attempts) {
      message("  retrying in 5s (attempt ", attempt + 1, " of ", max_attempts, ") ...")
      Sys.sleep(5)
    }
  }

  warning("GIVING UP after ", max_attempts, " attempts: ", url)
  invisible(FALSE)
}

# --- configuration ----------------------------------------------------------

config_yaml <- yaml::yaml.load_file(config_filename)
cat("read the YAML configuration file\n")

NOAA_base_url      <- config_yaml$NOAA_base_url
extension_names    <- config_yaml$extension_names
location_names     <- config_yaml$location_names
vector_type        <- config_yaml$vector_type
slr_height         <- config_yaml$slr_height
download_directory <- config_yaml$download_directory

if (!(vector_type == "slr" | vector_type == "low")) {
  message("Vector type is not valid, expecting 'slr' or 'low' but found ", vector_type)
  stop("Quitting!")
}

if (!(slr_height %% 1 == 0 && slr_height >= 0 && slr_height <= 10)) {
  message("SLR height is not valid, expecting an integer between 0 and 10 but found ", slr_height)
  stop("Quitting!")
}

if (dir.exists(download_directory) == FALSE) {
  dir_create(download_directory)
}

cat("downloading to: ", path_expand(download_directory), "\n")

# --- main download loop -----------------------------------------------------

results <- list()

if (vector_type == "slr") {
  for (l_name in location_names) {
    for (extension in extension_names) {

      # TX and LA regions have a source naming convention like this:
      # TX_merged_slr_3ft.shp               NOTE: slr_3ft, or more generally:
      # <location_name>_merged_<polygon_class>_<sea_level_height>ft.<extension>
      if (l_name == "TX" || l_name == "LA") {
        url_part <- paste0(l_name, "_merged_", vector_type, "_", slr_height, "ft.", extension)
        # add the missing _0 to the dest_path filename to make it consistent
        # with the naming convention for the other regions.
        # fixed() because "." in "ft." would otherwise be a regex wildcard.
        url_dest_part <- str_replace(url_part, fixed("ft."), "_0ft.")
        dest_path <- path(download_directory, url_dest_part)
      } else {
        # All other regions have a naming convention like this:
        # Atlantic_merged_slr_3_0ft.shp     NOTE: slr_3_0ft, or more generally:
        # <location_name>_merged_<polygon_class>_<sea_level_height>_0ft.<extension>
        url_part <- paste0(l_name, "_merged_", vector_type, "_", slr_height, "_0ft.", extension)
        dest_path <- path(download_directory, url_part)
      }
      url_full <- paste0(NOAA_base_url, url_part)
      cat(url_full, "\n")

      # Skip only if the existing file passes its integrity check. A file that
      # exists but is short is removed and re-downloaded -- the v3 skip rule
      # preserved exactly the kind of damage this script now looks for.
      if (file.exists(dest_path)) {
        if (check_shp(dest_path, quiet = TRUE)) {
          cat("   ", url_part, "already exists and verifies, skipping\n")
          results[[as.character(dest_path)]] <- "skipped"
          next
        } else {
          cat("   ", url_part, "exists but FAILS verification, re-downloading\n")
          file.remove(dest_path)
        }
      }

      ok <- download_file(url_full, dest_path)
      results[[as.character(dest_path)]] <- if (isTRUE(ok)) "downloaded" else "FAILED"
    }
  }
} else {
  cat("Downloader for 'low' file types not implemented yet...\n")
}

# --- final integrity sweep --------------------------------------------------
# Re-checks every .shp in the download directory, not only those touched by
# this run, so pre-existing damage from earlier versions surfaces too.

cat("\n=== integrity sweep: all .shp files in the download directory ===\n")

shps <- dir_ls(download_directory, glob = "*.shp", fail = FALSE)
if (length(shps) == 0) {
  cat("(no .shp files found)\n")
} else {
  sweep <- data.frame(
    file     = path_file(shps),
    declared = vapply(shps, shp_declared_length, numeric(1)),
    actual   = file.info(shps)$size,
    row.names = NULL, stringsAsFactors = FALSE)
  sweep$diff <- sweep$actual - sweep$declared
  sweep$status <- ifelse(is.na(sweep$declared), "UNREADABLE",
                  ifelse(sweep$diff == 0, "OK", "TRUNCATED"))
  print(sweep[order(sweep$status, sweep$file), ], row.names = FALSE)

  bad <- sum(sweep$status != "OK")
  if (bad > 0) {
    cat("\n*** ", bad, " file(s) failed verification. Delete and re-download ",
        "before ingesting. ***\n", sep = "")
  } else {
    cat("\nAll ", nrow(sweep), " .shp files verify against their headers.\n", sep = "")
  }
  write.csv(sweep, path(download_directory, "shp_integrity_check.csv"),
            row.names = FALSE)
}

# --- download summary -------------------------------------------------------

if (length(results) > 0) {
  cat("\n=== this run ===\n")
  print(table(unlist(results)))
  failed <- names(results)[unlist(results) == "FAILED"]
  if (length(failed) > 0) {
    cat("\nFAILED:\n"); cat(paste0("  ", failed, collapse = "\n"), "\n")
  }
}

try(beep(3), silent = TRUE)
cat("Done!\n")
