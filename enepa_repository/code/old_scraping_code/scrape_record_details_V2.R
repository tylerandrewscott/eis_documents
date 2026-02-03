# Load necessary libraries
library(rvest)
library(data.table)
library(stringr)

# Define the function to scrape metadata for each project record
scrape_project_metadata <- function(eis_urls, clobber = TRUE) {
  # Initialize an empty list to store metadata for each project
  project_metadata_list <- list()
  # Initialize an empty data table for document details
  document_details_dt <- data.table()
  
  # Check if the RDS files exist and read them if clobber is FALSE
  if (!clobber) {
    if (file.exists('enepa_repository/metadata/eis_record_detail_V2.rds')) {
      existing_metadata <- readRDS('enepa_repository/metadata/eis_record_detail_V2.rds')
      existing_urls <- existing_metadata$eis_url
      # Filter out URLs that are already in the existing metadata
      eis_urls <- setdiff(eis_urls, existing_urls)
    }
    # Don't read existing document details here - we'll handle that in the main loop
  }
  
  # Loop through each EIS URL to extract metadata
  for (url in eis_urls) {
    tryCatch({
      # Read the HTML content of the page
      page <- read_html(url)
      # Extract metadata fields and values
      metadata_names <- page %>% html_nodes(css = 'h4') %>% html_text(trim = TRUE)
      metadata_values <- page %>% html_nodes(css = '.form-item') %>% html_text(trim = TRUE)
      
      # Extract EIS documents (names and IDs)
      eis_links <- page %>% 
        html_nodes(xpath = "//strong[text()='EIS Document(s):']/following::a[contains(@href, 'javascript:void(0)') and (not(//strong[text()='Comment Letter(s):']) or following::strong[text()='Comment Letter(s):'])]")
      
      eis_document_names <- eis_links %>% html_text(trim = TRUE)
      eis_document_ids <- eis_links %>% 
        html_attr("onclick") %>% 
        str_extract("(?<=downloadAttachment', ')\\d+")
      
      # Extract Comment Letter documents (names and IDs)
      comment_links <- page %>% 
        html_nodes(xpath = "//strong[text()='Comment Letter(s):']/following::p/a[contains(@href, 'javascript:void(0)')]")
      
      comment_letter_names <- comment_links %>% html_text(trim = TRUE)
      comment_letter_ids <- comment_links %>% 
        html_attr("onclick") %>% 
        str_extract("(?<=downloadAttachment', ')\\d+")
      
      # Create a data table for documents with columns for eisId, document name, and type
      document_details <- rbind(
        data.table(eisId = gsub(".*eisId=", "", url), document_name = eis_document_names, document_id = eis_document_ids, type = "EIS Document"),
        data.table(eisId = gsub(".*eisId=", "", url), document_name = comment_letter_names, document_id = comment_letter_ids, type = "Comment Letter")
      )
      
      # Append the document details to the data table directly
      document_details_dt <- rbindlist(list(document_details_dt, document_details), fill = TRUE)
      
      # Create a data table for the current project
      project_metadata <- as.data.table(rbind(mapply(function(name, value) gsub(name, '', value), 
                                                     name = metadata_names, 
                                                     value = metadata_values, 
                                                     SIMPLIFY = FALSE)))
      
      # Add the EIS URL to the metadata
      project_metadata$eis_url <- url
      
      # Append the current project's metadata to the list
      project_metadata_list <- append(project_metadata_list, list(project_metadata))
      
      # Pause for 0.5 seconds to avoid overloading the system
      Sys.sleep(0.5)
    }, error = function(e) {
      message(paste("Error processing URL:", url, "Error message:", e$message))
    })
  }
  
  # Combine all project metadata into a single data table
  all_project_metadata <- rbindlist(project_metadata_list, fill = TRUE)
  
  return(list(project_metadata = all_project_metadata, document_details = document_details_dt))
}

base_record = readRDS('enepa_repository/metadata/eis_record_overview_V2.rds')

# Example usage
# Assuming eis_urls is a vector of URLs obtained from the previous scraping process
eis_hrefs <- base_record$href
eis_urls <- paste0('https://cdxapps.epa.gov/',eis_hrefs)

# Define the batch size for processing URLs
batch_size <- 500

# Check if the existing file for project metadata exists
if (file.exists('enepa_repository/metadata/eis_record_detail_V2.rds')) {
  existing_metadata <- readRDS('enepa_repository/metadata/eis_record_detail_V2.rds')
  existing_urls <- existing_metadata$eis_url
} else {
  existing_urls <- character(0)
  existing_metadata <- data.table()
}

# Filter out URLs that have already been processed
urls_to_scrape <- setdiff(eis_urls, existing_urls)

if (file.exists('enepa_repository/metadata/eis_document_record_V2.rds')) {
  all_document_details <- readRDS('enepa_repository/metadata/eis_document_record_V2.rds')
} else {
  all_document_details <- data.table()
}

all_project_metadata <- existing_metadata

# Process URLs in batches
for (i in seq(1, length(urls_to_scrape), by = batch_size)) {
  # Determine the current batch of URLs
  current_batch <- urls_to_scrape[i:min(i + batch_size - 1, length(urls_to_scrape))]
  
  # Scrape metadata for the current batch
  batch_metadata <- scrape_project_metadata(current_batch, clobber = FALSE)
  
  # Append the new metadata to the existing metadata
  all_project_metadata <- rbindlist(list(all_project_metadata, batch_metadata$project_metadata), fill = TRUE, use.names = TRUE)
  all_document_details <- rbindlist(list(all_document_details, batch_metadata$document_details), fill = TRUE, use.names = TRUE)
  
  # Save progress after each batch
  saveRDS(all_project_metadata, 'enepa_repository/metadata/eis_record_detail_V2.rds')
  saveRDS(all_document_details, 'enepa_repository/metadata/eis_document_record_V2.rds')
  
  # Print progress
  print(paste("Processed batch", i, "to", min(i + batch_size - 1, length(urls_to_scrape)), "of", length(urls_to_scrape), "URLs"))
  
  # Force garbage collection to free up memory
  gc()
}
