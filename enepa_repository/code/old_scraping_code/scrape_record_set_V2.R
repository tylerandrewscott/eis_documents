library(rvest)
library(stringr)
library(tidyverse)
library(data.table)
require(rvest)


#submissionsTable~ .pagelinks a:nth-child(3) , #export a:nth-child(4)

#### they changed the site so it gets wonky if it you just click search with no parameters
### so easiest to use dates. right now, since it's pretty updated, easiest thing is just to plug in the year
### would have to be updated to iterate through more years

fname = 'enepa_repository/metadata/eis_record_overview_V2.rds'
recheck = F
if(file.exists(fname)){ record_df = readRDS(fname)}else{record_df = data.table(stringsAsFactors = F)}
# 

# if(file.exists('input/epa_master_repository/eis_record_overview.csv')){
# record_df = fread('input/epa_master_repository/eis_record_overview.csv',stringsAsFactors = F)
# record_df = record_df %>% mutate_if(is.logical,as.character) %>% as.data.table()}
for(year in 2024:1987){
  print(year)
  #base_page <- 'https://cdxnodengn.epa.gov/cdx-enepa-public/action/eis/search'
  base_page <- 'https://cdxapps.epa.gov/cdx-enepa-II/public/action/eis/search'
  base_session = base_page %>% session()
  search_form = html_form(base_session)[[2]]
  search_form$fields$searchCriteria.onlyCommentLetters$value <- 'false'
  search_form$fields$searchCriteria.startFRDate$value <- paste0('01/01/',year)
  search_form$fields$searchCriteria.endFRDate$value <- paste0('12/31/',year)
  search_session = session_submit(base_session,search_form,submit = 'searchRecords')
  
  all_meta_tables <- list()
  
  repeat {
    # Extract the table from the current page
    current_table <- search_session |> read_html() |> html_nodes('table') |> html_table()
    current_table <- current_table[[1]]
    
    # Convert column names to replace spaces with dots
    colnames(current_table) <- gsub('\\s','.',colnames(current_table))
    
    # Extract hrefs for EIS overviews
    nodes <- search_session |> read_html() |> html_nodes('a')
    hrefs <- nodes |> html_attr('href')
    current_table$href <- hrefs[grepl('details\\?eisId', hrefs)]
    
    # Check if every row in the current table is already in the record_df
    if (all(paste(current_table$Title, current_table$Federal.Register.Date) %in% paste(record_df$Title, record_df$Federal.Register.Date))) {
      print(paste("All records for year", year, "are already in the dataset. Stopping for this year."))
      break
    }
    
    # Append the current table to the list of all tables
    all_meta_tables <- append(all_meta_tables, list(current_table))
    
    # Check if there is a "Next" link
    a_nodes <- search_session %>% read_html() %>% html_nodes('a') 
    a_titles <- a_nodes %>% html_text(trim = T)
    if (!'Next' %in% a_titles) {
      break
    }
    
    # Follow the "Next" link
    next_i <- min(which(a_titles == 'Next'))
    search_session <- session_follow_link(search_session, i = next_i)
    Sys.sleep(1.5)
  }
  
  # Combine all tables into one large table
  meta_table <- rbindlist(all_meta_tables, fill = TRUE)
  
  # Read the existing record_df to ensure the original data is preserved
  if(file.exists(fname)){
    record_df <- readRDS(fname)
  } else {
    record_df <- data.table(stringsAsFactors = F)
  }
  
  # Remove duplicates before appending
  meta_table <- meta_table[!paste(Title, Federal.Register.Date) %in% paste(record_df$Title, record_df$Federal.Register.Date),]
  
  # Append the new meta_table for the current year to the existing record_df
  record_df <- rbindlist(list(record_df, meta_table), fill = TRUE)
  
  # Save the updated record_df
  saveRDS(record_df, fname)
  print(paste("Data for year", year, "saved to", fname))
  
  # Pause to ensure the file is saved before proceeding to the next year
  Sys.sleep(1)
}

