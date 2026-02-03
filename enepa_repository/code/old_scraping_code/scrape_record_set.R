library(rvest)
library(stringr)
library(tidyverse)
library(data.table)
require(rvest)


#submissionsTable~ .pagelinks a:nth-child(3) , #export a:nth-child(4)

#### they changed the site so it gets wonky if it you just click search with no parameters
### so easiest to use dates. right now, since it's pretty updated, easiest thing is just to plug in the year
### would have to be updated to iterate through more years

fname = 'enepa_repository/metadata/eis_record_detail_V2.rds'
recheck = F
if(file.exists(fname)){ record_df = readRDS(fname)}else{record_df = data.table(stringsAsFactors = F)}
# 

# if(file.exists('input/epa_master_repository/eis_record_detail.csv')){
# record_df = fread('input/epa_master_repository/eis_record_detail.csv',stringsAsFactors = F)
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
    
    # Extract hrefs for EIS details
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





  head(meta_table)
  
  last_page = max(as.numeric(gsub('p=','',str_extract(hrefs[duplicated(hrefs)],'p=[0-9]{1,}'))),na.rm = T)
  hrefs = grep('searchRecords',hrefs,value = T)[1]
  
  
  hrefs
  #hrefs = hrefs[!duplicated(hrefs)]
  #next_link = which(search_session %>% read_html() %>% html_nodes('a') %>% html_text() == 'Next')[1]
  #search_session = session_follow_link(search_session,i = next_link)
  p = 1
  keep_going = T
  page_set <- 1:last_page
  #search_session = rvest::session_jump_to(search_session,'?searchCritera.primaryStates=&d-446779-p=137&reset=Reset&searchCriteria.onlyCommentLetters=false')
  #while(p < last_page & keep_going){
  while(keep_going & p %in% page_set){
      print(p)
      jump = str_replace(hrefs,'p=2',paste0('p=',p))
      next_link = paste0(base_page,jump)
      search_session = rvest::session_jump_to(search_session,next_link)
    #p = as.numeric(str_extract(str_extract(search_session$url,'\\bp=[0-9]{1,}\\b'),'[0-9]{1,}'))
      new_record = {search_session %>% read_html() %>% html_table(trim=T)}[[1]]
      eis_urls = grep('details\\?eisId=[0-9]{1,}$',search_session%>% read_html() %>% html_nodes('a') %>% html_attr('href'),value=T)
      eis_base = 'https://cdxapps.epa.gov/'
      new_record <- data.table(new_record)
      new_record$eis_url <- paste0(eis_base,eis_urls)
      colnames(new_record) <- gsub('\\s','.',colnames(new_record))
      new_record[,Download.Documents:=NULL]
      new_record$Title <- enc2utf8(new_record$Title)
      new_record$Title <- iconv(new_record$Title, "UTF-8", "UTF-8",sub='')
      new_record$Title <- str_remove_all(new_record$Title,'\\\"')
      new_record = new_record[!paste(Title,Federal.Register.Date) %in% paste(record_df$Title,record_df$Federal.Register.Date),]
    if(nrow(new_record)==0&!recheck){keep_going <<- FALSE}
    else if(nrow(new_record)==0&recheck){p = p + 1;Sys.sleep(0.25);next}
    else{
        print('looping through individual EIS pages')
      #if(nrow(new_record)==0){break}
        eis_info = lapply(seq_along(new_record$eis_url),function(x) {
        page = read_html(new_record$eis_url[x])
        i_name =  page %>% html_nodes(css = 'h4') %>% html_text(trim=T)
        i_value = page %>% html_nodes(css = '.form-item') %>% html_text(trim=T)
        entry = as.data.table(rbind(mapply(function(x,y) gsub(x,'',y) , x = i_name,y = i_value,SIMPLIFY=F)))
        entry$eis_url <- new_record$eis_url[x]
        Sys.sleep(1.5)
        entry = data.frame(do.call(cbind,sapply(entry,function(x) gsub('^ ','',unlist(x)),simplify = F)),stringsAsFactors = F)
        entry})
    eis_info_df = rbindlist(eis_info,fill = T)
    #eis_info_df$EIS.Title <- str_remove_all(eis_info_df$EIS.Title,'[^[:alnum:] ]')
    
    eis_info_df$EIS.Title <- enc2utf8(eis_info_df$EIS.Title)
    eis_info_df$EIS.Title <- iconv(eis_info_df$EIS.Title, "UTF-8", "UTF-8",sub='')
    eis_info_df$EIS.Title <- str_remove_all(eis_info_df$EIS.Title,'\\\"')
    
    eis_info_df$State.or.Territory <- stringr::str_replace_all(eis_info_df$State.or.Territory, "[\r\t\n]|\\s", "")
    eis_info_df$EIS.Comment.Due..Review.Period.Date <- stringr::str_replace_all(eis_info_df$EIS.Comment.Due..Review.Period.Date, "[\r\t\n]|\\s", "")
    eis_info_df$Federal.Register.Date <- stringr::str_replace_all(eis_info_df$Federal.Register.Date, "[\r\t\n]|\\s", "")
    eis_info_df$Rating..if.Draft.EIS. <- stringr::str_replace_all(eis_info_df$Rating..if.Draft.EIS.,"[\r\t\n]|\\s", "")
    eis_info_df$Rating..if.Draft.EIS. <- stringr::str_replace_all(eis_info_df$Rating..if.Draft.EIS.,"Rating\\(ifDraftEIS\\)", "")
    eis_info_df$EPA.Comment.Letter.Date <- stringr::str_replace_all(eis_info_df$EPA.Comment.Letter.Date,"[\r\t\n]|\\s", "")
    eis_info_df$Amended.Notice.Date <- stringr::str_replace_all(eis_info_df$Amended.Notice.Date,"[\r\t\n]|\\s", "")
    eis_info_df[,Federal.Register.Date:=NULL]
    eis_info_df[,EPA.Comment.Letter.Date:=NULL]
    
    new_record$EIS_ID <- as.numeric(str_extract(new_record$eis_url,'(?!eisId=)[0-9]{1,}'))
    
    new_record <- merge(new_record,eis_info_df,by = 'eis_url',all = T) 
    new_record$EIS.Number = as.numeric(new_record$EIS.Number)
    new_record[new_record==''] <- NA
    new_record = new_record %>% mutate_if(is.logical,as.character)
    new_record = data.table(new_record)
    record_df <<- rbindlist(list(record_df,new_record),fill = T)
    p = p + 1
    Sys.sleep(0.25)
    }
  #if(any(duplicated(record_df))){
  #    record_df <- record_df[!duplicated(record_df),]
  #    break}
  #if(current_page!=last_page)
  #current_page = current_page + 1
  }
}


table(str_extract(record_df$EIS.Number,'^[0-9]{4}'))
saveRDS(object = record_df,file = fname)
