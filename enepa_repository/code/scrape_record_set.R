library(rvest)
library(stringr)
library(tidyverse)
library(data.table)
require(rvest)

fname = 'enepa_repository/metadata/eis_record_detail.rds'
recheck = T
if(file.exists(fname)){
  record_df = readRDS(fname)}else{record_df = data.table(stringsAsFactors = F)}
# 

# if(file.exists('input/epa_master_repository/eis_record_detail.csv')){
# record_df = fread('input/epa_master_repository/eis_record_detail.csv',stringsAsFactors = F)
# record_df = record_df %>% mutate_if(is.logical,as.character) %>% as.data.table()}

#base_page <- 'https://cdxnodengn.epa.gov/cdx-enepa-public/action/eis/search'
base_page <- 'https://cdxapps.epa.gov/cdx-enepa-II/public/action/eis/search'
base_session = base_page %>% session()
search_form = html_form(base_session)[[2]]
search_form$fields$searchCriteria.onlyCommentLetters$value <- 'false'
#search_form <- set_values(search_form,searchCriteria.states = state)
search_session = session_submit(base_session,search_form)
hrefs = search_session %>% read_html() %>% html_nodes('a') %>% html_attr('href')
last_page = max(as.numeric(gsub('p=','',str_extract(hrefs[duplicated(hrefs)],'p=[0-9]{1,}'))))
print(last_page)

next_link = which(search_session %>% read_html() %>% html_nodes('a') %>% html_text() == 'Next')[1]
search_session = session_follow_link(search_session,i = next_link)

p = 1
keep_going = T

#search_session = rvest::session_jump_to(search_session,'?searchCritera.primaryStates=&d-446779-p=137&reset=Reset&searchCriteria.onlyCommentLetters=false')

#while(p < last_page & keep_going){
while(keep_going & length(page_set)>0 & p %in% page_set){
  print(p)
  search_session = rvest::session_jump_to(search_session,paste0('?searchCritera.primaryStates=&d-446779-p=',p,'&reset=Reset&searchCriteria.onlyCommentLetters=false'))
  #p = as.numeric(str_extract(str_extract(search_session$url,'\\bp=[0-9]{1,}\\b'),'[0-9]{1,}'))
  new_record = {search_session %>% read_html() %>% html_table(trim=T)}[[1]]
  new_record$eis_url = paste0('https://cdxapps.epa.gov',grep('details\\?eisId=[0-9]{1,}$',search_session%>% read_html() %>% html_nodes('a') %>% html_attr('href'),value=T))
  new_record <- data.table(new_record) 
  colnames(new_record) <- gsub('\\s','.',colnames(new_record))
  new_record[,Download.Documents:=NULL]
  new_record$Title <- enc2utf8(new_record$Title)
  new_record$Title <- iconv(new_record$Title, "UTF-8", "UTF-8",sub='')
  new_record$Title <- str_remove_all(new_record$Title,'\\\"')
  new_record = new_record[!paste(Title,Federal.Register.Date) %in% paste(record_df$Title,record_df$Federal.Register.Date),]

  if(nrow(new_record)==0&!recheck){keep_going <<-FALSE}
  else if(nrow(new_record)==0&recheck){page_set <<- page_set[!page_set %in% p];p <- page_set[1];Sys.sleep(0.25);next}
  else{
    #if(nrow(new_record)==0){break}
    eis_info = lapply(seq_along(new_record$eis_url),function(x) {
      new_record$eis_url[x]
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
  page_set <<- page_set[!page_set %in% p]
  p <- page_set[1]
  Sys.sleep(0.25)
  }
#if(any(duplicated(record_df))){
#    record_df <- record_df[!duplicated(record_df),]
#    break}
#if(current_page!=last_page)
#current_page = current_page + 1
}

#

saveRDS(object = record_df,file = fname)


