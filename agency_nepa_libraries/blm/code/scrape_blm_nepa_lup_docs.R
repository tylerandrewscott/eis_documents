
library(rvest)
library(data.table)
library(httr)
library(stringr)
library(xml2)
base = 'https://eplanning.blm.gov/'
blm_proj = fread('agency_nepa_libraries/blm/metadata/project_record_lup.csv')

doc_cols = '.epl_site_page_doclist_col_title'
doc_entries = '.epl_site_page_doclist_col_entry'
doc_link = '.epl_register_result'
file_storage = 'agency_nepa_libraries/blm/nepa_documents/'
blm_proj$Year = str_extract(gsub('^[A-Z0-9]{3,}-','',gsub('DOI-BLM-([A-Za-z0-9]|\\s)+-','',blm_proj$`NEPA #`)),'^[0-9]{4}')


nothing = integer(0)
#start_df = data.table()
drecord = 'agency_nepa_libraries/blm/metadata/document_lup_record.csv'
if(!file.exists(drecord)){start_df = data.table()}#EIS.Number = as.numeric(),Original_File_Name = as.character(),File_Name =as.character(),stringsAsFactors = F)}
if(file.exists(drecord)){start_df = fread(drecord,stringsAsFactors = F)}

library(doParallel)
library(parallel)

# cl = makeCluster(6)
# registerDoParallel(cl)
# clusterEvalQ(cl,require(rvest))
# clusterEvalQ(cl,require(httr))
# clusterEvalQ(cl,require(data.table))
# clusterEvalQ(cl,require(xml2))
# 
# parallel::clusterExport(cl,list(ls()[ls()!='cl']))

#for(i in c(1479,30347)){
#  print(i)
#if(file.exists('agency_nepa_libraries/blm/metadata/highest_reached.RDS')){start = readRDS('agency_nepa_libraries/blm/metadata/highest_reached.RDS')}else{start = 1}
#> start_df %>% dim()
#[1] 1053   10
start = 1


dtable_list = foreach(i = which(!blm_proj$`NEPA #` %in% start_df$NEPA_ID)) %do% {#nrow(blm_proj):start) %do% {
  #nrow(blm_proj)){
  print(i) 
  rm(doc_table);rm(ptable);rm(link_num)
  ptable = data.table(stringsAsFactors = F)
  #i = which(blm_proj$`NEPA #`=='DOI-BLM-WY-D030-2006-0001-EIS')
  Sys.sleep(0.1)
  url = paste0(base,blm_proj$href[i])
  page = rvest::html_session(url)
  base_page = read_html(page)
  
  a_titles = base_page %>% html_nodes('a') %>% html_attr('title')
  separate_doc_page  = any(grepl("Display Documents",a_titles))
  link_num =  grep("Display Documents",a_titles)
                          
  subd = blm_proj$Year[i]
  if(!separate_doc_page){
    tabs = page  %>% html_nodes('table') 
    if(any(!is.na(tabs %>% html_attr('class')) & {(tabs %>% html_attr('class')) %in% c('tablesorter','epl_document_list')})){
      wt = which(!is.na(tabs %>% html_attr('summary')) & !grepl('Maps|maps',tabs %>% html_attr('summary')) & grepl('document|Document',tabs %>% html_attr('summary')))
      if(!identical(wt,nothing)){
        doc_table = rbindlist(lapply(wt,function(t) {
          temp = data.table(tabs[[t]]  %>% html_table(trim=T,fill = T))
          names(temp) <- gsub(' ',' ',names(temp))
          temp = temp[`Document Name`!='',]
          url_vector = unique(tabs[[t]] %>% html_nodes('a') %>% html_attr('href'))
          url_vector = grep('interactive_document',url_vector,value=T,invert=T)
          temp$url = url_vector
          temp$File_Name = basename(temp$url)
          temp}))
        doc_table$NEPA_ID = blm_proj$`NEPA #`[i]
        doc_images =  page %>% html_nodes('img')
        doc_table$Set = !is.na(doc_images %>% html_attr('title'))& doc_images %>% html_attr('title')=='Document set'
        doc_table = doc_table[!grepl('Shapefiles',`Document Name`),]
        for(d in 1:nrow(doc_table)){
          if(doc_table$Set[d]){
            document_page = page %>% rvest::follow_link(i = min(which(page %>% read_html() %>% html_nodes('a') %>% html_attr('href')  == doc_table$url[d])))
            document_html = document_page %>% read_html() 
            dlinks =  document_html%>% html_nodes('a')%>% html_attr('href')
            files = basename(dlinks)
            names =  document_html %>% html_nodes('.epl_site_page_doclist_col_entry') %>% html_text(trim=T)
            set_table =  data.table(`Document Name` = names,Set_Name = doc_table$`Document Name`[d],Set = T,
            url =  document_html%>% html_nodes(xpath="//a[contains(@href, 'epl-front-office')]")%>% html_attr('href'),File_Name = basename( document_html%>% html_nodes(xpath="//a[contains(@href, 'epl-front-office')]")%>% html_attr('href')))
            set_table$NEPA_ID = blm_proj$`NEPA #`[i]
            names(set_table) <- gsub(' ',' ',names(set_table))
            doc_table = rbindlist(list(doc_table,set_table),fill=T,use.names = T)
            rm(set_table)
          }
        }  
        ptable = rbindlist(list(ptable,doc_table),use.names=T,fill = T)
      }
    }
  }    
  
  if(separate_doc_page){
    document_page = page %>% rvest::follow_link(i = link_num)
    tabs = document_page  %>% html_nodes('table') 
    wt = which(!is.na(tabs %>% html_attr('summary')) & !grepl('Maps|maps',tabs %>% html_attr('summary')) & grepl('document|Document',tabs %>% html_attr('summary')))
    if(!identical(wt,nothing)){
      section_titles = document_page %>% html_nodes('.epl_site_page_doc_category_name') %>% html_text(trim=T)
      doc_table = rbindlist(lapply(seq_along(wt),function(t) {
      doc_table = data.table(tabs[[wt[t]]]  %>% html_table(trim=T,fill = T))
      names(doc_table) <- gsub(' ',' ',names(doc_table))
      doc_table = doc_table[`Document Name`!='',]
      url_vector = unique(tabs[[wt[t]]] %>% html_nodes('a') %>% html_attr('href'))
      url_vector = grep('interactive_document|Commenting_Instructions',url_vector,value=T,invert=T)
      doc_table$Set_Type = section_titles[t]
      doc_table}))
      doc_table$NEPA_ID = blm_proj$`NEPA #`[i]
      doc_images =  document_page %>% html_nodes('img')
      doc_table$Set = !is.na(doc_images %>% html_attr('title'))& doc_images %>% html_attr('title')=='Document set'
      url_vector = unlist((sapply(tabs[wt],function(t) unique(t %>% html_nodes('a') %>% html_attr('href')))))
      url_vector = grep('interactive_document',url_vector,value=T,invert=T)
      doc_table$url <- NA
      doc_table$url[doc_table$`Available Formats`!=''] <- url_vector
      doc_table = doc_table[!is.na(url),]
      doc_table$File_Name = basename(doc_table$url)
      for(s in 1:nrow(doc_table)){
        if(grepl('documentId=[0-9]{1,}$',doc_table$url[s])){
          subset_html = read_html(paste0(base,doc_table$url[s]))
          dlinks =  subset_html%>% html_nodes('a')%>% html_attr('href')
          files = basename(dlinks)
          names =  subset_html %>% html_nodes('.epl_site_page_doclist_col_entry') %>% html_text(trim=T)
          set_table =  data.table(`Document Name` = names,Set_Name = doc_table$`Document Name`[s],Set = T,
                         url =  subset_html%>% html_nodes(xpath="//a[contains(@href, 'epl-front-office')]")%>% html_attr('href'),
                         File_Name = basename( subset_html%>% html_nodes(xpath="//a[contains(@href, 'epl-front-office')]")%>% html_attr('href')))
          names(set_table) <- gsub(' ',' ',names(set_table))
          set_table$NEPA_ID = blm_proj$`NEPA #`[i]
          doc_table = rbindlist(list(doc_table,set_table),use.names = T,fill=T)
        }
      }
      ptable = rbindlist(list(ptable,doc_table),use.names=T,fill = T)
    }
  }
  
  #fwrite(x = doc_df,file = paste0('agency_nepa_libraries/blm/metadata/document_record.csv'),row.names = F)
  #saveRDS(i,'agency_nepa_libraries/blm/metadata/highest_reached.RDS')
  if(nrow(ptable)>0){
    #doc_table[,V5:=NULL]
    #doc_table[,`Public Participation`:=NULL]  
    start_df = rbindlist(list(start_df,ptable),fill=T,use.names = T)
    start_df = start_df[!duplicated(start_df),]
    fwrite(x = start_df,file = paste0(drecord),row.names = F)
    }
  #saveRDS(i,'agency_nepa_libraries/blm/metadata/highest_reached.RDS')
} 



#stopImplicitCluster()
#stopCluster(cl)
#fwrite(x = doc_df,file = paste0('agency_nepa_libraries/blm/metadata/document_record.csv'),row.names = F)

