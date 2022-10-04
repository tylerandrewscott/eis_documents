###### NOTE THAT SOMETHING CHANGED IN THE USFS SITE AND THE ORIGINAL DOENS' TWORK CORRECTLY
##### ALSO SOME BAD  CHARACTERS CREATE 3M CHARACTER TEXT STRINGS, NEED TO CATCH THOSE
  
# ###two packages that will do most of what you need
library(tidyverse)
library(rvest)
# #progress bar lapply, useful for gauging how long/bad your code is in an apply loop
library(pbapply)
library(tidyverse)
library(rvest)
# #progress bar lapply, useful for gauging how long/bad your code is in an apply loop
library(pbapply)
library(data.table)

#flist = list.files('agency_nepa_libraries/usfs/metadata/',pattern = 'forest_service_project_overview',full.names = T)
#finfo = file.info(flist)
#last_read = fread(flist[which.max(finfo$mtime)])
#rec = list.files('agency_nepa_libraries/usfs/metadata/',"forest_service_project_overview_",full.names = T)
#fdf = last_read
fdf = fread('agency_nepa_libraries/usfs/metadata/forest_service_project_overview.csv')
fdf$Normal_Page = (grepl('project\\/\\?project|/nepa_project_exp|fs-usda-pop',fdf$Page,fixed = F)&grepl('fs\\.usda\\.gov|fs\\.fed\\.us',fdf$Page,fixed = F))+0
fdf$Normal_Page[grepl('test',fdf$Page)] <- 0
fdf$Good_Project_Page = NA

library(httr)
u <- paste0(fdf$Page[1],"&exp=detail")
g <- httr::GET(u)
good_url <- identical(status_code(g), 200L)
ht <- read_html(u)
det_names <- ht%>%html_nodes("#centercol strong")
dets <- ht %>% html_nodes("#centercol h2+ p , #centercol p+ p")

html_text(dets[9])
as.character(dets)[1]
str_extract(as.character(dets),"(<\\/strong>).+?(<strong>|$)")

str_replace(dets[1],'(<\\/strong>)(.+?)([^strong])')

,'\\2')

dets %>% html_nodes('strong')

dets
length(det_names)
length(dets)

good_url

 %>% html_nodes("#centercol strong")
read_html(u) %>% html_nodes("#centercol strong")
httr::url_success(fdf$Page[1])




project_detail_file = 'agency_nepa_libraries/usfs/metadata/forest_service_project_detail.csv'
#det_info = file.info(project_detail_files)
#if(nrow(det_info)>0){all_proj_records = fread(project_detail_files[which.max(det_info$mtime)])}else{all_proj_records = data.table(stringsAsFactors = F)}
if(!file.exists(project_detail_file)){all_proj_records = data.table(stringsAsFactors = F)}else{all_proj_records = fread(project_detail_file)}

document_file = 'agency_nepa_libraries/usfs/metadata/forest_service_document_record.csv'
if(!file.exists(document_file)){all_doc_records = data.table(stringsAsFactors = F)}else{all_doc_records = fread(document_file)}

possible_detail_items = c("Expected Analysis Type","Categorical Exclusion"  ,  "Last Updated:",
                          "Special Authority", "Lead Management Unit","Notice and Comment Regulation", "Project Purpose",              
                          "Project Activity","Current Status",  "Project Milestones","Decision Signed Date",         
                          "Legal Notice Date","Last Completed Milestone:",  "Next Milestone:")

switch_pages = which(fdf$Normal_Page==0&grepl('project=[0-9]{1,}|cid=.+[0-9]$',fdf$Page))
fdf$Page[switch_pages] = paste0('https://www.fs.usda.gov/project/?',str_extract(fdf$Page[switch_pages],'project=[0-9]{1,}$|cid=.+[0-9]$'))
fdf$Normal_Page[switch_pages] <- 1

fdf$Project = gsub('\"','',fdf$Project,fixed=T)
fdf$Project = gsub('""','',fdf$Project,fixed=T)

# grep('test',fdf$Page,value=T)
btry = 'https://www.fs.usda.gov/project/?project='
usfs_internal = fread('../manitou/common_inputs/Copy of MYTR_National Level(1100).xlsx - Decision Data Set.csv')
usfs_internal$ongoing = 0
usfs_internal2 = data.table(readRDS('../manitou/common_inputs/FS_ongoing_projects_11-2019.rds'),stringsAsFactors = F)
numcols = colnames(usfs_internal2)[as.vector(apply(usfs_internal2,2,function(x) !any(grepl('[^0-9]',x))))]
usfs_internal2[,(numcols):=lapply(.SD,as.numeric),.SDcols = numcols]
usfs_internal = merge(usfs_internal,usfs_internal2,all = T)
usfs_internal = usfs_internal[!duplicated(paste(`PROJECT NUMBER`,`LMU (ACTUAL)`)),] 
usfs_internal$`PROJECT NAME`= gsub('\"','',usfs_internal$`PROJECT NAME`,fixed=T)
usfs_internal$`PROJECT NAME` = gsub('""','',usfs_internal$`PROJECT NAME`,fixed=T)
fdf$PID = str_extract(fdf$Page,'[0-9]{1,}$')
fdf$PID[is.na(fdf$PID)] <- usfs_internal$`PROJECT NUMBER`[match(toupper(fdf$Project[is.na(fdf$PID)]),toupper(usfs_internal$`PROJECT NAME`))]

second_switch = which(!is.na(fdf$PID) & fdf$Normal_Page==0)
fdf$Page[second_switch] <- paste0(btry,fdf$PID[second_switch])
fdf$Normal_Page[second_switch] <- 1
fdf$Page[grepl('fs.fed.us',fdf$Page,fixed = T)&!is.na(fdf$PID)]<- paste0('https://www.fs.usda.gov/project/?project=',fdf$PID[grepl('fs.fed.us',fdf$Page,fixed = T)&!is.na(fdf$PID)])


(donthave = which(!fdf$Page %in% all_proj_records$Project_Page & fdf$Normal_Page==1))

fs = fread('../manitou/common_inputs/Copy of MYTR_National Level(1100).xlsx - Decision Data Set.csv')
fs$ongoing = 0
fs2 = data.table(readRDS('../manitou/common_inputs/FS_ongoing_projects_11-2019.rds'),stringsAsFactors = F)
numcols = colnames(fs2)[as.vector(apply(fs2,2,function(x) !any(grepl('[^0-9]',x))))]
fs2[,(numcols):=lapply(.SD,as.numeric),.SDcols = numcols]
fs = merge(fs,fs2,all = T)
fs = fs[!fs$`PROJECT NUMBER` %in% fdf$PID,]

basepage = 'https://www.fs.usda.gov/project/?project='
#donthave = which(is.na(fdf$Good_Project_Page)&fdf$Normal_Page==1)
#fdf[PID=='46078',]

id1 = fdf$PID[donthave]
id2 = fs$`PROJECT NUMBER`
ids = c(id1,id2)

project_data_in_lists = lapply(seq_along(ids),function(p){
  #which(fdf$Normal_Page==1 & !fdf$Page %in% all_proj_records$Project_Page[all_proj_records$Project_Status=='Archived']),function(p){
  #print(x)
  if(ids[p] %in% id1){url = as.character(fdf$Page[p])}else{url <- paste0(basepage,ids[p])}
  sess = session(url)
  proj_page = sess %>% read_html()
  if(proj_page %>% html_nodes('h1') %>% html_text(trim = T) != 'Invalid Project'){
  css_get_project_brief = "style+ p"
  css_location_summary = "h2+ p"
  ov_suffix = "&exp=overview"
  de_suffix = "&exp=detail"
  lo_suffix = "&exp=location"
  Brief_Summary = proj_page %>% html_nodes(css_get_project_brief) %>% html_text(trim=T)
  Location_Summary = proj_page %>% html_nodes(css_location_summary) %>% html_text(trim=T)
  Location_Summary <- Location_Summary[1]
  css_location_values = "#centercol p~ p+ p"
  ld_stuff = paste0(fdf$Page[p],lo_suffix) %>% html_session() %>% read_html() %>% html_nodes(css_location_values) %>% html_text(trim=T)
  ld_stuff = ld_stuff[ld_stuff != '']
  loc_colnames = str_extract(ld_stuff,'^Forest|^District|^State|^County|^Counties|^Legal Land Description')
  loc_values = gsub('^Forest|^District|^State|^County|^Counties|^Legal Land Description','',ld_stuff)
  loc_values = gsub('^: \n','',loc_values)
  ldf = data.table(t(loc_values),stringsAsFactors = F)
  colnames(ldf) <- loc_colnames
  #centercol p+ p , #centercol h2+ p
  css_names = "#centercol strong"
  css_detail_items = "#centercol p+ p , #centercol h2+ p"
  #css_detail_items = "h2~ p"
  detail_names = paste0(fdf$Page[p],de_suffix) %>% html_session() %>% read_html() %>% html_nodes(css_names) %>% html_text(trim=T)
  detail_items = paste0(fdf$Page[p],de_suffix) %>% html_session() %>% read_html() %>% html_nodes(css_detail_items) %>% html_text(trim=T)
  coln = str_extract(detail_items,paste0(possible_detail_items,collapse='|'))
  values = gsub(paste0(possible_detail_items,collapse='|'),'',detail_items)
  detail_df = data.table(t(values),stringsAsFactors = F)
  colnames(detail_df) <- coln
  
  proj_df = data.table(Project_Page = fdf$Page[p],
                       Project_Status = fdf$Status[p],Brief_Summary,Location_Summary,ldf,detail_df,stringsAsFactors = F)
  css_tab_links = ".tablinks"
  tabs_for_project = proj_page %>% html_nodes(css_tab_links) %>% html_text()
  
  if(length(tabs_for_project)>0)
  {
    proj_docs_list = lapply(tabs_for_project, function(x) {
      if(length(proj_page %>% html_nodes(css=paste0("#",x," a")) %>% html_text(trim=T))==0)
      {tab = proj_page %>% html_nodes(css=paste0("#tab_",paste0(which(x == tabs_for_project)-1)))}
      if(length(proj_page %>% html_nodes(css=paste0("#",x," a")) %>% html_text(trim=T))>0)
      {tab = proj_page %>% html_nodes(css=paste0("#",x," a"))}
      
      df = data.table(Stage=x,
                      Document_Name = tab %>% html_text(trim=T),
                      Document_File = tab %>% html_attr('href'),stringsAsFactors = F)
      df})
    
    proj_docs_df = rbindlist(proj_docs_list,use.names = T,fill = T)
    proj_docs_df = proj_docs_df[Document_File!='#',]
    proj_docs_df$Project_Page = fdf$Page[p]
    temp_list = list(proj_df = proj_df,docs_df = proj_docs_df)
  }
  if(length(tabs_for_project)==0){temp_list = list(proj_df = proj_df)}
  Sys.sleep(0.25)
  fdf$Good_Project_Page[p]<- any(class(proj_df)=='data.frame')
  if(any(class(proj_df)=='data.table')){temp_list}
}})

#saveRDS(object = project_data_in_lists,'scratch/proj_rec_list.RDS')

library(stringr)

keep = sapply(project_data_in_lists,class)
proj_dets = lapply(project_data_in_lists[keep=='list'],function(x) x$proj_df[1,])
new_proj_records = rbindlist(proj_dets,fill=T)
new_doc_records = rbindlist(lapply(project_data_in_lists[keep=='list'],function(x) x$docs_df))


## not quite sure at this point whether duplications are in the data or due to coding error
new_doc_records = new_doc_records[!duplicated(new_doc_records),]
new_proj_records = new_proj_records[!duplicated(new_proj_records),]
new_proj_records$Project_Num = str_extract(new_proj_records$Project_Page,'[0-9]{1,}$')
new_doc_records$Project_Num = str_extract(new_doc_records$Project_Page,'[0-9]{1,}$')
new_proj_records$Lead.Management.Unit = gsub('\n|\t|\r','',new_proj_records$Lead.Management.Unit)
new_proj_records$Project.Purpose = gsub('\n|\t|\r','',new_proj_records$Project.Purpose)
new_proj_records$Project.Activity = gsub('\n|\t|\r','',new_proj_records$Project.Activity)
new_proj_records$Current.Status = gsub('\n|\t|\r','',new_proj_records$Current.Status)
new_proj_records$Expected.Analysis.Type = gsub('\n|\t|\r','',new_proj_records$Expected.Analysis.Type)
#new_proj_records$forest = gsub('projects\\/|\\/landmanagement','',str_extract(new_proj_records$Project_Page,'projects/.+/landmanagement'))
new_proj_records$Proj_Num = str_extract(new_proj_records$Project_Page,'[0-9]{1,}$')
library(tidyverse)
new_doc_records$File_Name = paste0(new_doc_records$Project_Num,'_',str_extract(new_doc_records$Document_File,'[^\\/]+$'))
#new_doc_records$forest = new_proj_records$forest[match(new_doc_records$Project_Num,new_proj_records$Proj_Num)]

combined_proj_records = rbindlist(list(all_proj_records,new_proj_records),fill = T)
combined_doc_records =  rbindlist(list(all_doc_records,new_doc_records),fill = T)

fwrite(x = combined_proj_records ,file = paste0('agency_nepa_libraries/usfs/metadata/forest_service_project_detail.csv'))
fwrite(x = combined_doc_records,file = paste0('agency_nepa_libraries/usfs/metadata/forest_service_document_record.csv'))

# docs = fread('agency_nepa_libraries/usfs/metadata/forest_service_document_record.csv')
# test = fread('../../Downloads/forest_service_document_2019-08-12.csv')
# docs = docs[!duplicated(docs),]
# test = test[!duplicated(test),]
# flist = list.files('agency_nepa_libraries/usfs/documents/',recursive = T)
# baseflist = basename(flist)
# test = test[!test$File_Name %in% docs$File_Name]
# test = test[pbsapply(test$Document_File,RCurl::url.exists,cl = 6),]
# 
# temp = rbindlist(list(docs,test),use.name=T,fill=T)
# fwrite(x = temp,file = paste0('agency_nepa_libraries/usfs/metadata/forest_service_document_record.csv'))
# 

