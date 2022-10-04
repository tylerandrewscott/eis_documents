# ###two packages that will do most of what you need
library(tidyverse)
library(rvest)
# #progress bar lapply, useful for gauging how long/bad your code is in an apply loop
library(pbapply)


proj_url = read_csv('agency_nepa_libraries/usfs/metadata/forest_project_page_urls.csv')


# # ### css tage to get the project archives from each page
# # ### I use the "selectorgadget" chrome add on
output_titles_css = "#outputDiv div a"
# # 
# # ### loop through archive pages
fs_df = do.call(rbind,pblapply(1:nrow(proj_url), function(x) 
{
  #    ## not needed, good for monitoring progress and debugging
  #    #print(proj_archives[x])
  # #   ### read url, find link nodes
  links = read_html(proj_url$project_urls[x]) %>% html_nodes(css = output_titles_css) 
  # #   ### link text project names
  tx = links %>% html_text(trim=T)
  # #if no links, skip the rest
  if(length(tx)!=0){
    #  ### link href to projects
    tl = links %>% html_attr('href')
    # #   ### make little dataframe with project name, project url, and archive page
    data.frame(Project = tx,Page = tl,NF = proj_url$project_urls[x],stringsAsFactors = F,Status = proj_url$status[x])
  }
}))
base_usfs = "https://www.fs.usda.gov"
fs_df$Page[grepl("/project/?project=",fs_df$Page,fixed = T)&!grepl("http",fs_df$Page,fixed = T)] = paste0(base_usfs,fs_df$Page[grepl("/project/?project=",fs_df$Page,fixed = T)&!grepl("http",fs_df$Page,fixed = T)])
# # 
# # #### save project listing df
write_csv(x = fs_df,path = paste0('agency_nepa_libraries/usfs/metadata/forest_service_project_overview_', Sys.Date(),".csv"))
# # 


library(tidyverse)
library(rvest)
# #progress bar lapply, useful for gauging how long/bad your code is in an apply loop
library(pbapply)
rec = list.files('agency_nepa_libraries/usfs/metadata/',"forest_service_project_overview_",full.names = T)
fl = do.call(rbind,lapply(rec,file.info))
fdf = read_csv(rec[which(max(fl$mtime)==fl$mtime)])
fdf$Normal_Page = (grepl('project/?project',fdf$Page,fixed = T)&grepl('fs.usda.gov',fdf$Page,fixed = T))+0
fdf$Good_Project_Page = NA
possible_detail_items = c("Expected Analysis Type","Categorical Exclusion"  ,  "Last Updated:",
                          "Special Authority", "Lead Management Unit","Notice and Comment Regulation", "Project Purpose",              
                          "Project Activity","Current Status",  "Project Milestones","Decision Signed Date",         
                          "Legal Notice Date","Last Completed Milestone:",  "Next Milestone:")

project_data_in_lists = pblapply(which(fdf$Normal_Page==1),function(p){
  print(p)
  #print(x)
  proj_page = read_html(fdf$Page[p])
  css_get_project_brief = "style+ p"
  css_location_summary = "h2+ p"
  ov_suffix = "&exp=overview"
  de_suffix = "&exp=detail"
  lo_suffix = "&exp=location"
  Brief_Summary = proj_page %>% html_nodes(css_get_project_brief) %>% html_text(trim=T)
  Location_Summary = proj_page %>% html_nodes(css_location_summary) %>% html_text(trim=T)
  css_location_values = "#centercol p~ p+ p"
  
  ld_stuff = paste0(fdf$Page[p],lo_suffix) %>% read_html() %>% html_nodes(css_location_values) %>% html_text(trim=T)
  ld_stuff = ld_stuff[ld_stuff != '']
  loc_colnames = str_extract(ld_stuff,'^Forest|^District|^State|^County|^Counties|^Legal Land Description')
  loc_values = gsub('^Forest|^District|^State|^County|^Counties|^Legal Land Description','',ld_stuff)
  loc_values = gsub('^: \n','',loc_values)
  ldf = data.frame(t(loc_values),stringsAsFactors = F)
  colnames(ldf) <- loc_colnames
  
  css_names = "#centercol strong"
  css_detail_items = "h2~ p"
  detail_names = paste0(fdf$Page[p],de_suffix) %>% read_html() %>% html_nodes(css_names) %>% html_text(trim=T)
  detail_items = paste0(fdf$Page[p],de_suffix) %>% read_html() %>% html_nodes(css_detail_items) %>% html_text(trim=T)
  coln = str_extract(detail_items,paste0(possible_detail_items,collapse='|'))
  values = gsub(paste0(possible_detail_items,collapse='|'),'',detail_items)
  detail_df = data.frame(t(values),stringsAsFactors = F)
  colnames(detail_df) <- coln
  
  proj_df = data.frame(Project_Page = fdf$Page[p],
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
      
      df = data.frame(Stage=x,
                      Document_Name = tab %>% html_text(trim=T),
                      Document_File = tab %>% html_attr('href'),stringsAsFactors = F)
      df})
    
    proj_docs_df = do.call(rbind,proj_docs_list) %>% filter(Document_File!='#')
    proj_docs_df$Project_Page = fdf$Page[p]
    temp_list = list(proj_df = proj_df,docs_df = proj_docs_df)
  }
  if(length(tabs_for_project)==0){temp_list = list(proj_df = proj_df)}
  Sys.sleep(0.25)
  fdf$Good_Project_Page[p]<- (class(proj_df)=='data.frame')
  if(class(proj_df)=='data.frame'){temp_list}
})

saveRDS(object = project_data_in_lists,'agency_nepa_libraries/usfs/metadata/proj_rec_list.RDS')


library(stringr)

proj_dets = lapply(project_data_in_lists,function(x) x$proj_df[1,])
all_proj_records = Reduce(plyr::rbind.fill,proj_dets)
all_doc_records = do.call(rbind,lapply(project_data_in_lists,function(x) x$docs_df))
## not quite sure at this point whether duplications are in the data or due to coding error
all_doc_records = all_doc_records[!duplicated(all_doc_records),]
all_proj_records = all_proj_records[!duplicated(all_proj_records),]
all_proj_records$Project_Num = str_extract(all_proj_records$Project_Page,'[0-9]{1,}$')
all_doc_records$Project_Num = str_extract(all_doc_records$Project_Page,'[0-9]{1,}$')
all_proj_records$Lead.Management.Unit = gsub('\n|\t|\r','',all_proj_records$Lead.Management.Unit)
all_proj_records$Project.Purpose = gsub('\n|\t|\r','',all_proj_records$Project.Purpose)
all_proj_records$Project.Activity = gsub('\n|\t|\r','',all_proj_records$Project.Activity)
all_proj_records$Current.Status = gsub('\n|\t|\r','',all_proj_records$Current.Status)
all_proj_records$Expected.Analysis.Type = gsub('\n|\t|\r','',all_proj_records$Expected.Analysis.Type)
#all_proj_records$forest = gsub('projects\\/|\\/landmanagement','',str_extract(all_proj_records$Project_Page,'projects/.+/landmanagement'))
all_proj_records$Proj_Num = str_extract(all_proj_records$Project_Page,'[0-9]{1,}$')
library(tidyverse)
all_doc_records$File_Name = paste0(all_doc_records$Project_Num,'_',str_extract(all_doc_records$Document_File,'[^\\/]+$'))
#all_doc_records$forest = all_proj_records$forest[match(all_doc_records$Project_Num,all_proj_records$Proj_Num)]
write_csv(all_proj_records,paste0('agency_nepa_libraries/usfs/metadata/forest_service_project_detail_',Sys.Date(),".csv"))
write_csv(all_doc_records,paste0('agency_nepa_libraries/usfs/metadata/forest_service_document_',Sys.Date(),".csv"))
