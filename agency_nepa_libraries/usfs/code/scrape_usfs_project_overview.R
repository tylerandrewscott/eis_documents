# ###two packages that will do most of what you need
library(tidyverse)
library(rvest)
# #progress bar lapply, useful for gauging how long/bad your code is in an apply loop
library(pbapply)
library(data.table)

file = 'agency_nepa_libraries/usfs/metadata/forest_service_project_overview.csv'
if(!file.exists(file)){last_read = data.table(stringsAsFactors = F)}else{last_read = fread(file)}
#last_read = data.table(stringsAsFactors = F)
proj_url = fread('agency_nepa_libraries/usfs/metadata/forest_project_page_urls.csv')
proj_url = proj_url[grepl('https',proj_url$project_urls),]

sapply(proj_url$project_urls,httr::url_ok)
# # ### css tage to get the project archives from each page
# # ### I use the "selectorgadget" chrome add on
output_titles_css = "#outputDiv div a"


# # 
# # ### loop through archive pages
fs_list = pblapply(1:nrow(proj_url), function(x) 
{
  print(proj_url$project_urls[x])
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
    data.table(nf = proj_url$nf[x],Project = tx,Page = tl,NF = proj_url$project_urls[x],stringsAsFactors = F,Status = proj_url$status[x])
  }
},cl = 1)

fs_df = rbindlist(fs_list)

base_usfs = "https://www.fs.usda.gov"
fs_df$Page[grepl("/project/?project=",fs_df$Page,fixed = T)&!grepl("http",fs_df$Page,fixed = T)] = paste0(base_usfs,fs_df$Page[grepl("/project/?project=",fs_df$Page,fixed = T)&!grepl("http",fs_df$Page,fixed = T)])
# # 


new_results = rbindlist(list(fs_df,last_read),use.names = T,fill=T)
new_results <- new_results[!duplicated(new_results),]
# # #### save project listing df

write_csv(x = new_results,path = paste0('agency_nepa_libraries/usfs/metadata/forest_service_project_overview.csv'))
# # 

