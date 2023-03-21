

library(rvest)
library(data.table)
library(httr)
library(stringr)
library(xml2)
require(RSelenium)
require(wdman)
require(doParallel)


base = 'https://eplanning.blm.gov/'
blm_proj = fread('agency_nepa_libraries/blm/metadata/new_project_record.csv')

det_file = 'agency_nepa_libraries/blm/metadata/new_project_record_detailed.rds'
if(file.exists(det_file)){dets = readRDS(det_file)}else{dets = data.table()}

blm_proj$pnum = str_extract(blm_proj$link,'[0-9]{1,}$')
blm_proj = blm_proj[!blm_proj$pnum %in% dets$projectId,]


base_query = 'https://eplanning.blm.gov/eplanning-ws/epl/site/getProjectSite/'
require(pbapply)
require(jsonlite)
dt_list = pblapply(blm_proj$pnum,function(x) {
  page = paste0(base_query,x)
  result = fromJSON(page)
  Sys.sleep(0.25)
  result[sapply(result,length)==0]<-NA
  result = result[1:which(names(result)=='contacts')]
  vector_response = sapply(sapply(result,nrow),is.null)
  vector_dt = as.data.table(result[vector_response])
  list_dt = as.data.table(lapply(names(result[!vector_response]),function(x) {
    data.table(x=list(result[[x]]))}))
  names(list_dt)<-names(result[!vector_response])  
  proj_dt = cbind(vector_dt,list_dt)
  proj_dt
  },cl = 4)
dt_list = dt_list[sapply(dt_list,class)!='try-error']
all_dt  = rbindlist(dt_list,use.names=T,fill = T)

if(nrow(all_dt)>0){
  dets = rbind(dets,all_dt,use.names = T,fill = T)
}
saveRDS(dets,det_file)

