library(data.table)
library(rvest)
library(httr)
library(lubridate)
library(pbapply)
library(stringr)
proj = fread('agency_nepa_libraries/usfs/metadata/forest_service_project_detail.csv')
proj$Year = year(ymd(proj$`Decision Signed Date`))
proj$Year[is.na(proj$Year)|proj$Year==''] <- year(mdy(proj$`Last Updated:`[is.na(proj$Year)|proj$Year=='']))
docs  = fread('agency_nepa_libraries/usfs/metadata/forest_service_document_record.csv')
docs = docs[!duplicated(Document_File),]
docs$Year = proj$Year[match(docs$Project_Num,proj$Proj_Num)]
docs = docs[grepl('^h',docs$Document_File),]
floc = 'agency_nepa_libraries/usfs/documents/'
docs = docs[order(-Year),]
docs = docs[Year>2010,]


flist = list.files(floc,recursive = T,full.names = T)
flist = gsub('\\/\\/','\\/',flist)
finifo = file.info(flist)
finifo = data.table(finifo)
#dtime = lubridate::parse_date_time(finifo$mtime,orders = "YmdHMS")
fl = paste0(floc,docs$Year,'/',paste(docs$Project_Num,basename(docs$Document_File),sep = '_'))
       
docs = docs[!fl%in%flist,]

proj$`Expected Analysis Type` = str_remove_all(proj$`Expected Analysis Type`,'\\r|\\t|\\n')
docs$Analysis_Type = proj$`Expected Analysis Type`[match(docs$Project_Num,proj$Proj_Num)]
docs = docs[grepl('pdf$|PDF$',Document_File),]
ex = file.exists(paste0(floc,docs$Year,'/',paste(docs$Project_Num,basename(docs$Document_File),sep = '_')))

seqalong = (!ex & docs$Analysis_Type %in% c('Environmental Assessment','Environmental Impact Statement')&
              docs$Stage %in% c('Analysis','Decision','Assessment'))
seqalong = !ex

pblapply(which(seqalong),function(x) {
  Sys.sleep(0.1)
  if(!dir.exists(paste0(floc,docs$Year[x]))){dir.create(paste0(floc,docs$Year[x]))}
  if(!file.exists(paste0(floc,docs$Year[x],'/',paste(docs$Project_Num[x],basename(docs$Document_File[x]),sep='_')))){
    tryCatch(httr::GET(docs$Document_File[x], #verbose(),
               write_disk(paste0(floc,docs$Year[x],'/',paste(docs$Project_Num[x],basename(docs$Document_File[x]),sep='_'))), overwrite=TRUE),error=function(e) NULL)
  }
},cl = 4)



