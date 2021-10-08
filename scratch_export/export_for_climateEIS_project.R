require(data.table)
require(stringr)
which_years = c(2013:2021)
corp = list.files('enepa_repository/corpus/',full.names = T,recursive = T)

keep_corp = corp[str_extract(corp,'[0-9]{4}') %in% which_years]
meta_files = list.files('enepa_repository/meta_data/',full.names = T,recursive = T)

proj = fread(grep('record_detail',meta_files,value = T))
proj = proj[Document=='Draft',]
proj = proj[str_extract(proj$EIS.Number,'^[0-9]{4}') %in% which_years,]

docs = fread(grep('document',meta_files,value = T))
docs = docs[EIS.Number %in% proj,]

fwrite(docs,file = paste0('../tuolumne/climate_in_eis_project/input/deis_document_record.csv'))
fwrite(proj,file = paste0('../tuolumne/climate_in_eis_project/input/deis_project_record.csv'))

for(corp in keep_corp){
temp_cor = fread(corp,sep = '\t')
savedir = '../tuolumne/climate_in_eis_project/input/'
dir.create(savedir)
temp_deis_cor = temp_cor[str_extract(temp_cor$File,'^[0-9]{8}') %in% proj$EIS.Number,]
saveRDS(temp_deis_cor, file = paste0(savedir,paste0('deis_corpus_',str_extract(corp,'[0-9]{4}'),'.txt')))
}

