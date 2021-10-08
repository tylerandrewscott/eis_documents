corp = list.files('enepa_repository/corpus/',full.names = T,recursive = T)

keep_corp = corp[str_extract(corp,'[0-9]{4}') %in% 2013:2020]

require(data.table)

all_corp = lapply(keep_corp,fread,sep = '\t')

corp_dt = rbindlist(all_corp)

saveRDS(object = corp_dt, file = '../tuolumne/boilerplate_project/input/eis_corpus_2013-2020.rds')

meta_files = list.files('enepa_repository/meta_data/',full.names = T,recursive = T)
for(m in meta_files){
  file.copy(from = m,to = paste0('../tuolumne/boilerplate_project/input/',basename(m)))
}