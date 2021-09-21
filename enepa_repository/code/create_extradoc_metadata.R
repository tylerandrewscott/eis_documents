require(data.table)
dir = 'enepa_repository/documents/extra_docs/'
fls = list.files(dir)

file.rename(paste0(dir,fls),paste0(dir,gsub('\\s','_',fls)))
fls = list.files(dir)
require(stringr)
dt = data.table(EIS.Number = str_extract(fls,'^[0-9]{8}'),File_Name = fls)
fwrite(dt,'enepa_repository/meta_data/extra_docs.csv')

