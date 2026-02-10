library(pdftools)
library(data.table)
library(tidyverse)


flist <- list.files('enepa_repository/box_files/documents/',full.names = T,recursive = T)
frecord <- readRDS('enepa_repository/metadata/eis_document_record.rds')

flist <- flist[grepl('pdf$|PDF$',flist)]

for(i in rev(flist)){
  f <- basename(i)
  info <- tryCatch(pdf_info(i),error = function(e) NULL)
  if(is.null(info)){
    print('bad PDF!')
    print(paste('removing',f))
    frecord$BAD_FILE[frecord$File_Name==f] <- T
    file.remove(i)
  }
}
