
#devtools::install_github("govscienceuseR/referenceExtract")
library(referenceExtract)
library(data.table)
library(stringr)
library(pbapply)
#note: to make this work, you probably want to install chruby on your computer, and then use chruby to run anystyle in new ruby that isn't what the system uses
#https://mac.install.guide/ruby/12.html
#then you can install https://github.com/inukshuk/anystyle

CLOBBER = F ### don't do this unless you want it to run for a very long time

doc_loc <- 'enepa_repository/documents/2023/'
js_loc <- 'enepa_repository/reference_jsons/'
if(!dir.exists(js_loc)){dir.create(js_loc)}
fls <- list.files(doc_loc,full.names=T,recursive = T, pattern = 'pdf$|PDF$')

ref_dir<-js_loc
js_files <- list.files(js_loc)

if(!CLOBBER){
  need <- !basename(str_replace(fls,'pdf$|PDF$','json')) %in% basename(js_files)
  fls <- fls[need]
}
library(pdftools)

### might need to install anystyle
# gem install anystyle 
referenceExtract::reference_extract(files = fls,ref_dir = js_loc,cores = 4)
