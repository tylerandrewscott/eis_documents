### this script runs a set of scripts in sequence to update the EIS library
#scrape current EPA site for new EIS records
source('enepa_repository/code/scrape_record_set.R')

##### create symbolic links
# code and metadata live on github, but corpus is too big
# corpus lives in Box
# this sets symlink so that files appear to live in same directory
real_location = '~/Library/CloudStorage/Box-Box/eis_documents/enepa_repository/documents/'
symbolic_location  = 'enepa_repository/'
term_call <- paste0('ln -s ',real_location,' ',symbolic_location)
system(term_call)

#this does the same thing for text representations
real_location = '~/Library/CloudStorage/Box-Box/eis_documents/enepa_repository/text_as_datatable/'
symbolic_location  = 'enepa_repository/'
term_call <- paste0('ln -s ',real_location,' ',symbolic_location)
system(term_call)


#download observed docs not currently possessed
source('enepa_repository/code/download_eis_files.R')
#convert pdfs into clean-ish .txt data.table files
source('enepa_repository/code/make_filter_text_tables.R')

####### this file should only be run if you have manually added other pdfs to the extradocs folder
#source('enepa_repository/code/create_extradoc_metadata.R')
