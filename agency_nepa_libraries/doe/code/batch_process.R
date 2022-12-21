### this script runs a set of scripts in sequence to update DOE NEPA data
# scrape project records
source('agency_nepa_libraries/doe/code/scrape_doe_nepa_records.R')
# scrape documents associated with projects
source('agency_nepa_libraries/doe/code/scrape_doe_document_records.R')
##### create symbolic links
# code and metadata live on github, but corpus is too big
# corpus lives in Box
# this sets symlink so that files appear to live in same directory
real_location = '~/Library/CloudStorage/Box-Box/eis_documents/agency_nepa_libraries/doe/documents/'
symbolic_location  = 'agency_nepa_libraries/doe/'
term_call <- paste0('ln -s ',real_location,' ',symbolic_location)
system(term_call)

#this does the same thing for text representations
real_location = '~/Library/CloudStorage/Box-Box/eis_documents/agency_nepa_libraries/doe/text_as_datatable/'
symbolic_location  = 'agency_nepa_libraries/doe/'
term_call <- paste0('ln -s ',real_location,' ',symbolic_location)
system(term_call)


#download observed docs not currently possessed
source('agency_nepa_libraries/doe/code/download_doe_nepa_documents.R')
#convert pdfs into clean-ish .txt data.table files
source('agency_nepa_libraries/doe/code/make_filtered_text_tables.R')

####### this file should only be run if you have manually added other pdfs to the extradocs folder
#source('enepa_repository/code/create_extradoc_metadata.R')
