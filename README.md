# eis_documents
Code to scrape and store NEPA documents

## content
There are two main subdirectorys:
/enepa_repository, which relates to the EPA's https://cdxapps.epa.gov/cdx-enepa-II/public/action/eis/search webpage containing a record of EISs filed w/ the EPA since October, 2012
/agency_nepa_libraries, which contain agency-specific records for some agencies that maintain NEPA records

## subdirectory structure
The enepa_repository and agency library subdirectories share a common structure:
\code used for scraping and data wrangling
\metadata records of projects and documents
\documents raw downloaded documents
\text_as_datatable full text conversion of each document with each page stored as a row in a .txt file which can be read into R as a data.table object

## data storage
Github is designed for code--many NEPA documents are large. This respository is desiged to work using a combined approach pairing Github w/ Box Drive or a file storage mechanism (local or remote) of your choice. This can be done using symbolic links. /document and /text_as_datatable directories are for file storage, /code and /metadata directories live on github. In the command line, you can run:
  ln -s path/to/documents path/to/github_repository
 and this will make the /documents subdirectory show up as a symbolic link like: /path/to/github_repository/documents
 
 ## public access
 You can also access current versions of the document corpuses here (caveat emptor!)
  https://ucdavis.box.com/v/agency-nepa-repositories
  https://ucdavis.box.com/v/enepa-repository
