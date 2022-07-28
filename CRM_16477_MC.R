# MC users
# Jung Mee Park
# jmpark@arizona.edu
# 2022-28-07

#### clear previous workspace ####
rm(list=ls(all=TRUE)) 
options(digits=3)

####Load Libraries####
library('tidyverse')
library('dplyr')
library(dbplyr)
library('salesforcer')
library('lubridate')
library(reshape2)
library(readxl)
library(Rserve)

####check working directory####
getwd() #figure out the working directory
setwd("~/CRM-16477")
# setwd("D:/Users/jmpark/Box/Trellis/Program Team/Trellis Metrics Reports/Trellis KPIs/Raw data")
# setwd("~/monthly_report_SF_trellis_users")

####capture current month####
today <- Sys.Date()
this_month <- month(today)
last_month<-month(today() - months(1))
this_year <- year(today)

#### function to write copy files into csv####
write_named_csv <- function(x) 
  write_csv(x, file = paste0(
    # "D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/Apr 2022 data files/",
    # "D:/Users/jmpark/Box/My Box Notes/June 2022 data/",
    deparse(substitute(x)),"_", last_month, "_",this_year,".csv"))

####Import data from SF####
sf_auth()

###bring in Contact Records
my_soql_contact <- sprintf("SELECT Id,	Emplid__c, Email, User__c, 
                            Primary_Department__r.Name, NetId__c, 
                            hed__Primary_Organization__c, MDM_Primary_Type__c
                            FROM Contact")

contact_records <- sf_query(my_soql_contact, object_name="Contact", api_type="Bulk 1.0")

###bring in User object fields
soql_users <- sprintf("SELECT Id, Email, UserType, Name, NetID__c, Profile.Name, 
                    ContactId, CreatedDate, Department, ProfileId, UserRoleId, 
                    UserRole.Name, Title, Username FROM User
                    WHERE IsActive=TRUE ")
users_SF <- sf_query(soql_users, object_name="User", api_type="Bulk 1.0")

###bring in affiliation Records
my_soql_aff <- sprintf("SELECT Academic_Department__c, hed__Affiliation_Type__c, 
                        hed__Contact__c, hed__Primary__c, hed__Account__r.Name, 
                        Parent_Organization__c FROM hed__Affiliation__c")

affiliations <- sf_query(my_soql_aff, object_name="hed__Affiliation__c", api_type="Bulk 1.0")
