# MC users
# Jung Mee Park
# jmpark@arizona.edu
# 2022-15-08 updated with Business_Unit__c


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
                            EDS_Primary_Affiliation__c,
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

affiliations <- sf_query(my_soql_aff, object_name="hed__Affiliation__c", 
                         api_type="Bulk 1.0")

###bring in permissionsets Records
solq_perms<-sprintf("select AssigneeId, PermissionSet.Name, PermissionSet.Type, 
                    PermissionSet.ProfileId, 
                    PermissionSetGroupId from PermissionSetAssignment")
permissionsets <- sf_query(solq_perms, object_name="PermissionSetAssignment", api_type="Bulk 1.0")

#### bring in business units ####
# solq_BU <-sprintf("SELECT et4ae5__Business_Unit_ID__c,
#                       et4ae5__Business_Unit_ID2__c, 
#                       et4ae5__Business_Unit_Name__c
#                     FROM et4ae5__Business_Unit__c")
# business_units <- sf_query(solq_BU, object_name="et4ae5__Business_Unit__c", 
#                            api_type="Bulk 1.0")
solq_BU <-sprintf("SELECT Account__c,
                      Business_Unit__c, CreatedDate, Id,
                      Name
                    FROM Business_Unit_Account__c")
business_units <- sf_query(solq_BU, object_name="Business_Unit_Account__c", 
                           api_type="Bulk 1.0")
# #### business unit history ####
# solq_BU_hist <-sprintf("SELECT CreatedById, DataType, Field, CreatedDate, Id, ParentId
#                     FROM Business_Unit_Account__History")
# business_units_hist <- sf_query(solq_BU_hist, object_name="Business_Unit_Account__History", 
#                            api_type="Bulk 1.0")

#### perms2prods ####
perms2prods <- read_excel("20220224_Permissionsets_mapped_to_Products.xlsx")


#### Marketing Cloud data ####
MC_Logins <- read.csv("all_marketing_cloud.csv")

# solq_MC<-sprintf("select Campaign_Member_Field__c, Campaign_Record_Types__c
#                  FROM Marketing_Cloud_Mappings_mdt")
# MC_mapping <- sf_query(solq_MC, object_name="Marketing_Cloud_Mappings_mdt", api_type="Bulk 1.0")

#rename var indicating that login was for a particular tool
MC_Logins$Profile.Name<-"MC"

# colnames(MC_Logins)[1]<-"NetID__c"
library(stringr)
library(stringi)

MC_Logins$NetID__c <- stri_match_first_regex(MC_Logins$Email, "(.*?)\\@")[,2]

MC_Logins <- MC_Logins %>%
  relocate(NetID__c, .before = Email)

# drop some columns from MC
names(MC_Logins)

####STEP 3: Merge files####
# from Frances
names(users_SF)

users<-users_SF[,-c(5,8:12)] # remove Email,"ProfileId", "Title", "Username","UserRoleId","UserType" 
# users<-unique(users)
users<-distinct(users)

names(permissionsets)
names(perms2prods)
perms2prods<-perms2prods[,-c(2)] # drop 'n'
#### merge permission sets to users ####
users_perms<-merge(users, permissionsets, by.x = "Id", by.y = "AssigneeId")
users_perms_prods<-merge(users_perms, perms2prods, by.x = "PermissionSet.Name", by.y = "PermissionSet.Name", all.x = TRUE)
names(users_perms_prods)

upp_c<-merge(users_perms_prods, contact_records, by.x = "Id", by.y = "User__c", all.x = TRUE)
names(upp_c)

upp_c %>% count(Profile.Name)

upp_c <-distinct(upp_c)


#### create 'base' product from profile name ####
upp_c$Product[upp_c$UserRole.Name=="Data Analyst" & is.na(upp_c$Product)]<-"Reports"
upp_c$Product[(upp_c$UserRole.Name=="SAFER Volunteer" | upp_c$UserRole.Name=="SAFER") & is.na(upp_c$Product)]<-"SAFER"
upp_c$Product[upp_c$Profile.Name=="Advising Base" & is.na(upp_c$Product)]<-"Scheduling/Notes"
upp_c$Product[upp_c$Profile.Name=="Service Desk Base" & is.na(upp_c$Product)]<-"Service Desk"
upp_c$Product[upp_c$UserRole.Name=="Marketer" & is.na(upp_c$Product)]<-"Marketing - SF"

upp_c2<-unique(upp_c)

####MC_s_c_u creation####
# subset contact records
contact_records <- subset(contact_records, !is.na(contact_records$NetID__c)) # 337326

# merge MC contacts
MC_contacts <- merge(contact_records, MC_Logins, by.x = "NetID__c", 
                     by.y = "NetID__c", all = TRUE)

MC_s_c_u <- merge(MC_contacts, upp_c, by.x = c("NetID__c"),
                  by.y = c("NetID__c.x"), all=TRUE)
names(MC_s_c_u) # marketing contacts users

# product_longer <- MC_s_c_u %>% 
#   pivot_longer(cols = c(Profile.Name.x, Product),
#                names_to = "col_name", 
#                values_to = "MC-SFProduct") %>% 
#   drop_na("MC-SFProduct") %>% 
#   distinct()
# names(product_longer)
