# MC users
# Jung Mee Park
# jmpark@arizona.edu
# 2022-29-08 updated with Campaign and Campaign Member


#### clear previous works`pace ####
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
    deparse(substitute(x)),"_", this_month, "_",this_year,".csv"))

####Import data from SF####
sf_auth()

#### bring in business units ####
solq_BU <-sprintf("SELECT 
                      et4ae5__Business_Unit_ID__c, CreatedDate, Id,
                      Display_Name__c
                    FROM et4ae5__Business_Unit__c")
business_units <- sf_query(solq_BU, object_name="et4ae5__Business_Unit__c", 
                           api_type="Bulk 1.0")

#### et4ae5__SendDefinition__c ####
solq_email <- sprintf("SELECT et4ae5__FromEmail__c, 
                        et4ae5__FromName__c, 
                        et4ae5__DateSent__c,
                        et4ae5__Business_Unit__c,
                       Id, OwnerId, Name
                       FROM et4ae5__SendDefinition__c")
email <- sf_query(solq_email, object_name="et4ae5__SendDefinition__c",
                  api_type="Bulk 1.0")

###bring in User object fields
soql_users <- sprintf("SELECT Id, Email, UserType, Name, 
                        NetID__c, Profile.Name, 
                        ContactId, CreatedDate, Department, 
                        ProfileId, UserRoleId, 
                        UserRole.Name, Title, Username FROM User
                        WHERE IsActive=TRUE")
users_SF <- sf_query(soql_users, object_name="User", api_type="Bulk 1.0")

#### Campaign ####
solq_campaign <- sprintf("SELECT LastModifiedById, Type, RecordTypeId,
                            NumberOfContacts, Business_Unit__c, 
                            ParentId, CreatedById, OwnerId
                       FROM Campaign")
campaign <- sf_query(solq_campaign, object_name="Campaign",
                  api_type="Bulk 1.0")


#### Campaign Memeber ####
solq_campaign_mem <- sprintf("SELECT CampaignId, ContactId, 
                            LastModifiedById, CreatedDate, CreatedById, Id,
                            Campaign_Member_Source__c
                             FROM CampaignMember")
campaign_mem <- sf_query(solq_campaign_mem, object_name="CampaignMember",
                     api_type="Bulk 1.0")


#### merge in business units and email ####
library(stringr)
library(stringi)
# colnames(business_units)[colnames(business_units) == "Id"] <- "et4ae5__Business_Unit__c"

# BU_join <- inner_join(business_units, email, by = c("Business_Unit__c"="et4ae5__Business_Unit__c"))
# inner join won't work in R but does in Tableau as an Id to et4ae5__BU


BU_join <- inner_join(email, business_units, by = c("et4ae5__Business_Unit__c"="Id"))

BU_join$from_NetID__c <- stri_match_first_regex(BU_join$et4ae5__FromEmail__c,
                                                "(.*?)\\@")[,2]

memory.limit(size=2000000)



# split the dataset for BU_join
BU_join_1a <- BU_join[1:500000, ]      
BU_join_1b <- BU_join[500001:949745, ]      

#### merge BU Join to campaign ####
BU_campaign_1a <- inner_join(campaign, BU_join_1a,
                      by = c("Business_Unit__c"="Business_Unit__c")) # filter outhalf of BU_join

BU_campaign_1b <- inner_join(campaign, BU_join_1b,
                             by = c("Business_Unit__c"="Business_Unit__c")) # filter outhalf of BU_join



# error message: Error: cannot allocate vector of size 1.8 Gb
# try to break it down ie filter
# break down number of rows on right 
# remove some rows that aren't necessary
# subset of both datasets 
# you don't need all the data 
# split the BU join in half

# check unique campaign members source
unique(campaign_mem$Campaign_Member_Source__c) #there are 8

# subset data based on the campaign member source
historical_mem <- campaign_mem[campaign_mem$Campaign_Member_Source__c == 
                                 "Historical",]
pc_list_mem <- campaign_mem[campaign_mem$Campaign_Member_Source__c == 
                              "Parent/Child - List",]

recipient_mem <- campaign_mem[campaign_mem$Campaign_Member_Source__c == 
                                "Recipient Request",]
full_service_mem <- campaign_mem[campaign_mem$Campaign_Member_Source__c == 
                                   "Contact Import - Full Service",]

manual_mem <- campaign_mem[campaign_mem$Campaign_Member_Source__c == 
                             "Manual",]
self_service_mem <- campaign_mem[campaign_mem$Campaign_Member_Source__c == 
                                   "Contact Import - Self Service",]

MCBU_mem <- campaign_mem[campaign_mem$Campaign_Member_Source__c == 
                           "Marketing Cloud Business Unit Relationship",]
pc_relationship_mem <- campaign_mem[campaign_mem$Campaign_Member_Source__c == 
                                      "Parent/Child - Relationship",]

#### merge campaign and campaign member ####
campaign_historical <- inner_join(campaign, historical_mem, 
                      by = c("CreatedById"="CreatedById"))
write_named_csv(campaign_historical)


campaign_pc_list <- inner_join(campaign, pc_list_mem, 
                                  by = c("CreatedById"="CreatedById"))
write_named_csv(campaign_pc_list)


campaign_manual <- inner_join(campaign, manual_mem, 
                               by = c("CreatedById"="CreatedById"))
write_named_csv(campaign_manual)


campaign_self_serv <- inner_join(campaign, self_service_mem, 
                               by = c("CreatedById"="CreatedById"))
write_named_csv(campaign_self_serv)


campaign_MCBU <- inner_join(campaign, MCBU_mem, 
                              by = c("CreatedById"="CreatedById"))
write_named_csv(campaign_MCBU)


campaign_pc_rel <- inner_join(campaign, pc_relationship_mem, 
                               by = c("CreatedById"="CreatedById"))
write_named_csv(campaign_pc_relationship)