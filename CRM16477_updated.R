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
                      CreatedDate, Id,
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
                            NumberOfContacts, Business_Unit__c, Id, Status,
                            ParentId, CreatedById, OwnerId
                       FROM Campaign")
campaign <- sf_query(solq_campaign, object_name="Campaign",
                  api_type="Bulk 1.0")


#### Campaign Member ####
solq_campaign_mem <- sprintf("SELECT CampaignId,
                            CreatedDate, CreatedById, Id,
                            Campaign_Member_Source__c
                             FROM CampaignMember") 
# try with date ContactId, LastModifiedById, CreatedDate, CreatedById, Id, 
# AND Created Date>=2022
campaign_mem <- sf_query(solq_campaign_mem, object_name="CampaignMember",
                     api_type="Bulk 1.0")

# #### add sendjunction ####
# solq_junction <- sprintf("SELECT et4ae5__Campaign__c, 
#                         et4ae5__SendDefinition__c
#                        FROM et4ae5__SendJunction__c")
# junction <- sf_query(solq_junction, object_name="et4ae5__SendJunction__c",
#                   api_type="Bulk 1.0") 

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
#### merge users####
# campaign_users <- inner_join(campaign_mem, users_SF,
#                              by = c("CreatedById"="Id"))
# campaign_users <- inner_join(agg_campaign, users_SF, 
#                              by = c("CreatedById.y"="Id"))

# # split the dataset for BU_join
# BU_join_1a <- BU_join[1:500000, ]      
# BU_join_1b <- BU_join[500001:949745, ]      

#### merge campaign to campaign mem ####
campaign_join <- inner_join(campaign, campaign_mem, 
                            by = c("Id" = "CampaignId"))
# rename Id to CampaignId

campaign_join <- rename(campaign_join, "CampaignId" = Id)

unique(campaign_join$Campaign_Member_Source__c)

# campaign join is too large so consider aggregation
campaign_subset <- campaign_join %>%
  select(Business_Unit__c, CreatedDate, Campaign_Member_Source__c, 
         CampaignId, 
         CreatedById.y, NumberOfContacts) %>% 
  distinct()

# campaign_subset[] <- lapply(campaign_subset, function(x) type.convert(as.character(x)))
# aggregate(. ~ Business_Unit__c, campaign_subset, sum)


agg_campaignId <- campaign_subset %>%
  group_by(Business_Unit__c, Campaign_Member_Source__c,
           CampaignId,
           CreatedById.y, NumberOfContacts) %>%
  count() %>%
  distinct() %>% 
  ungroup() # including number of contacts and Created date 
# gets too large with CreatedDate

write_named_csv(agg_campaignId)
# agg_campaign <- campaign_subset %>%
#   group_by(Business_Unit__c, Campaign_Member_Source__c,
#            CreatedById.y) %>%
#   count() %>%
#   ungroup()
  # summarise(BU_agg = count(Business_Unit__c),
  #           CMS_agg = count(Campaign_Member_Source__c))
# aggregate(freq~Business_Unit__c+Campaign_Member_Source__c,data=campaign_subset,sum)

#### merge campiang users to Users_SF
campaign_users <- inner_join(agg_campaignId, users_SF,
                             by = c("CreatedById.y"="Id"))

#### Merge aggregated campaigns to BU_join
# campaign_join_BU <- inner_join(agg_campaign, BU_join,
#                         by = c("Business_Unit__c"="et4ae5__Business_Unit__c"))
# write_named_csv(campaign_join_BU)

# campaign_join_BU <- campaign_join_BU %>% 
#   distinct()

users_campaign_join_BU <- inner_join(campaign_users, BU_join,
                               by = c("Business_Unit__c"="et4ae5__Business_Unit__c"))
write_named_csv(users_campaign_join_BU) # with Users


#### clean netid ####
users_campaign_join_BU$from_User <- stri_match_first_regex(users_campaign_join_BU$Username,
                                                "(.*?)\\@")[,2]
users_campaign_BU <- users_campaign_join_BU %>%
  dplyr::mutate(NetID = case_when(!is.na(from_NetID__c) ~ from_NetID__c,
                                  TRUE ~ from_User))
write_named_csv(users_campaign_BU)
#### subset data with useful variables ####
# users_campaign_BU2 <- users_campaign_BU %>% 
#   select(Business_Unit__c, Campaign_Member_Source__c, CreatedById.y,
#          n, NetID, Display_Name__c,NumberOfContacts, CampaignId,
#          et4ae5__DateSent__c, et4ae5__FromEmail__c, et4ae5__FromName__c, 
#          from_NetID__c, Profile.Name, UserRole.Name, from_User) %>% 
#   distinct()  # without the CreatedDate 
# 
# write_named_csv(users_campaign_BU2)

users_campaign_BU3 <- users_campaign_BU %>% 
  select(Business_Unit__c, Campaign_Member_Source__c, CreatedById.y,
         n, NetID, Display_Name__c,NumberOfContacts, CampaignId,
         et4ae5__DateSent__c, et4ae5__FromEmail__c, et4ae5__FromName__c, 
         from_NetID__c, Profile.Name, UserRole.Name, from_User) %>% 
  distinct()  # without the CreatedDate 

#### remove Test and Trellis ####
users_campaign_BU4 <- subset(users_campaign_BU3, 
                             !(Display_Name__C %in% c("Test", "Trellis")))

write_named_csv(users_campaign_BU4)

#### subset users to see percentages ####
users_campaign_BU3 %>% 
  group_by(Campaign_Member_Source__c) %>% 
  # summarise(count = n() ) %>%
  # mutate(prop = count / sum(count))
  summarise(n = n()) %>%
  mutate(freq = paste0(round(n / sum(n) * 100, 1), "%"))

#### count the NA ####
colSums(is.na(users_campaign_BU3))

# users_campaign_BU <- inner_join(campaign_join_BU, users_SF,
#                                 by = c("CreatedById.y" = "Id"))
#campaign_subset <- campaign_join[campaign_join$Campaign_Member_Source__c %in% c('Manual','Contact Import - Full Service','Contact Import - Self Service'),]

#### merge BU Join to campaign ####
# BU_campaign_1a <- inner_join(campaign, BU_join_1a,
#                       by = c("Business_Unit__c"="Business_Unit__c")) # filter outhalf of BU_join
# 
# BU_campaign_1b <- inner_join(campaign, BU_join_1b,
#                              by = c("Business_Unit__c"="Business_Unit__c")) # filter outhalf of BU_join



# error message: Error: cannot allocate vector of size 1.8 Gb
# try to break it down ie filter
# break down number of rows on right 
# remove some rows that aren't necessary
# subset of both datasets 
# you don't need all the data 
# split the BU join in half

# check unique campaign members source
# unique(campaign_mem$Campaign_Member_Source__c) #there are 8

# subset data based on the campaign member source
# historical_mem <- campaign_mem[campaign_mem$Campaign_Member_Source__c == 
#                                  "Historical",]
# pc_list_mem <- campaign_mem[campaign_mem$Campaign_Member_Source__c == 
#                               "Parent/Child - List",]
# 
# recipient_mem <- campaign_mem[campaign_mem$Campaign_Member_Source__c == 
#                                 "Recipient Request",]
# full_service_mem <- campaign_mem[campaign_mem$Campaign_Member_Source__c == 
#                                    "Contact Import - Full Service",]
# 
# manual_mem <- campaign_mem[campaign_mem$Campaign_Member_Source__c == 
#                              "Manual",]
# self_service_mem <- campaign_mem[campaign_mem$Campaign_Member_Source__c == 
#                                    "Contact Import - Self Service",]
# 
# MCBU_mem <- campaign_mem[campaign_mem$Campaign_Member_Source__c == 
#                            "Marketing Cloud Business Unit Relationship",]
# pc_relationship_mem <- campaign_mem[campaign_mem$Campaign_Member_Source__c == 
#                                       "Parent/Child - Relationship",]
# historical_mem <- campaign_mem[campaign_mem$Campaign_Member_Source__c == 
#                                  "Historical",]
# pc_list_mem <- campaign_mem[campaign_mem$Campaign_Member_Source__c == 
#                               "Parent/Child - List",]
# 
# recipient_mem <- campaign_mem[campaign_mem$Campaign_Member_Source__c == 
#                                 "Recipient Request",]
# full_service_mem <- campaign_mem[campaign_mem$Campaign_Member_Source__c == 
#                                    "Contact Import - Full Service",]
# 
# manual_join <- campaign_join[campaign_join$Campaign_Member_Source__c == 
#                              "Manual",]
# self_service_join <- campaign_join[campaign_join$Campaign_Member_Source__c == 
#                                    "Contact Import - Self Service",]
# # 
# # MCBU_join <- campaign_join[campaign_join$Campaign_Member_Source__c == 
# #                            "Marketing Cloud Business Unit Relationship",]
# # pc_relationship_join <- campaign_join[campaign_join$Campaign_Member_Source__c == 
# #                                       "Parent/Child - Relationship",]
# 
# full_service_join <- campaign_join[campaign_join$Campaign_Member_Source__c ==
#                                    "Contact Import - Full Service",]
# # OR 
# full_service_join <- subset(campaign_join, Campaign_Member_Source__c ==
#                               "Contact Import - Full Service")

# #### merge campaign and campaign member ####
# BU_campaign <- inner_join(campaign_subset, BU_join, 
#                           by = c("Business_Unit__c"="et4ae5__Business_Unit__c"))

# #### merge users to campaign Mem ####
# BU_manual <- inner_join(manual_join, BU_join, 
#                           by = c("Business_Unit__c"="et4ae5__Business_Unit__c"))
# BU_manual <- Bu_manual %>% 
#   distinct()
# 
# write_named_csv(BU_manual)
# 
# 
# BU_full_service <- inner_join(full_service_join, BU_join, 
#                               by = c("Business_Unit__c"="et4ae5__Business_Unit__c"))
# BU_self_service <- inner_join(self_service_join, BU_join, 
#                               by = c("Business_Unit__c"="et4ae5__Business_Unit__c"))
# 
# #### aggregate full and self service data
# full_service_join$Business_Unit__c <- as.factor(full_service_join$Business_Unit__c)
# 
# library(dplyr) # to try
# full_service_join %>%
#   group_by(Business_Unit__c) %>%
#   summarise(occurrences = n()) %>% 
#   ungroup()
# 
# # head(aggregate(full_service_join$Business_Unit__c,
# #                list(time = full_service_join$CreatedDate),
# #                sum))
# 
# # full_service_join %>% 
# #   group_by(Business_Unit__c) %>% 
# #   summarize_each(funs(sum)) %>% 
# #   ungroup()
# # 
# # full_service_join %>% 
# #   group_by(Business_Unit__c) %>% 
# #   summarise (agg_bu = sum(Business_Unit__c))
# # library(tidyr)
# # agg_BU = aggregate(full_service_join[,5],
# #                    by=list(full_service_join$Business_Unit__c),
# #                    FUN=sum, na.rm=TRUE)
# # agg_BU
# # 
# # library(reshape2)
# # melted_data <- melt(full_service_join, id.vars = "Business_Unit__c")
# # dcast(melted_data, Business_Unit__c ~ variable, sum)
# 
# ####
# # campaign_historical <- inner_join(campaign, historical_mem, 
# #                       by = c("Id"="CampaignId"))
# # write_named_csv(campaign_historical)
# # 
# # 
# # campaign_pc_list <- inner_join(campaign, pc_list_mem, 
# #                                   by = c("CreatedById"="CreatedById"))
# # write_named_csv(campaign_pc_list)
# # 
# # 
# # campaign_manual <- inner_join(campaign, manual_mem, 
# #                                by = c("CreatedById"="CreatedById"))
# # write_named_csv(campaign_manual)
# # 
# # 
# # campaign_self_serv <- inner_join(campaign, self_service_mem, 
# #                                by = c("CreatedById"="CreatedById"))
# # write_named_csv(campaign_self_serv)
# # 
# # 
# # campaign_MCBU <- inner_join(campaign, MCBU_mem, 
# #                               by = c("CreatedById"="CreatedById"))
# # write_named_csv(campaign_MCBU)
# # 
# # 
# # campaign_pc_rel <- inner_join(campaign, pc_relationship_mem, 
# #                                by = c("CreatedById"="CreatedById"))
# # write_named_csv(campaign_pc_relationship)