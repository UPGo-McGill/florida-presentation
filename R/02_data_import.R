#### DATA IMPORT: RUN ONLY ONCE ################################################

source("R/01_helper_functions.R")

### Data retrieval #############################################################

## Open connection to remote tables

library(RPostgres)

con <- RPostgres::dbConnect(
  RPostgres::Postgres(),
  dbname = "airdna")

property <- tbl(con, "property")
daily <- tbl(con, "daily")


## Retrieve Florida tables

Florida_property <- 
  property %>% 
  filter(region == "Florida") %>% 
  collect()

Florida_daily <- 
  daily %>% 
  filter(property_ID %in% !! Florida_property$property_ID) %>% 
  collect()

Florida_daily <- 
  strr_expand_daily(Florida_daily)


## Retrieve ML tables

ML_property <- 
  property %>% 
  filter((!is.na(ab_host) & ab_host %in% !! Florida_property$ab_host)) %>% 
  collect()

ML_property2 <- 
  property %>% 
  filter((!is.na(ha_host) & ha_host %in% !! Florida_property$ha_host)) %>% 
  collect()

ML_property <- 
  ML_property %>% 
  rbind(ML_property2) %>% 
  filter(!(property_ID %in% Florida_property$property_ID)) %>% 
  distinct(property_ID, .keep_all = TRUE)

ML_daily <- 
  daily %>% 
  filter(property_ID %in% !! ML_property$property_ID) %>% 
  collect()

ML_daily <- 
  strr_expand_daily(ML_daily)


## Save raw files

save(Florida_property, file = "data/Florida_property.Rdata")
save(Florida_daily, file = "data/Florida_daily.Rdata")
save(ML_property, file = "data/ML_property.Rdata")
save(ML_daily, file = "data/ML_daily.Rdata")


### Process property and daily files ###########################################

## Process property file

Florida_property <- 
  Florida_property %>% 
  select(property_ID:scraped, latitude:longitude, city:metropolitan_area,
         ab_property:ha_host) %>% 
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%
  st_transform(32617) %>% 
  mutate(housing = if_else(property_type %in% c(
    "House", "Private room in house", "Apartment", "Cabin",
    "Entire condominium", "Townhouse", "Condominium", "Entire apartment",
    "Private room", "Loft", "Place", "Entire house", "Villa", "Guesthouse",
    "Private room in apartment", "Guest suite", "Shared room in dorm",
    "Chalet", "Dorm", "Entire chalet", "Shared room in loft", "Cottage",
    "Resort", "Serviced apartment", "Other", "Bungalow", "Farm stay",
    "Private room in villa", "Entire loft", "Entire villa",
    "Private room in guesthouse", "Island", "Entire cabin", "Vacation home",
    "Entire bungalow", "Earth house", "Nature lodge", "In-law",
    "Entire guest suite", "Shared room in apartment", "Private room in loft",
    "Tiny house", "Castle", "Earth House", "Private room in condominium",
    "Entire place", "Shared room", "Hut", "Private room in guest suite",
    "Private room in townhouse", "Timeshare", "Entire townhouse",
    "Shared room in house", "Entire guesthouse", "Shared room in condominium",
    "Cave", "Private room in cabin", "Dome house",
    "Private room in vacation home", "Private room in dorm",
    "Entire serviced apartment", "Private room in bungalow",
    "Private room in serviced apartment", "Entire Floor", "Entire earth house",
    "Entire castle", "Shared room in chalet", "Shared room in bungalow",
    "Shared room in townhouse", "Entire cottage", "Private room in castle",
    "Private room in chalet", "Private room in nature lodge", "Entire in-law",
    "Shared room in guesthouse", "Casa particular", "Serviced flat", "Minsu",
    "Entire timeshare", "Shared room in timeshare", "Entire vacation home",
    "Entire nature lodge", "Entire island", "Private room in in-law",
    "Shared room in serviced apartment", "Shared room in cabin", "Entire dorm",
    "Entire cave", "Private room in timeshare", "Shared room in guest suite",
    "Private room in cave", "Entire tiny house",
    "Private room in casa particular (cuba)", "Casa particular (cuba)",
    "Private room in cottage", "Private room in tiny house",
    "Entire casa particular", ""), TRUE, FALSE)) %>% 
  mutate(host_ID = if_else(!is.na(ab_host), 
                           as.character(ab_host), ha_host)) %>% 
  arrange(property_ID)

Florida_property <- 
  Florida_daily %>%
  filter(status != "U") %>% 
  group_by(property_ID) %>% 
  summarize(created2 = min(date)) %>% 
  left_join(Florida_property, .) %>% 
  mutate(created = if_else(is.na(created), created2, created)) %>% 
  select(-created2)


## Process daily file

Florida_daily <- 
  Florida_property %>% 
  st_drop_geometry() %>% 
  select(property_ID, host_ID, listing_type, created, scraped, housing) %>% 
  left_join(Florida_daily, .)

Florida_daily <- 
  Florida_daily %>% 
  filter(date >= created, date <= scraped + 30, status != "U")


## Create LTM property file

Florida_property_LTM <- 
  Florida_property %>% 
  filter(created <= "2019-04-30", scraped >= "2018-05-01")

Florida_property_LTM <- 
  Florida_daily %>% 
  filter(status == "R", date >= "2018-05-01", date <= "2019-04-30") %>% 
  group_by(property_ID) %>% 
  summarize(revenue = sum(price)) %>% 
  left_join(Florida_property_LTM, .)


## Multilistings

Florida_daily_ML <- 
  Florida_daily %>%
  filter(listing_type != "Shared room") %>% 
  group_by(listing_type, host_ID, date) %>%
  count() %>% 
  ungroup() %>%
  filter(n >= 2) %>% 
  filter((listing_type == "Entire home/apt" & n >= 2 |
            (listing_type == "Private room" & n >= 3))) %>% 
  mutate(ML = TRUE) %>% 
  select(-count)

Florida_daily <-
  left_join(Florida_daily, Florida_daily_ML) %>%
  mutate(ML = if_else(is.na(ML), FALSE, ML))


## Calculate FREH and GH listings

FREH <- 
  Florida_daily %>% 
  strr_FREH("2015-09-30", "2019-04-30", cores = 7) %>% as_tibble() %>% 
  filter(FREH == TRUE) %>% 
  select(-FREH)

GH <- 
  Florida_property %>% 
  filter(Housing == TRUE) %>% 
  strr_ghost(property_ID, host_ID, created, scraped, "2014-10-01", "2019-04-30",
             listing_type, cores = 7)


## Save processed files

save(Florida_property, file = "data/Florida_property.Rdata")
save(Florida_property_LTM, file = "data/Florida_property_LTM.Rdata")
save(Florida_daily, file = "data/Florida_daily.Rdata")




### Get geographies and ACS data ###############################################

## Florida counties and regions

Florida_counties <- 
  get_acs(geography = "county", variables = "B25001_001", state = "FL", 
          geometry = TRUE) %>% 
  as_tibble() %>% 
  st_as_sf() %>% 
  st_transform(32617) %>% 
  rename(housing_units = estimate) %>% 
  select(-variable, -moe)

Florida_regions <- 
  Florida_counties %>% 
  mutate(region = case_when(
    GEOID %in% c(12033, 12113, 12091, 12131, 12059, 12133, 12005, 12013, 12063,
                 12045, 12077, 12037) ~ "Northwest",
    GEOID %in% c(12039, 12073, 12129, 12065, 12079, 12123, 12047, 12023, 12121,
                 12067, 12125, 12007, 12001, 12041, 12029, 12075, 12083, 12017
    ) ~ "North central",
    GEOID %in% c(12003, 12031, 12089, 12019, 12109, 12107) ~ "Northeast",
    GEOID %in% c(12035, 12127, 12119, 12069, 12117, 12095, 12097, 12009
    ) ~ "East central",
    GEOID %in% c(12053, 12101, 12103, 12057, 12105, 12081, 12049, 12115, 12055,
                 12027) ~ "West central",
    GEOID %in% c(12061, 12093, 12111, 12099, 12085) ~ "Southeast",
    GEOID %in% c(12043, 12015, 12021, 12051, 12071) ~ "Southwest",
    TRUE ~ "South"
  )) %>% 
  group_by(region) %>% 
  summarize(housing_units = sum(housing_units))


## CTs and BGs

Florida_CTs <- 
  get_acs(geography = "tract", variables = "B25001_001", state = "FL",
          geometry = TRUE) %>% 
  as_tibble() %>% 
  st_as_sf() %>% 
  st_transform(32617) %>% 
  rename(housing_units = estimate) %>% 
  select(-variable, -moe)

Florida_BGs <- 
  get_acs(geography = "block group", variables = "B25001_001", state = "FL",
          geometry = TRUE) %>% 
  as_tibble() %>% 
  st_as_sf() %>% 
  st_transform(32617) %>% 
  rename(housing_units = estimate) %>% 
  select(-variable, -moe)




Florida_daily %>% 
  group_by(date, metropolitan_area) %>% 
  summarize(listings = n()) %>% 
  mutate(msa = case_when(
    metropolitan_area == "Miami-Fort Lauderdale-West Palm Beach, FL Metro Area" ~ "Miami",
    metropolitan_area == "Orlando-Kissimmee-Sanford, FL Metro Area" ~ "Orlando",
    metropolitan_area == "Tampa-St. Petersburg-Clearwater, FL Metro Area" ~ "Tampa",
    metropolitan_area == "Lakeland-Winter Haven, FL Metro Area" ~ "Lakeland",
    metropolitan_area == "Crestview-Fort Walton Beach-Destin, FL Metro Area" ~ "Crestview",
    metropolitan_area == "North Port-Sarasota-Bradenton, FL Metro Area" ~ "North Port",
    metropolitan_area == "Cape Coral-Fort Myers, FL Metro Area" ~ "Cape Coral",
    metropolitan_area == "Panama City, FL Metro Area" ~ "Panama City",
    metropolitan_area == "Jacksonville, FL Metro Area" ~ "Jacksonville",
    metropolitan_area == "Deltona-Daytona Beach-Ormond Beach, FL Metro Area" ~ "Deltona",
    TRUE ~ "Other MSA"
  )) %>% 
  ggplot() +
  geom_line(aes(date, listings, colour = metropolitan_area), size = 2)
  
