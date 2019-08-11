







## Area diagram carving up active listings by type:
# - Non-housing
# - Casual non-ML
# - FREH/GH/ML

library(treemapify)

Florida_property_LTM %>% 
  st_drop_geometry() %>% 
  filter(!is.na(listing_type)) %>% 
  group_by(listing_type, housing) %>% 
  summarize(n = n(),
            revenue = sum(as.numeric(revenue), na.rm = TRUE)) %>% 
  ggplot(aes(area = n, subgroup = listing_type, subgroup2 = housing, 
             fill = listing_type)) +
  geom_treemap() +
  #geom_treemap_subgroup2_text() +
  geom_treemap_subgroup_text() +
  geom_treemap_subgroup2_border() +
  geom_treemap_subgroup_border()



## Then repeat that by revenue






library(viridis)

Florida_property %>% 
  filter(created <= "2019-04-30", scraped >= "2019-04-30") %>% 
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%
  st_transform(4269) %>% 
  st_join(st_transform(Florida_CTs, 4269)) %>% 
  st_drop_geometry() %>% 
  group_by(GEOID) %>% 
  summarize(listings = n()) %>% 
  left_join(st_transform(select(Florida_CTs, GEOID, estimate), 4269)) %>%
  filter(listings > 4) %>% 
  st_as_sf() %>% 
  ggplot() +
  geom_sf(data = Florida_regions, fill = "grey60", lwd = 0) +
  geom_sf(aes(fill = listings / estimate), lwd = 0) +
  scale_fill_viridis(limits = c(0, 0.05), oob = scales::squish)


Florida_property %>% 
  filter(created <= "2019-04-30", scraped >= "2019-04-30") %>% 
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%
  st_transform(4269) %>% 
  st_join(st_transform(Florida_BGs, 4269)) %>% 
  st_drop_geometry() %>% 
  group_by(GEOID) %>% 
  summarize(listings = n()) %>% 
  left_join(st_transform(select(Florida_BGs, GEOID, estimate), 4269)) %>%
  filter(listings > 4) %>% 
  st_as_sf() %>% 
  ggplot() +
  geom_sf(data = Florida_regions, fill = "grey50", lwd = 0) +
  geom_sf(aes(fill = listings / estimate), lwd = 0) +
  scale_fill_viridis(limits = c(0, 0.05), oob = scales::squish)
