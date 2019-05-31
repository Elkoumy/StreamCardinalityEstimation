library(ggplot2)
library(scales) 
library(magrittr) # need to run every time you start R and want to use %>%
library(dplyr)  
library(ggplot2)

query_time =read.csv("C:\\Gamal Elkoumy\\PhD\\OneDrive - Tartu Ülikool\\University of Tartu Courses\\Big Data\\CourseProject\\StreamCardinality\\Results\\query_time_result.csv")
insertion_time =read.csv("C:\\Gamal Elkoumy\\PhD\\OneDrive - Tartu Ülikool\\University of Tartu Courses\\Big Data\\CourseProject\\StreamCardinality\\Results\\insertion_time_result3_edited.csv")

query_time=na.omit(query_time)
insertion_time=na.omit(insertion_time)

# approximate=c("CKMS","Frugal","GK","MPQ","QD","SQ","SumQ","TD","CM")
# exact=c("DH", "RB","SL" , "VEB" )

approximate=c("LL","AC","HLL","LC","FM","HLLP","KMV","BF")




query_aggregate_approximate=query_time %>% 
  filter(algorithm %in% approximate)%>% 
  group_by(approach,algorithm,data_distribution,tps) %>%
  summarise( query_time_mean = mean(query_time_mean), query_time_median = median(query_time_median))

ggplot(query_aggregate_approximate, aes(x=algorithm , y=query_time_mean, fill=approach)) +
  geom_bar(stat="identity", position = "dodge")+
  scale_y_continuous(labels = scales::comma,trans='log10')+
  ggtitle("Approximate Algorithms Query Time")+
  labs(x="Algorithm", y="Mean of Query Time log(n sec)")+
  facet_grid(data_distribution~ tps, switch = "y")+
  theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        panel.background = element_blank(),strip.background = element_blank(),
        strip.placement = "outside",axis.text.x = element_text(angle = 90, hjust = 1))
ggsave("C:\\Gamal Elkoumy\\PhD\\OneDrive - Tartu Ülikool\\University of Tartu Courses\\Big Data\\CourseProject\\StreamCardinality\\Results\\approximate_queryTime_mean.png")

ggplot(query_aggregate_approximate, aes(x=algorithm , y=query_time_median, fill=approach)) +
  geom_bar(stat="identity", position = "dodge")+
  scale_y_continuous(labels = scales::comma,trans='log10')+
  ggtitle("Approximate Algorithms Query Time")+
  labs(x="Algorithm", y="Median of Query Time log(n sec)")+
  facet_grid(data_distribution~ tps, switch = "y")+
  theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        panel.background = element_blank(),strip.background = element_blank(),
        strip.placement = "outside",axis.text.x = element_text(angle = 90, hjust = 1))
ggsave("C:\\Gamal Elkoumy\\PhD\\OneDrive - Tartu Ülikool\\University of Tartu Courses\\Big Data\\CourseProject\\StreamCardinality\\Results\\approximate_queryTime_median.png")

#######################################################################
########## insertion time
###########################################################################

insertion_aggregate_approximate=insertion_time %>% 
  filter(algorithm %in% approximate)%>% 
  group_by(approach,algorithm,data_distribution,tps) %>%
  summarise( insertion_time_mean = mean(insertion_time_mean), insertion_time_median = median(insertion_time_median))

ggplot(insertion_aggregate_approximate, aes(x=algorithm , y=insertion_time_mean, fill=approach)) +
  geom_bar(stat="identity", position = "dodge")+
  # scale_y_continuous(labels = scales::comma,trans='log10')+
  ggtitle("Approximate Algorithms insertion Time")+
  labs(x="Algorithm", y="Mean of insertion Time (n sec)")+
  facet_grid(data_distribution~ tps, switch = "y")+
  theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        panel.background = element_blank(),strip.background = element_blank(),
        strip.placement = "outside",axis.text.x = element_text(angle = 90, hjust = 1))
ggsave("C:\\Gamal Elkoumy\\PhD\\OneDrive - Tartu Ülikool\\University of Tartu Courses\\Big Data\\CourseProject\\StreamCardinality\\Results\\approximate_insertionTime_mean.png")

ggplot(insertion_aggregate_approximate, aes(x=algorithm , y=insertion_time_median, fill=approach)) +
  geom_bar(stat="identity", position = "dodge")+
  # scale_y_continuous(labels = scales::comma,trans='log10')+
  ggtitle("Approximate Algorithms insertion Time")+
  labs(x="Algorithm", y="Median of insertion Time (n sec)")+
  facet_grid(data_distribution~ tps, switch = "y")+
  theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        panel.background = element_blank(),strip.background = element_blank(),
        strip.placement = "outside",axis.text.x = element_text(angle = 90, hjust = 1))
ggsave("C:\\Gamal Elkoumy\\PhD\\OneDrive - Tartu Ülikool\\University of Tartu Courses\\Big Data\\CourseProject\\StreamCardinality\\Results\\approximate_insertionTime_median.png")







# 
# 
# 
# 
# 
# 
# query_aggregate_exact=query_time %>% 
#   filter(algorithm %in% exact & approach=="scotty")%>% 
#   group_by(approach,algorithm,data_distribution,tps) %>%
#   summarise( query_time_mean = mean(query_time_mean), query_time_median = median(query_time_median))
# 
# ggplot(query_aggregate_exact, aes(x=algorithm , y=query_time_mean, fill=approach)) +
#   geom_bar(stat="identity", position = "dodge")+
#   scale_y_continuous(labels = scales::comma,trans='log10')+
#   ggtitle("Exact Algorithms Query Time")+
#   labs(x="Algorithm", y="Mean of Query Time (n sec)")+
#   facet_grid(data_distribution~ tps, switch = "y")+
#   theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
#         panel.background = element_blank(),strip.background = element_blank(),
#         strip.placement = "outside")
# 
# 
# 
# ggplot(query_aggregate_exact, aes(x=algorithm , y=query_time_median, fill=approach)) +
#   geom_bar(stat="identity", position = "dodge")+
#   scale_y_continuous(labels = scales::comma,trans='log10')+
#   ggtitle("Exact Algorithms Query Time")+
#   labs(x="Algorithm", y="Median of Query Time (n sec)")+
#   facet_grid(data_distribution~ tps, switch = "y")+
#   theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
#         panel.background = element_blank(),strip.background = element_blank(),
#         strip.placement = "outside")
# 
# 
# 
# insertion_aggregate_exact=insertion_time %>% 
#   filter(algorithm %in% exact & approach=="scotty")%>% 
#   group_by(approach,algorithm,data_distribution,tps) %>%
#   summarise( insertion_time_mean = mean(insertion_time_mean), insertion_time_median = median(insertion_time_median))
# 
# ggplot(insertion_aggregate_exact, aes(x=algorithm , y=insertion_time_mean, fill=approach)) +
#   geom_bar(stat="identity", position = "dodge")+
#   scale_y_continuous(labels = scales::comma,trans='log10')+
#   ggtitle("Exact Algorithms insertion Time")+
#   labs(x="Algorithm", y="Mean of insertion Time (n sec)")+
#   facet_grid(data_distribution~ tps, switch = "y")+
#   theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
#         panel.background = element_blank(),strip.background = element_blank(),
#         strip.placement = "outside")
# 
# 
# 
# ggplot(insertion_aggregate_exact, aes(x=algorithm , y=insertion_time_median, fill=approach)) +
#   geom_bar(stat="identity", position = "dodge")+
#   scale_y_continuous(labels = scales::comma,trans='log10')+
#   ggtitle("Exact Algorithms insertion Time")+
#   labs(x="Algorithm", y="Median of insertion Time (n sec)")+
#   facet_grid(data_distribution~ tps, switch = "y")+
#   theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
#         panel.background = element_blank(),strip.background = element_blank(),
#         strip.placement = "outside")