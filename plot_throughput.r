library(ggplot2)
library(scales) 

# install.packages("magrittr") # only needed the first time you use it
# install.packages("dplyr")    # alternative installation of the %>%
library(magrittr) # need to run every time you start R and want to use %>%
library(dplyr)  
library(ggplot2)
data =read.csv("C:\\Gamal Elkoumy\\PhD\\OneDrive - Tartu Ülikool\\University of Tartu Courses\\Big Data\\CourseProject\\StreamCardinality\\Results\\throughput_result.csv")
data=na.omit(data)
#converting the time into the second number in the experiment
# data$out_time=(data$out_time-min(data$out_time))/1000/1000
approximate=c("LL","AC","HLL","FM","HLLP","BF")
# approximate=c("HLL","FM","HLLP","KMV","BF")
# exact=c("VEB" )

# exact_algorithms=subset(data ,algorithm=="RB" & data_distribution=="normal" & approach=="aggregate")
# exact_algorithms$out_time=(exact_algorithms$out_time-min(exact_algorithms$out_time))/1000/1000
# ggplot(data=exact_algorithms,aes(x=out_time,y=cdf))+geom_line()




approx_fig=data %>%
  group_by(approach,algorithm,data_distribution,tps) %>%
  filter(algorithm %in% approximate && tps %in% c("1000000"))%>%
  mutate(out_time, relative_out_time = (out_time-min(out_time))/1000/1000/1000)

ggplot(data=approx_fig,aes(x=relative_out_time,y=cdf,colour=algorithm))+
  geom_line(size=1)+
  ggtitle("Approximate Algorithms Throughput")+
  labs(x="Experiment Execution Time (Seconds) ", y="% CDF")+
  # scale_x_continuous(labels = scales::comma,trans='log10')+
  facet_grid( approach~data_distribution, switch = "y")+
  theme(strip.background = element_blank(),
        strip.placement = "outside",axis.text.x = element_text(angle = 90, hjust = 1))







check=data %>% 
  group_by(approach,algorithm,data_distribution,tps) %>%
  filter(algorithm %in% c("MRL") && approach %in%c("aggregate"))%>% 
  summarise( sum = sum(window_count))
# mutate(out_time, relative_out_time = (out_time-min(out_time))/1000/1000/1000)
