#!/usr/bin/env Rscript
library(ggplot2)
library(scales)
library(dplyr)
theme_set(theme_bw())
options("scipen"=10)
args <- commandArgs(TRUE)
tps <- as.numeric(args[2])
duration <- as.numeric(args[3])
tps_count <- as.numeric(args[4])
engines_all <- c("flink","spark_dataset","spark_dstream", "kafka", "jet", "storm")
engines <- c("flink","spark_dataset","spark_dstream", "kafka", "jet")
storms <- c("storm","storm_no_ack")
source('/root/stream-benchmarking/reporting/StreamServerReport.r')
source('/root/stream-benchmarking/reporting/KafkaServerReport.r')
source('/root/stream-benchmarking/reporting/BenchmarkResult.r')
source('/root/stream-benchmarking/reporting/BenchmarkPercentile.R')
source('/root/stream-benchmarking/reporting/ResourceConsumptionReport.r')
trim <- function (x) gsub("^\\s+|\\s+$", "", x)
########### added by Gamal ###########
multiplot <- function(..., plotlist=NULL, file, cols=1, layout=NULL) {
  require(grid)

  # Make a list from the ... arguments and plotlist
  plots <- c(list(...), plotlist)

  numPlots = length(plots)

  # If layout is NULL, then use 'cols' to determine layout
  if (is.null(layout)) {
    # Make the panel
    # ncol: Number of columns of plots
    # nrow: Number of rows needed, calculated from # of cols
    layout <- matrix(seq(1, cols * ceiling(numPlots/cols)),
                    ncol = cols, nrow = ceiling(numPlots/cols))
  }

 if (numPlots==1) {
    print(plots[[1]])

  } else {
    # Set up the page
    grid.newpage()
    pushViewport(viewport(layout = grid.layout(nrow(layout), ncol(layout))))

    # Make each plot, in the correct location
    for (i in 1:numPlots) {
      # Get the i,j matrix positions of the regions that contain this subplot
      matchidx <- as.data.frame(which(layout == i, arr.ind = TRUE))

      print(plots[[i]], vp = viewport(layout.pos.row = matchidx$row,
                                      layout.pos.col = matchidx$col))
    }
  }
}




generateBenchmarkReport(args[1], tps, duration, tps_count)
generateStreamServerLoadReport(args[1], tps, duration, tps_count)
generateKafkaServerLoadReport(args[1], tps, duration, tps_count)
generateBenchmarkPercentile(args[1], tps, duration, tps_count)


tps_count = 15
#generateBenchmarkPercentile("kafka", 1000, 600, 15)

if(length(args) == 0){
  for (i in 1:length(engines_all)) { 
    generateBenchmarkReport(engines_all[i], 1000, 600, tps_count)
    generateStreamServerLoadReport(engines_all[i], 1000, 600, tps_count)
    generateKafkaServerLoadReport(engines_all[i], 1000, 600, tps_count)
    generateBenchmarkPercentile(engines_all[i], 1000, 600, tps_count)
    generateResourceConsumptionReportByTps(engines_all[i], 1000, 600, tps_count)
  }
  generateResourceConsumptionReport(engines_all, 1000, 600, tps_count)
  generateBenchmarkSpesificPercentile(engines, 1000, 600, 99, tps_count)
  generateBenchmarkSpesificPercentile(engines, 1000, 600, 95, tps_count)
  generateBenchmarkSpesificPercentile(engines, 1000, 600, 90, tps_count)
  
}
  
generateResourceConsumptionReport(engines_all, 1000, 600, 15)


