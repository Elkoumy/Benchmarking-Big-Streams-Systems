######################################################################################################################################
##########################                        Benchmark Kafka Server Load                               ##########################
######################################################################################################################################

generateKafkaServerLoadReport <- function(engine, tps, duration, tps_count){
  for(i in 1:tps_count) {
    TPS = toString(tps*i)
    memoryUsage= NULL
    cpuUsage= NULL
    sourceFolder = paste("/root/stream-benchmarking/result/", engine, "/TPS_", TPS,"_DURATION_",toString(duration),"/", sep = "")
    for(x in 1:3) {
      kafkaCpu = read.table(paste(sourceFolder, "kafka-node-0", x,".cpu",sep=""),header=F,stringsAsFactors=F,sep=',')
      kafkaMem = read.table(paste(sourceFolder, "kafka-node-0", x,".mem",sep=""),header=F,stringsAsFactors=F,sep=',')
      
      SecondsCpu = c()
      for(c in 1:length(kafkaCpu$V1)) {
        SecondsCpu[c] = c
      }
      
      SecondsMem = c()
      for(m in 1:length(kafkaMem$V1)) {
        SecondsMem[m] = m
      }
      
      dfCpu <- data.frame(paste("Node " , x, sep=""), as.numeric(trim(substr(kafkaCpu$V1, 9, 14))), SecondsCpu)
      dfMemory <- data.frame(paste("Node " , x, sep=""), as.numeric(trim(substr(kafkaMem$V3, 2, 10)))*100/as.numeric(trim(substr(kafkaMem$V1, 11, 19))), SecondsMem)
      cpuUsage <- rbind(cpuUsage, dfCpu)
      memoryUsage <- rbind(memoryUsage, dfMemory)
    }
    
    names(cpuUsage) <- c("NODE","USAGE", "TIME")
    names(memoryUsage) <- c("NODE","USAGE","TIME")
    p1 <- ggplot(data=cpuUsage, aes(x=TIME, y=USAGE, group=NODE, colour=NODE)) + 
      geom_smooth(method="loess", se=F, size=0.5) + 
      guides(fill=FALSE) +
      scale_y_continuous(breaks= pretty_breaks()) +
      labs(x="Seconds", y="CPU load percentage") +
           #subtitle=paste(toupper(engine)," Benchmark","Kafka cpu usage with", toString(tps*i*10), "TPS")) +
      theme(plot.title = element_text(size = 8, face = "plain"), 
            plot.subtitle = element_text(size = 7, face = "plain"), 
            text = element_text(size = 7, face = "plain"),
            legend.justification = c(1, 0), 
            legend.position = c(1, 0),
            legend.box.margin=margin(c(3,3,3,3)),
            legend.key.height=unit(0.5,"line"),
            legend.key.width=unit(0.5,"line"),
            legend.text=element_text(size=rel(0.7)))
    ggsave(paste("KAFKA", "CPU.pdf", sep = "_"), width = 8, height = 8, units = "cm", device = "pdf", path = sourceFolder)
    
    p2 <- ggplot(data=memoryUsage, aes(x=TIME, y=USAGE, group=NODE, colour=NODE)) + 
      geom_smooth(method="loess", se=F, size=0.5) + 
      guides(fill=FALSE) +
      scale_y_continuous(breaks= pretty_breaks()) +
      labs(x="Seconds", y="Memory load percentage") +
           #subtitle=paste(toupper(engine),"Benchmark","Kafka memory usage with", toString(tps*i*10), "TPS")) +
      theme(plot.title = element_text(size = 8, face = "plain"), 
            plot.subtitle = element_text(size = 7, face = "plain"), 
            text = element_text(size = 7, face = "plain"),
            legend.justification = c(1, 0), 
            legend.position = c(1, 0),
            legend.box.margin=margin(c(3,3,3,3)),
            legend.key.height=unit(0.5,"line"),
            legend.key.width=unit(0.5,"line"),
            legend.text=element_text(size=rel(0.7)))
    ggsave(paste("KAFKA", "MEMMORY.pdf", sep = "_"), width = 8, height = 8, units = "cm", device = "pdf", path = sourceFolder)
    
    pdf(paste(sourceFolder, "TPS_",TPS,"_KAFKA_RESOURCE_LOAD", ".pdf", sep = ""), width = 8, height = 4)
    multiplot(p1, p2, cols = 2)
    dev.off()
  }
}
