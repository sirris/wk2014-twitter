################################################################################
# Bar chart with player mentions
################################################################################

ds = read.delim('freq.players.tab', sep='\t')
ds.o = ds[order(ds$freq, decreasing=TRUE),]

png('freq.players.png', width=2000, height=1000, res=200)
par(mar=c(8,4,2,1))
barplot(ds.o$freq, xlab='', ylab='', axes=F, col='white', border=F)
hlines = seq(from=0, to=max(ds$freq), by=5000)
abline(h=hlines, col='grey', lty=3)
barplot(ds.o$freq, ylab='Amount of mentions in tweets', 
        cex.names=0.7, cex.lab=0.6, cex.axis=0.7, 
        col=rgb(1, 0.1, 0.1,0.8), add=TRUE,
        names.arg=ds.o$name, las=2)
dev.off()

################################################################################
# R Map with geolocated tweets
################################################################################

# stuff that you need to install/load
library(sp)
library(maptools)
library(spdep)
library(RANN)
library(maps)
library(mapdata)
library(xtable)

## some functions that are needed for non-us maps
loadGADM <- function (fileName, level = 0, ...) {
	print(paste("./", fileName, "_adm", level, ".RData", sep = ""))
	load(paste("./", fileName, "_adm", level, ".RData", sep = ""))
	gadm
}

changeGADMPrefix <- function (GADM, prefix) {
	GADM <- spChFIDs(GADM, paste(prefix, row.names(GADM), sep = "_"))
	GADM
}

loadChangePrefix <- function (fileName, level = 0, ...) {
	theFile <- loadGADM(fileName, level)
	theFile <- changeGADMPrefix(theFile, fileName)
	theFile
}

getCountries <- function (fileNames, level = 0, ...) {
	polygon <- sapply(fileNames, loadChangePrefix, level)
	polyMap <- do.call("rbind", polygon)
  polyMap
}

# read locations table
locations = read.delim('locations.tab', sep=',', header=F)
colnames(locations) = c('lng', 'lat', 'freq')
fr = locations$freq/max(locations$freq)

# get map
spdf <- getCountries(c("BEL"), level=1)

# plot coordinates
png('tweet.in.belgium.png', height=1500, width=1500, res=200)
plot(spdf, border='darkgrey')
points(locations$lng, locations$lat, pch=16, col=rgb(1, 0.1, 0.1,0.8), cex=fr+0.5)
points(locations$lng, locations$lat, cex=fr+0.5, col='darkgrey')
dev.off()

################################################################################
# Time Series plot with annotations
################################################################################

# read table
tweets.minute = read.delim('tweets.minute.tab', sep='\t', header=T)
starttime = strptime('18:00', '%H:%M')
stoptime = strptime('20:15', '%H:%M')

# annotations
annos = read.delim('annos_belrus.tab', sep=';', header=F)
# parse timestamp
tweets.minute$timestamp = strptime(tweets.minute$variable, '%H:%M') + 3600*2
# shorten
tweets.minute = droplevels( subset(tweets.minute,
                       timestamp > (starttime - (15*60)) &
                       timestamp < stoptime) )
# sort
tweets.minute.ordered = tweets.minute[ order(tweets.minute$timestamp), ]
d = tweets.minute.ordered[!is.na(tweets.minute.ordered$timestamp),]
# plot
png('tweets.minute.png', height=1000, width=3000, res=180)
plot(d$timestamp, d$freq, type='l', ylim=c(0, max(d$freq) + 5000),
     xlab='', ylab='Amount of tweets', axes=F, cex.lab=0.6
    )
axis.POSIXct(1, x=d$timestamp, 
             at=seq(from=starttime,to=d$timestamp[length(d$timestamp)],by=600), 
             cex.axis=0.7)
axis(2, at=seq(from=1000, max(d$freq), by=2000), las=1, cex.axis=0.7)
# set horizontal lines
hl = seq(from=1000, to=max(d$freq), by=1000)
abline(h=hl, col='grey', lty=2)
# create annotations
for (i in 1:nrow(annos)){
  t = starttime + (annos[i,1] * 60) + (annos[i,2] * 60)
  abline(v=t, lty=3, col='darkgrey')
  text(t, max(d$freq) + 1500, annos[i,3], srt=90, pos=3, cex=0.7)
}
# create area
polygon( x=c( d$timestamp[1], d$timestamp, d$timestamp[nrow(d)], 
              d$timestamp[nrow(d)], rev(d$timestamp), d$timestamp[1]),
         y=c( 0, d$freq, 0,
              rep(0, nrow(d) + 2) ),
         col=rgb(1, 0.1, 0.1,0.8), border=NA
       )
dev.off()
