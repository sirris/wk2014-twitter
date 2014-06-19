# read table
tweets.minute = read.delim('tweets.minute.tab', sep='\t', header=T)

starttime = strptime('18:00', '%H:%M')
# annotations
annos = read.delim('annos_nedaus.tab', sep=';', header=F)

# parse timestamp
tweets.minute$timestamp = strptime(tweets.minute$variable, '%H:%M') + 3600*2

# sort
tweets.minute.ordered = tweets.minute[ order(tweets.minute$timestamp), ]
d = tweets.minute.ordered[!is.na(tweets.minute.ordered$timestamp),]

# plot
png('tweets.minute.png', height=1000, width=2500, res=200)
plot(d$timestamp, d$freq, type='l', ylim=c(0, max(d$freq) + 5000),
     xlab='', ylab='Amount of tweets', axes=F, cex.lab=0.6
    )
axis.POSIXct(1, x=d$timestamp, 
             at=seq(from=starttime, to=d$timestamp[length(d$timestamp)], by=600), 
             cex.axis=0.7)
axis(2, at=seq(from=1000, max(d$freq), by=2000), las=1, cex.axis=0.7)

# set horizontal lines
hl = seq(from=1000, to=max(d$freq), by=1000)
abline(h=hl, col='grey', lty=2)

# create annotations
for (i in 1:nrow(annos)){
  t = starttime + annos[i,1] * 60
  abline(v=t, lty=3, col='darkgrey')
  text(t, max(d$freq) + 1000, annos[i,2], srt=90, pos=3, cex=0.7)
}

# create area
polygon( x=c( d$timestamp[1], d$timestamp, d$timestamp[nrow(d)], 
              d$timestamp[nrow(d)], rev(d$timestamp), d$timestamp[1]),
         y=c( 0, d$freq, 0,
              rep(0, nrow(d) + 2) ),
         col=rgb(1, 0.1, 0.1,0.8), border=NA
       )
dev.off()
