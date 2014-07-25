### @authored malam, 28 June 2014 ###
### modified 3 July 2014, malam ###
### Generates frequency CSV to be used in word cloud graphing ###
### Uses Older Version of TM on RHEL ###
### wget http://cran.r-project.org/src/contrib/Archive/tm/tm_0.5-8.3.tar.gz ###
### follow errors to install dependencies ###


#### Prepares a word-freq table for use in wordcloud visualizations ####
library("stringr")
library("tm")
library("wordcloud")

# Read In Data File
tweet_data <- read.csv("news_text_all.csv",encoding="UTF-8",header=TRUE)

# Create a corpus of text field
tweet.corpus <- Corpus(DataframeSource(data.frame(as.character(tweet_data$text))))

#Clean Up tweets
tweet.corpus <- tm_map(tweet.corpus, tolower)
tweet.corpus <- tm_map(tweet.corpus, removePunctuation)
tweet.corpus <- tm_map(tweet.corpus, function(x) removeWords(x, stopwords("english")))

#Create TermDocumentMatrix
tweetTDM <- TermDocumentMatrix(tweet.corpus)
tdMatrix <- as.matrix(tweetTDM)  #IF THIS STEP GIVES AN ERROR, LIMIT NUMBER OF TERMS
sortedMatrix <- sort(rowSums(tdMatrix), decreasing=TRUE)

#create word- freq table
cloudFrame <- data.frame(word=names(sortedMatrix), freq=sortedMatrix)

#Write file out to CSV
write.csv(cloudFrame,file="word-freq-news-all.csv",row.names=FALSE)

to_plot <-wordcloud(cloudFrame$word, cloudFrame$freq, max.words=50, 
          colors=topo.colors(10)))

# save wordcloud file as .png
# note: not nearly this simple for ggplot2 lattice charts
png("wordcloud_static.png", width=12, height=8, units="in", res=300)
wordcloud(cloudFrame$word, cloudFrame$freq, max.words=50, 
           colors=brewer.pal(8, "Dark2"))
dev.off()
