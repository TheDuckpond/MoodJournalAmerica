# Plot the term weights of manually labeled topics and topic
# distribution of classified documents.
# @authored rpetchler http://rosspetchler.com/enron-topic-modeling.html
# @modified malam 21 July 2014

library(ggplot2)
library(RJSONIO)
library(scales)


######################################################################
# Posterior manual labeling of topic

# Modify number of topics as needed
topics <- data.frame(
  rbind(
    c(1, "Topic I"),
    c(2, "Topic II"),
    c(3, "Topic III"),
    c(4, "Topic IV")
  )
)
colnames(topics) <- c("topic", "label")


######################################################################
# Plot the term weights of manually labeled topics

dat <- readLines("topics_overall.txt")
dat <- dat[2:5] # skip header, final line
dat <- gsub('"', "",dat)
dat <- strsplit(dat, split = " + ", fixed = TRUE)
dat <- lapply(dat, strsplit, split = "*", fixed = TRUE)
dat <- lapply(dat, do.call, what = rbind)
dat <- do.call(rbind, dat)

dat <- as.data.frame(dat, stringsAsFactors = FALSE)
colnames(dat) <- c("weight", "term")
dat$weight <- as.numeric(dat$weight)

dat$topic <- rep(seq(4), each = 10) # again, modify with number of topics
dat$index <- rep(seq(40))

dat <- merge(dat, topics)
dat <- dat[, c("label", "index", "term", "weight")]

p <- ggplot(dat, aes(x = weight, y = index, label = term)) +
  facet_wrap(~ label,scales="free_y") +
  geom_text(hjust = 0, size = 5) +
  scale_x_continuous("Weight") + scale_y_continuous("hello")+
  theme(axis.title.y = element_blank(),
        axis.text.y = element_blank(),
        axis.ticks.y = element_blank())

# Not necessary - make sure group permissions are set appropriately after cron
#file.remove("topic_term_weights.png")

#Save file to data directory
ggsave("topic_term_weights.png", width = 16, height = 8)