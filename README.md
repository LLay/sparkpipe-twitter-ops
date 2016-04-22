# Uncharted Spark Pipeline Twitter Operations &nbsp;[![Build Status](https://travis-ci.org/unchartedsoftware/sparkpipe-twitter-ops.svg?branch=master)](https://travis-ci.org/unchartedsoftware/sparkpipe-twitter-ops)&nbsp;[![Coverage Status](https://coveralls.io/repos/github/unchartedsoftware/sparkpipe-twitter-ops/badge.svg?branch=LLay-patch-1)](https://coveralls.io/github/unchartedsoftware/sparkpipe-twitter-ops?branch=LLay-patch-1)

This is a [Sparkpipe](http://unchartedsoftware.github.io/sparkpipe-core) library of operations designed for manipulating Twitter data. The focus is the extraction and aggregation of diffuse or nested data into new columns, making data more accessible. The library is divided into seperate packages for each of the objects present in the twitter API: Tweets, Users, Entities, and Places.


###Twitter Schemas
The library contains a schema for each object. Below schema fields are indicated with *italics*.

#Packages

##Tweets
#####Schema
[Tweets](https://dev.twitter.com/overview/api/tweets) are the objects associated with a given tweet. Tweets contain the text of the tweet, as well as various information about the context of the tweet and it's status on twitter, including *retweet\_count*, *favourite_count*, etc. Tweet objects can also contain another tweet in the form of a retweet (i.e. *retweeted_status* or *quoted_status*). These nested tweets, however, cannot contain tweets themselves, therefore tweet nesting is capped at a depth of 1.
#####Functionality
The Tweets package contains a read function that allows you to read in tweets from a source file (perhaps obtained from a [GET search/tweets](https://dev.twitter.com/rest/reference/get/search/tweets)) into a Spark Dataframe. 
```scala
scala> import software.uncharted.sparkpipe.Pipe
scala> val pipe = Pipe(sqlContext).to(ops.community.twitter.tweets.read("/my/path/to/tweets.json", "json"))
scala> val dataframe = pipe.run
```
The package also contains operations to extract all hashtags or user mentions present in the tweet. Additionally you can extract the geographic coordinates of the tweet, and all urls in the tweet object, excluding those in the retweet object.
```scala
scala> val pipe = Pipe(sqlContext).to(read("/my/path/to/tweets.json", "json")).to(hashtags())
scala> val dataframe = pipe.run
```

##Users
#####Schema
[Users](https://dev.twitter.com/overview/api/users) are the objects associated with a given twitter profile. They contain extensive information about that twitter profile, as well as a *status* Tweet object. Users are present in tweet objects as the author of that tweet.
#####Functionality
Similar to Tweets, the Users package also has a read function that allows you to read in data from a source file.

##Entities
#####Schema
[Entities](https://dev.twitter.com/overview/api/entities) and [Entities in Objects](https://dev.twitter.com/overview/api/entities-in-twitter-objects) contain information about a tweet or user that can be logically grouped. Some of the various entity objects are: 
- media
- urls
- user_mentions
- hashtags
- symbols
- etc.

These entity objects are found in the *entities* object in a tweet or user object

##Places
#####Schema
[Places](https://dev.twitter.com/overview/api/places) are objects that represent specific named locations with corresponding geo coordinates. They are attached to tweets in the *place* field
