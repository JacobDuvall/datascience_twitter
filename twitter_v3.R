# terminal error statuses:
# 10 - Failure to connect_to_datebase()
# 11 - Failure to SWIFT_get_search_phrases_and_dates()
# 12 - Failure to SWIFT_turn_on_is_running()
# 13 - Failure to SWIFT_turn_off_is_running()
# 14 - Failure to process_requests()
# 15 - Failure to format_tweets()
# 16 - Failure to connect_to_twitter()

# non-terminal error statuses:
# Failure to diconnect_from_database()
# Failure to insert_tweet_cache()
# Failure to SWIFT_interest_zero()
# Failure to SWIFT_interest_error()


library(RODBC)
library(RODBCext)
library(DBI)
library(rtweet)
library(twitteR)
library(stringr)
library(rvest)


# static variables
zero <- 0
success <- 'C'
failure <- 'E'


#  calculate potential views for a tweet
calculate_views <- function(name) {
  tryCatch({
    user <- getUser(name)
    return(user$followersCount)
  }, error = function(e) {
    return(-1)
  })
}




# connect to twitter with keys 
connect_to_twitter <- function() {
  tryCatch({
    consumer_key <- "ui5q58rlnjLIzolSsCQdxsNyy"
    consumer_secret <- "QYl8n7Iww9JVUHXm7N0BlGZowcpuWlo8SbvLtT4MgTsiGos8cM"
    access_token <- "1178665586278174721-Zhm3e9rO8s4HAq7N8jRmjOyuK7QUrj"
    access_secret <- "3Fwp3vqIwnjJA6v6Fjpahof3szIclfekWm0ACQJY5XjDS"
    setup_twitter_oauth(consumer_key, consumer_secret, access_token, access_secret)
  }, error = function(e) {
    write("Failure to connect_to_twitter()", stderr())
    quit(status=16)
  })
}


# connect to database
connect_to_database <- function() {
  tryCatch({
    conn <- odbcDriverConnect('driver={SQL Server};server=DevSQLswift;database=SwiftStage;trusted_connection=true')
    return(conn)
  }, warning = function(w) {
    write("Failure to connect to database", stderr())
    quit(status=10)
  })
}


# disconnect from database
disconnect_from_database <- function(conn) {
  tryCatch({
    try(SWIFT_turn_off_is_running(conn=conn))
    odbcClose(conn)
  }, error = function(e) {
    write("Failure to diconnect from database", stderr())
  })
}


# get search phrases and dates from Twitter.Request table in SwiftStage
SWIFT_get_search_phrases_and_dates <- function(conn) {
  tryCatch({
  exec_twitter <- "EXEC Twitter.get_search_phrase_dates_2"
  suppressWarnings({requests <- sqlExecute(channel=conn, query=exec_twitter, fetch = TRUE)})
  return(requests)
  }, error = function(e) {
    write("Failure to get search phrases and dates from Swift", stderr())
    disconnect_from_database(conn=conn)
    quit(status=11)
  })
}


# process requests from Twitter.Request
process_requests <- function(requests, conn)  {
  for(i in 1:nrow(requests)) {
    tryCatch({
      request_id <- requests$Request_Id[i]
      
      search_phrase <- requests$Search_Phrase[i]
      search_phrase <- gsub(' ', '', search_phrase, fixed = TRUE)
      search_phrase <- paste('#', search_phrase, sep = '')
      
      sample_date <- requests$Sample_Date[i]
  
      today <- format(Sys.Date(), '%Y-%m-%d')
      days_ago <- as.Date(today) - as.Date(sample_date) # int days between today and sample date
      
      date_since <- format(Sys.Date()-days_ago-1,'%Y-%m-%d')
      date_until <- format(Sys.Date()-days_ago, '%Y-%m-%d')
      
      process <-list(request_id = request_id, search_phrase = search_phrase,
                  date_since = date_since, date_until = date_until,
                  sample_date = sample_date)
    }, error = function(e) {
      write("Failure to process_requests()", stderr())
      disconnect_from_database(conn=conn)
      quit(status=14)
    })
    
    search_twitter(process=process, conn=conn)
  }
}


# format tweet_df into appropriate values
format_tweets <-function(tweet_df, row_number) {
  tryCatch({
    tweet = vector(mode="list")
    tweet$status_id <- tweet_df$status_id[row_number]
    tweet$user_id <- tweet_df$user_id[row_number]
    tweet$screen_name <- tweet_df$screen_name[row_number]
    text <- tweet_df$text[row_number]
    tweet$text <- gsub("'", "\''", text, fixed = TRUE)
    tweet$favorite_count <- tweet_df$favorite_count[row_number]
    hashtags <- tweet_df$hashtags[row_number]
    tweet$hashtags <- paste(hashtags, collapse = ', ')
    tweet$retweet_count <- tweet_df$retweet_count[row_number]
    tweet$reply_count <- tweet_df$reply_count[row_number]
    tweet$created_at <- tweet_df$created_at[row_number]
    if(is.na(tweet$reply_count) || tweet$reply_count == 0){
      tweet$reply_count <- 0
    }
    tweet$lang <- tweet_df$lang[row_number]
    #tweet$potential_views <- 100 #calculate_views(name = screen_name)
    tweet$potential_views <- calculate_views(name=tweet$screen_name)
  }, error = function(e) {
    write("Failure to format_tweets()", stderr())
    disconnect_from_database(conn=conn)
    quit(status=15)
  })
  
  return(tweet)
}

    
# insert tweet into Twitter.Tweet_Cache
insert_tweet_cache <- function(process, tweet, conn) {
  tryCatch({
    sqlExecute(channel = conn, 
               query="INSERT INTO Twitter.Tweet_Cache (Request_Id, Tweet_Id, Created_Dttm, User_Id, 
                                           Sample_Date, Screen_Name, Text, Favorite_Count, Retweet_Count, Reply_Count, Hashtags, Language, 
                                           Potential_Views) 
                                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
               data=data.frame(process$request_id, tweet$status_id, tweet$created_at, tweet$user_id, process$sample_date, 
                               tweet$screen_name, tweet$text, tweet$favorite_count, tweet$retweet_count, tweet$reply_count, 
                               tweet$hashtags, tweet$lang, tweet$potential_views))
  }, warning = function(w) {
    sqlExecute(channel = conn, 
               query="DELETE FROM Twitter.Tweet_Cache WHERE Request_Id = ? AND Sample_Date = ?",
               date = data.frame(process$request_id, process$sample_date))
    write("Failure to insert_tweet_cache", stderr())
  })
}


# search twitter with the requests from Twitter.Request
# and format tweets
# and insert tweets into Twitter.Tweet_Cache
search_twitter <- function(process, conn) { # try catch
  tryCatch({
    tweet_dataframe <- search_tweets(process$search_phrase, 
                                     include_rts = FALSE, 
                                     since = process$date_since, 
                                     until = process$date_until, 
                                     retryonratelimit = TRUE)
    if(is.data.frame(tweet_dataframe) && nrow(tweet_dataframe) > 0) {
      for(row_number in 1:nrow(tweet_dataframe)) {
        formatted_tweet <- format_tweets(tweet_df=tweet_dataframe, row_number)
        insert_tweet_cache(process, formatted_tweet, conn)
      }
    }
    else {
      SWIFT_interest_zero(process, conn)
  }
  }, error = function(e) {
    SWIFT_interest_error(process, conn, e)
  })
}


# run SwiftStage.Twitter.calculate_interest_from_cache
SWIFT_calculate_tweet_interest <- function(conn) {
  tryCatch({
    exec_twitter <- "EXEC Twitter.calculate_interest_from_cache"
    sqlExecute(channel=conn, query=exec_twitter)
  }, warning = function(w) {
    write("Failure to SWIFT_calculate_tweet_interest", stderr())
  })
}


# run SwiftStage.Twitter.interest_zero_or_error for no results found
SWIFT_interest_zero <- function(process, conn) {
  tryCatch({
    datetime <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    sqlExecute(channel = conn, 
               query="EXEC Twitter.interest_zero_or_error @Request_Id = ?, @Sample_Date = ?, 
               @Total_Tweets = ?, @Total_Retweets = ?, @Total_Favorites = ?, @Total_Potential_Views = ?, 
               @Result_Code = ?, @Result_Dttm = ?",
               data=data.frame(process$request_id, process$sample_date, zero, zero, zero, zero, success, datetime))
  }, warning = function(w) {
    write("Failure to SWIFT_interest_zero()", stderr())
  })
}

# run SwiftStage.Twitter.interest_zero_or_error for error in Twitter Search
SWIFT_interest_error <- function(process, conn, e) {
  tryCatch({
    datetime <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    sqlExecute(channel = conn, 
               query="EXEC Twitter.interest_zero_or_error @Request_Id = ?, @Sample_Date = ?, 
               @Total_Tweets = NULL, @Total_Retweets = NULL, @Total_Favorites = NULL, @Total_Potential_Views = NULL, 
               @Result_Code = ?, @Result_Dttm = ?, @Result_Detail = ?",
               data=data.frame(process$request_id, process$sample_date, failure, datetime, e$message))
  }, warning = function(w) {
    write("Failure to SWIFT_interest_error()", stderr())
  })

}


# set Is_Running in SwiftStage.Twitter.Status to 1
SWIFT_turn_on_is_running <- function(conn) {
  tryCatch({
    sqlExecute(channel = conn, 
               query="UPDATE SwiftStage.Twitter.[Status] SET Is_Running = 1")
  }, error = function(e) {
    write("Failure to update TWitter.[Status] to 1", stderr())
    disconnect_from_database(conn=conn)
    quit(status=12)
  })
}


# set Is_Running in SwiftStage.Twitter.Status to 0
SWIFT_turn_off_is_running <- function(conn) {
  tryCatch({
    sqlExecute(channel = conn, 
               query="UPDATE SwiftStage.Twitter.[Status] SET Is_Running = 0")
  }, error = function(e) {
    write("Failure to update TWitter.[Status] to 0", stderr())
    disconnect_from_database(conn=conn)
    quit(status=13)
  })
}


# MAIN 
# Establish Twitter Connection and SQL Server Connection
connect_to_twitter()
conn <- connect_to_database()

# Tell Swift this program is running
SWIFT_turn_on_is_running(conn=conn)

# Get and process requests from Swift
requests <- SWIFT_get_search_phrases_and_dates(conn=conn)
process_requests(requests=requests, conn=conn)

# Calculate interest from cache in Swift
SWIFT_calculate_tweet_interest(conn=conn)

# Disconnect from Swift and end program
disconnect_from_database(conn=conn)


