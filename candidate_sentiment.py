from twitter_credentials import *
import datetime
from textblob import TextBlob
from sql_server import *
from retry import retry
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Gets yesterday's date and today's date
def get_dates():

    today = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')

    yesterday = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(1), '%Y-%m-%d')

    before_yesterday = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(2), '%Y-%m-%d')

    return before_yesterday, yesterday, today


def format_search(phrase):

    no_spaces = phrase.replace(' ', '')

    no_spaces_hastag = '#' + no_spaces

    return no_spaces_hastag

def format_search_last_name(phrase):

    last_name = phrase.split(' ', 1)[-1]

    return last_name


def get_sentiment(tweet):

    threshold = 0

    analysis = TextBlob(tweet)

    if analysis.sentiment.polarity > threshold:
        return 1, analysis.sentiment.polarity

    if analysis.sentiment.polarity < threshold:  # -threshold:
        return -1, analysis.sentiment.polarity

    return 0, analysis.sentiment.polarity


def sort_id_by_date(tweet_):

    id_date = []

    for tweet in tweet_:
        info = {
            "Tweet Id": tweet.id,
            "Tweet Created At": tweet.created_at
        }

        id_date.append(info)

    id_date.sort(key=lambda x:x['Tweet Created At'], reverse=False)

    if not id_date:
        return 0

    return id_date[0]['Tweet Id']


def get_last_tweet_from_before_yesterday(api, before_yesterday, yesterday, search_phrase):

    @retry(Exception, delay=15*61, tries=24)
    def try_get_last_tweet(api1, before_yesterday1, yesterday1, search_phrase1):
        tweet_ = api1.GetSearch(term=search_phrase1,
                                since=before_yesterday1,
                                until=yesterday1,
                                count=13)
        return sort_id_by_date(tweet_)

    return try_get_last_tweet(api, before_yesterday, yesterday, search_phrase)


def load_tweet_list(api, phrase):

    count = 0
    flag = 0

    tweets_list = []

    before_yesterday, yesterday, today = get_dates()

    last_tweet = get_last_tweet_from_before_yesterday(api, before_yesterday, yesterday, phrase)

    while count < 20:
        print(count)
        @retry(Exception, delay=15 * 61, tries=24)  # 6 hours
        def get_search(phrase1, yesterday1, today1, last_tweet1, flag1):
            print('get_search')
            if flag1 == 0:
                tweet_1 = api.GetSearch(term=phrase1,
                                        since=yesterday1,
                                        until=today1,
                                        count=100,
                                        since_id=last_tweet1)
                return tweet_1
            else:
                tweet_1 = api.GetSearch(term=phrase1,
                                        since=yesterday1,
                                        until=today1,
                                        count=100,
                                        max_id=last_tweet1)
                return tweet_1

        tweet_ = get_search(phrase, yesterday, today, last_tweet, flag)
        flag = 1

        for tweet in tweet_:
            tweets_list.append(tweet.full_text)
            last_tweet = tweet.id
        if len(tweet_) < 100:
            return tweets_list
        else:
            count += 1

    return tweets_list


def get_compound_sentiment(tweets):
    analyzer = SentimentIntensityAnalyzer()
    vs_total = 0

    for tweet in tweets:
        vs = analyzer.polarity_scores(tweet)
        vs_total += vs['compound']

    return vs_total / len(tweets)


def calculate_sentiment(tweets):

    positive_count = 0
    negative_count = 0
    neutral_count = 0
    compound_count = 0

    for tweet in tweets:
        score, compound = get_sentiment(tweet)
        compound_count = compound_count + compound
        if score > 0:
            positive_count += 1
        elif score < 0:
            negative_count += 1
        else:
            neutral_count += 1

    return positive_count, negative_count, neutral_count, (compound_count / len(tweets))


def write_sentiment(candidate, positive, negative, neutral, compound, compound_tb):

    yesterday = get_dates()[1]

    query = "INSERT INTO Candidate_Sentiment ([name], sentiment_date, positive_tweet_count," \
            " negative_tweet_count, neutral_tweet_count, compound_sentiment_vadersentiment, " \
            "compound_sentiment_textblob)" \
            " VALUES ('%s', '%s', '%s', '%s', '%s', %s, %s)" % (candidate, yesterday, positive,
                                                        negative, neutral, compound, compound_tb)

    insert_database(query)


def get_tweets_by_phrase(candidate):

    api = twitter_connection()

    search_phrase = candidate # format_search_last_name(phrase)

    tweets = load_tweet_list(api, search_phrase)

    positive, negative, neutral, compound_textblob = calculate_sentiment(tweets)

    compound = get_compound_sentiment(tweets)

    write_sentiment(candidate, positive, negative, neutral, compound, compound_textblob)





def get_tweet(phrase):

    api = twitter_connection()


def candidate_analysis():

    candidate = select_database('SELECT [name] FROM Candidate')

    for name in candidate['name']:
        get_tweets_by_phrase(name)


if __name__ == '__main__':

    candidate_analysis()
