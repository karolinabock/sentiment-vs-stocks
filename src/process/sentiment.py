#sentiment analyze
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

vader = SentimentIntensityAnalyzer()

def get_sentiment(text):
    if not isinstance(text, str) or not text.strip():
        return None, None

    vader_score = vader.polarity_scores(text)['compound']

    if vader_score >= 0.05:
        sentiment = 'positive'
    elif vader_score <= -0.05:
        sentiment = 'negative'
    else:
        sentiment = 'neutral'

    return vader_score, sentiment
