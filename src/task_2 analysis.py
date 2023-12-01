from loader import *
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from textblob import TextBlob


def time_diff_distribution(df):
    # Assuming df is your DataFrame
    df["msg_sent_time"] = pd.to_datetime(df["msg_sent_time"], unit="s")

    # Calculate time differences
    df["time_diff_msg"] = df["msg_sent_time"].diff().dt.total_seconds()
    df["time_diff_reply"] = (
        pd.to_datetime(df["tm_thread_end"], unit="s") - df["msg_sent_time"]
    ).dt.total_seconds()
    df[
        "time_diff_reaction"
    ] = ...  # Calculate the time difference for reactions similarly

    # Plot histograms
    plt.figure(figsize=(12, 8))
    plt.subplot(221)
    plt.hist(
        df["time_diff_msg"].dropna(), bins=50, color="blue", alpha=0.7, label="Messages"
    )
    plt.legend()
    plt.title("Time Difference Between Consecutive Messages")

    plt.subplot(222)
    plt.hist(
        df["time_diff_reply"].dropna(),
        bins=50,
        color="green",
        alpha=0.7,
        label="Replies",
    )
    plt.legend()
    plt.title("Time Difference Between Consecutive Replies")

    plt.subplot(223)
    plt.hist(
        df["time_diff_reaction"].dropna(),
        bins=50,
        color="orange",
        alpha=0.7,
        label="Reactions",
    )
    plt.legend()
    plt.title("Time Difference Between Consecutive Reactions")

    plt.tight_layout()
    plt.show()


def message_sentiment_analysis(df):
    df["sentiment"] = df["msg_content"].apply(lambda x: TextBlob(x).sentiment)

    # Classification and Topic Modeling
    vectorizer = CountVectorizer(stop_words="english")
    X = vectorizer.fit_transform(df["msg_content"])

    lda = LatentDirichletAllocation(n_components=10, random_state=42)
    topics = lda.fit_transform(X)

    df["topic"] = topics.argmax(axis=1)
    return df


def top_10_topics_by_channel(df):
    top_topics_per_channel = (
        df.groupby(["channel", "topic"])
        .size()
        .groupby("channel")
        .nlargest(10)
        .reset_index(level=0, drop=True)
    )
    print("********** top 10 topis of channel")
    print(top_topics_per_channel)


def sentiment_analysis_overtime(df):
    df["date"] = df["msg_sent_time"].dt.date
    daily_sentiment = df.groupby("date")["sentiment"].mean()

    plt.figure(figsize=(12, 6))
    daily_sentiment.plot()
    plt.title("Sentiment Trend Over Time")
    plt.xlabel("Date")
    plt.ylabel("Average Sentiment")
    plt.show()


def main(data_path):
    print("in main")
    loader = SlackDataLoader(data_path)
    df = loader.slack_parser()
    time_diff_distribution(df)
    df = message_sentiment_analysis(df)
    top_10_topics_by_channel(df)
    sentiment_analysis_overtime(df)


if __name__ == "__main__":
    # Using the configuration from config.py
    data_path = cfg.path
    main(data_path)
