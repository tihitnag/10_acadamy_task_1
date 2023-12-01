from loader import *
import matplotlib.pyplot as plt
import seaborn as sns
import datetime


def make_analysis_on_slack(data_path):
    loader = SlackDataLoader(data_path)
    df = loader.slack_parser()

    print("#################Top and Bottom 10 Users:############################")
    # reply count
    top_10_reply_users = df.groupby("sender_name")["reply_count"].sum().nlargest(10)
    bottom_10_reply_users = df.groupby("sender_name")["reply_count"].sum().nsmallest(10)
    print("----------top_10_reply_users--------")
    print(top_10_reply_users)
    print("----------bottom_10_reply_users--------")
    print(bottom_10_reply_users)
    # mention count
    # # mention count
    # Extract mentioned users using regular expression
    # df['mentioned_users'] = loader.get_tagged_users(df)
    df["mentioned_users"] = df["msg_content"].apply(
        lambda x: re.findall(r"<@(.*?)>", x) if pd.notna(x) else []
    )
    # Count mentions for each user
    df["mention_count"] = df["mentioned_users"].apply(len)
    # Who are the top and bottom 10 users by Mention?
    top_10_mention_users = df.groupby("sender_name")["mention_count"].sum().nlargest(10)
    bottom_10_mention_users = (
        df.groupby("sender_name")["mention_count"].sum().nsmallest(10)
    )
    print("----------top_10_mention_users--------")
    print(top_10_mention_users)
    print("----------bottom_10_mention_users--------")
    print(bottom_10_mention_users)

    # message count
    top_10_message_users = df["sender_name"].value_counts().nlargest(10)
    bottom_10_message_users = df["sender_name"].value_counts().nsmallest(10)
    print("----------top_10_message_users--------")
    print(top_10_message_users)
    print("----------bottom_10_message_users--------")
    print(bottom_10_message_users)
    # reaction count
    # Exploding the 'reactions_users' column to have one row per user
    df_expanded = df.explode("reactions_users")

    # Counting the occurrences of each user
    user_reaction_counts = df_expanded["reactions_users"].value_counts()
    top_10_reaction_users = user_reaction_counts.nlargest(10)
    bottom_10_reaction_users = user_reaction_counts.nsmallest(10)
    print("----------top_10_reaction_users--------")
    print(top_10_reaction_users)
    print("----------bottom_10_reaction_users--------")
    print(bottom_10_reaction_users)

    print("#################Top 10 Messages:############################")
    # reply count
    top_10_replies = df[df["reply_count"] > 0].nlargest(10, "reply_count")
    print("----------top_10_replies--------")
    print(top_10_replies)
    # mention count
    top_10_mentions = (
        df["reply_users"]
        .str.split(",", expand=True)
        .stack()
        .value_counts()
        .nlargest(10)
    )
    print("----------top_10_mentions--------")
    print(top_10_mentions)
    # reaction count
    # Explode the 'reactions_users' column to have one row per user
    df_expanded = df.explode("reactions_users")
    top_10_reactions = df_expanded.groupby("msg_content").size().nlargest(10)
    print("----------top_10_reactions--------")
    print(top_10_reactions)
    # Channel with Highest Activity:
    most_active_channel = df["channel"].value_counts().idxmax()
    print("----------most_active_channel--------")
    print(most_active_channel)

    # 2D Scatter Plot:
    print("^^^^^^^^^^^^@@@@@@@ 2D Scatter Plot @@@@@@@@@^^^^^^^^^^^^^^^")
    # Create a new DataFrame with channel-level data
    channel_data = (
        df.groupby("channel")
        .agg({"msg_content": "count", "reply_count": "sum", "reactions_count": "sum"})
        .reset_index()
    )

    # Plot
    plt.figure(figsize=(10, 8))
    scatter_plot = sns.scatterplot(
        x="msg_content", y="reply_count", hue="channel", data=channel_data
    )
    plt.show()
    # Fraction of Messages Replied Within First 5 Minutes:
    print(
        "^^^^^^^^^^^^@@@@@@@ Fraction of Messages Replied Within First 5 Minutes @@@@@@@@@^^^^^^^^^^^^^^^"
    )

    # first_reply_time = df.groupby('channel')['tm_thread_end'].min()
    df["tm_thread_end"] = pd.to_datetime(df["tm_thread_end"], unit="s")
    df["msg_sent_time"] = pd.to_datetime(df["msg_sent_time"], unit="s")

    # Calculate the time difference in seconds
    df["time_diff"] = df["tm_thread_end"] - df["msg_sent_time"]

    # Count messages replied within 5 minutes within each group
    messages_replied_within_5mins = (
        df[df["time_diff"] <= pd.to_timedelta("300 seconds")]
        .groupby("channel")["msg_content"]
        .count()
    )

    # Calculate first reply time for each channel
    first_reply_time = df.groupby("channel")["tm_thread_end"].min()

    # Calculate the time difference in seconds
    df["time_diff"] = df["tm_thread_end"] - df["msg_sent_time"]

    # Calculate total messages for each channel
    total_messages = df.groupby("channel")["msg_content"].count()
    # Calculate fraction of messages replied within 5 minutes
    fraction_replied_within_5mins = messages_replied_within_5mins / total_messages

    # Print or use the results as needed
    print("~~~~~~~~~~~~first_reply_time~~~~~~~~~~~~~")

    print(first_reply_time)
    print("~~~~~~~~~~~~messages_replied_within_5mins~~~~~~~~~~~~~")

    print(messages_replied_within_5mins)
    print("~~~~~~~~~~~~fraction_replied_within_5mins~~~~~~~~~~~~~")

    print(fraction_replied_within_5mins)

    # 2D Scatter Plot with Time Difference and Time of the Day:
    print(
        "^^^^^^^^^^^^@@@@@@@ 2D Scatter Plot with Time Difference and Time of the Day: @@@@@@@@@^^^^^^^^^^^^^^^"
    )

    # Convert timestamps to datetime objects
    df["msg_sent_time"] = pd.to_datetime(df["msg_sent_time"], unit="s")
    df["tm_thread_end"] = pd.to_datetime(df["tm_thread_end"], unit="s")

    # Calculate time difference
    df["time_difference"] = (
        df["tm_thread_end"] - df["msg_sent_time"]
    ).dt.total_seconds()

    # Plot
    plt.figure(figsize=(10, 8))
    scatter_plot_time = sns.scatterplot(
        x="time_difference", y=df["msg_sent_time"].dt.hour, hue="channel", data=df
    )
    plt.show()


if __name__ == "__main__":
    # Using the configuration from config.py
    data_path = cfg.path
    make_analysis_on_slack(data_path)
