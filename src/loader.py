import glob
import json
import argparse
import os
import re
import io
import shutil
import copy
from datetime import datetime
from pick import pick
from time import sleep
import pandas as pd
import json
import glob
from config import cfg


# Create wrapper classes for using slack_sdk in place of slacker
#
class SlackDataLoader:
    """
    Slack exported data IO class.

    When you open slack exported ZIP file, each channel or direct message
    will have its own folder. Each folder will contain messages from the
    conversation, organised by date in separate JSON files.

    You'll see reference files for different kinds of conversations:
    users.json files for all types of users that exist in the slack workspace
    channels.json files for public channels,

    These files contain metadata about the conversations, including their names and IDs.

    For secruity reason, we have annonymized names - the names you will see are generated using faker library.

    """

    def __init__(self, path):
        """
        path: path to the slack exported data folder
        """
        self.path = path
        self.channels = self.get_channels()
        self.users = self.get_users()

    # combine all json file in all-weeks8-9
    def slack_parser(self):
        """parse slack data to extract useful informations from the json file
        step of execution
        1. Import the required modules
        2. read all json file from the provided path
        3. combine all json files in the provided path
        4. extract all required informations from the slack data
        5. convert to dataframe and merge all
        6. reset the index and return dataframe
        """

        # specify path to get json files
        combined = []
        path_channel = self.path
        # print("---------",path_channel)

        for json_file in glob.glob(
            os.path.join(path_channel, "all-week1", "*.json"), recursive=True
        ):
            with open(json_file, "r", encoding="utf8") as slack_data:
                combined.append(json.load(slack_data))
                # json_content = json.load(slack_data)
                # combined.extend(json_content)

        # loop through all json files and extract required informations
        dflist = []
        for slack_data in combined:
            (
                msg_type,
                msg_content,
                sender_id,
                time_msg,
                msg_dist,
                time_thread_st,
                reply_users,
                reply_count,
                reply_users_count,
                tm_thread_end,
                reactions_count,
                reactions_users,
            ) = ([], [], [], [], [], [], [], [], [], [], [], [])

            for row in slack_data:
                if "bot_id" in row.keys():
                    continue
                else:
                    msg_type.append(row["type"])
                    msg_content.append(row["text"])
                    if "user_profile" in row.keys():
                        sender_id.append(row["user_profile"]["real_name"])
                    else:
                        sender_id.append("Not provided")
                    time_msg.append(row["ts"])
                    if (
                        "blocks" in row
                        and row["blocks"]
                        and "elements" in row["blocks"][0]
                        and row["blocks"][0]["elements"]
                    ):
                        msg_dist.append(
                            row["blocks"][0]["elements"][0]["elements"][0]["type"]
                        )

                    else:
                        msg_dist.append("reshared")
                    if "thread_ts" in row.keys():
                        time_thread_st.append(row["thread_ts"])
                    else:
                        time_thread_st.append(0)
                    if "reply_users" in row.keys():
                        reply_users.append(",".join(row["reply_users"]))
                    else:
                        reply_users.append(0)
                    if "reply_count" in row.keys():
                        reply_count.append(row["reply_count"])
                        reply_users_count.append(row["reply_users_count"])
                        tm_thread_end.append(row["latest_reply"])
                    else:
                        reply_count.append(0)
                        reply_users_count.append(0)
                        tm_thread_end.append(0)
                    if "reactions" in row.keys():
                        reactions = row["reactions"]
                        reactions_count_per_message = len(reactions)
                        users_list_per_reaction = ",".join(
                            [
                                user
                                for reaction in reactions
                                for user in reaction["users"]
                            ]
                        )
                        reactions_count.append(reactions_count_per_message)
                        reactions_users.append(users_list_per_reaction)
                    else:
                        reactions_count.append(0)
                        reactions_users.append("")

            data = zip(
                msg_type,
                msg_content,
                sender_id,
                time_msg,
                msg_dist,
                time_thread_st,
                reply_count,
                reply_users_count,
                reply_users,
                tm_thread_end,
                reactions_count,
                reactions_users,
            )
            columns = [
                "msg_type",
                "msg_content",
                "sender_name",
                "msg_sent_time",
                "msg_dist_type",
                "time_thread_start",
                "reply_count",
                "reply_users_count",
                "reply_users",
                "tm_thread_end",
                "reactions_count",
                "reactions_users",
            ]
            # print("--------------",reply_count)
            # print("00000000000000",reply_users)
            # print("77777777",reply_users_count)

            df = pd.DataFrame(data=data, columns=columns)
            df = df[df["sender_name"] != "Not provided"]
            dflist.append(df)

        # print("---------",dflist)

        dfall = pd.concat(dflist, ignore_index=True)
        dfall["channel"] = path_channel.split("/")[-1].split(".")[0]
        dfall = dfall.reset_index(drop=True)

        print(dfall.columns)

        return dfall

    def convert_2_timestamp(self, column, data):
        """convert from unix time to readable timestamp
        args: column: columns that needs to be converted to timestamp
                data: data that has the specified column
        """
        if column in data.columns.values:
            timestamp_ = []
            for time_unix in data[column]:
                if time_unix == 0:
                    timestamp_.append(0)
                else:
                    a = datetime.fromtimestamp(float(time_unix))
                    timestamp_.append(a.strftime("%Y-%m-%d %H:%M:%S"))
            return timestamp_
        else:
            print(f"{column} not in data")

    def get_tagged_users(self, df):
        """get all @ in the messages"""

        # return df['msg_content'].map(lambda x: re.findall(r'@U\w+', x))
        return df["msg_content"].apply(
            lambda x: re.findall(r"<@(.*?)>", x) if pd.notna(x) else []
        )

    def get_users(self):
        """
        write a function to get all the users from the json file
        """
        with open(os.path.join(self.path, "users.json"), "r") as f:
            users = json.load(f)

        return users

    def get_channels(self):
        """
        write a function to get all the channels from the json file
        """
        with open(os.path.join(self.path, "channels.json"), "r") as f:
            channels = json.load(f)

        return channels

    def get_channel_messages(self, channel_name):
        """
        write a function to get all the messages from a channel

        """

    #
    def get_user_map(self):
        """
        write a function to get a map between user id and user name
        """
        userNamesById = {}
        userIdsByName = {}
        for user in self.users:
            userNamesById[user["id"]] = user["name"]
            userIdsByName[user["name"]] = user["id"]
        return userNamesById, userIdsByName
