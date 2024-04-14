import os
import pandas as pd
from datetime import datetime
import random
import string

from models.user import CustomerInDB



# @app.post("/save_expense_history/")
async def save_expense_history(payload: dict):
    # Extract data from payload
    saving_user = payload.get("userId")
    expense_amount = payload.get("TotalSplittedAmount")
    expense_description = payload.get("desc")
    friends = payload.get("FriendsBetweenSplitting")

    # Prepare data for saving to expense history
    expense_data = {
        "SavingUser": saving_user,
        "ExpenseAmount": expense_amount,
        "ExpenseDescription": expense_description,
        "ExpenseDate": datetime.now(),
            "Friends": friends
    }

    # Create DataFrame with expense data
    df = pd.DataFrame(expense_data)

    # Define file path for saving expense history
    file_path = 'data/expense_history.parquet'

    # Check if the file already exists
    if os.path.exists(file_path):
        # Append new data to the existing file
        existing_table = pd.read_parquet(file_path)
        updated_table = pd.concat([existing_table, df], ignore_index=True)
        updated_table.to_parquet(file_path, index=False)
    else:
        # Create a new file and save the data
        df.to_parquet(file_path, index=False)

    return {"message": "Expense history saved successfully", "Response": "Success"}


def get_customer_by_username(username: str):
    file_path = 'data/customer_details.parquet'
    if not os.path.exists(file_path):
        return None

    df = pd.read_parquet(file_path)
    customer_data = df[df['username'] == username]
    if customer_data.empty:
        return None
    customer = CustomerInDB(username=customer_data['username'].iloc[0],
                            hashed_password=customer_data['hashed_password'].iloc[0],
                            mobileNo=customer_data['mobileNo'].iloc[0], email=customer_data['email'].iloc[0],
                            fullName=customer_data['fullName'].iloc[0], userId=customer_data['userId'].iloc[0])

    return customer



def username_exists(username: str):
    file_path = 'data/customer_details.parquet'
    if os.path.exists(file_path):
        df = pd.read_parquet(file_path)
        return username in df['username'].values
    else:
        return False


def phone_number_exists(phone_number: str):
    file_path = 'data/customer_details.parquet'
    if os.path.exists(file_path):
        df = pd.read_parquet(file_path)
        return phone_number in df['mobileNo'].values
    else:
        return False


# Function to get user details from customer_details.parquet

def get_user_details(userId: str):
    file_path = 'data/customer_details.parquet'
    df = pd.read_parquet(file_path)
    user_row = df[df['userId'] == userId]
    if not user_row.empty:
        user_details = {
            'username': user_row['username'].iloc[0],
            'userId': user_row['userId'].iloc[0],
            'fullName': user_row['fullName'].iloc[0]
        }
        return user_details
    else:
        return None



# Function to create parquet file for pending friend requests
def create_pending_requests_parquet():
    file_path = 'data/PendingRequests.parquet'
    if not os.path.exists(file_path):
        df = pd.DataFrame(columns=['requesterUserId', 'requestedUserId'])
        df.to_parquet(file_path, index=False)



def add_friend_to_connections(data, userid):
    file_path = f'data/MyConnection_{userid}.parquet'
    df = pd.read_parquet(file_path)
    print(df)
    print(data)
    print(userid)
    print(data.get("userId"))
    if data.get("userId") not in df['friendUserId'].values:
        new_data = {'friendUsername': [data.get("username")], 'friendUserId': [data.get("userId")],
                    'expenseAddedBy': [userid],
                    'TotalSplittedAmount': [0], 'amountFromFriend': [0], 'CalculatedMoney': [0]}
        new_df = pd.DataFrame(new_data)
        df = pd.concat([df, new_df], ignore_index=True)
        df.to_parquet(file_path, index=False)


def generate_user_id(username):
    # Generate a random string of digits
    random_digits = ''.join(random.choices(string.digits, k=6))
    # Concatenate username with random digits
    user_id = f"{username}{random_digits}"
    return user_id


# Function to create parquet file for user's connections
def create_connections_parquet(userId: str):
    file_path = f'data/MyConnection_{userId}.parquet'
    if not os.path.exists(file_path):
        df = pd.DataFrame(
            columns=['fullName', 'friendUsername', 'friendUserId', 'expenseAddedBy', 'TotalSplittedAmount',
                     'amountFromFriend', 'CalculatedMoney'])
        df.to_parquet(file_path, index=False)


# Function to create Parquet file for user's connections
def create_initially_connections_parquet(userId: str, username: str, fullName: str):
    file_path = f'data/MyConnection_{userId}.parquet'
    df = pd.DataFrame(
        columns=['friendUsername', 'friendUserId', 'expenseAddedBy', 'TotalSplittedAmount', 'amountFromFriend',
                 'CalculatedMoney'])

    df.loc[0] = [username, userId, username, 0, 0, 0]  # Add user's own details as the first entry
    df.to_parquet(file_path, index=False)



def update_friend_expense(userId: str, friend_userId: str, amount: float):
    file_path = f'data/MyConnection_{userId}.parquet'
    if os.path.exists(file_path):
        df = pd.read_parquet(file_path)
        if friend_userId in df['friendUserId'].values:
            # Update friend's expense amount
            df.loc[df['friendUserId'] == friend_userId, 'TotalSplittedAmount'] += amount

            # Get the updated TotalSplittedAmount for the friend
            amount_update_in_friends_file = \
                df.loc[df['friendUserId'] == friend_userId, 'TotalSplittedAmount'].values[0]

            # Save the changes to the user's file
            df.to_parquet(file_path, index=False)

            # Update friend's amountFromFriend in friend's file
            friend_file_path = f'data/MyConnection_{friend_userId}.parquet'
            if os.path.exists(friend_file_path):
                friend_df = pd.read_parquet(friend_file_path)
                friend_df.loc[
                    friend_df['friendUserId'] == userId, 'amountFromFriend'] = -amount_update_in_friends_file
                friend_df.to_parquet(friend_file_path, index=False)

                # Update user's amountFromFriend in user's file
                user_amount_from_friend = -amount_update_in_friends_file
                amount_update_in_My_file = \
                    friend_df.loc[friend_df['friendUserId'] == userId, 'TotalSplittedAmount'].values[0]
                user_file_path = f'data/MyConnection_{userId}.parquet'
                if os.path.exists(user_file_path):
                    user_df = pd.read_parquet(user_file_path)
                    user_df.loc[
                        user_df['friendUserId'] == friend_userId, 'amountFromFriend'] = -amount_update_in_My_file
                    user_df.to_parquet(user_file_path, index=False)
