from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import pandas as pd
from passlib.context import CryptContext
import uvicorn as uvicorn
import pyarrow as pa
import pyarrow.parquet as pq
import os

from starlette.middleware.cors import CORSMiddleware

from authentication.auth import oauth2_scheme
from authentication.jwt import create_access_token, decode_token
from authentication.password import verify_password, hash_password
from models.user import User, ExpensePayload, SplitExpensePayload, Customer, CustomerInDB
from datetime import datetime
from typing import List
import random
import string

app = FastAPI()
# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Create Parquet file structure
# if not os.path.exists('data/'):
#     os.makedirs('data/')
# if not os.path.exists('data/personal_expenses.parquet'):
#     df_personal_expenses = pd.DataFrame(columns=['username', 'date','expenseName', 'amount'])
#     df_personal_expenses.to_parquet('data/personal_expenses.parquet', index=False)


@app.post("/save_personal_expense/")
async def save_personal_expense(payload: ExpensePayload, token: str = Depends(oauth2_scheme)):
    user_data = decode_token(token)
    username = user_data.get("username")

    # Validate payload
    if not payload.username or not payload.amount:
        raise HTTPException(status_code=400, detail="Username and amount are required")

    # Save personal expense
    date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = {'username': [payload.username], 'date': [date], 'expenseName': [payload.expenseName],
            'amount': [payload.amount]}
    df = pd.DataFrame(data)

    file_path = 'data/personal_expenses.parquet'
    if os.path.exists(file_path):
        existing_table = pq.read_table(file_path)
        new_table = pa.Table.from_pandas(df)
        combined_table = pa.concat_tables([existing_table, new_table])
        pq.write_table(combined_table, file_path)
    else:
        df.to_parquet(file_path, index=False)

    return {"message": "Personal expense saved successfully"}


@app.post("/split_expense/")
async def split_expense(payload: SplitExpensePayload):
    amount = payload.amount
    friends = payload.friends
    added_by = payload.added_by

    # Validate friends, amount, and added_by
    if not friends:
        raise HTTPException(status_code=400, detail="List of friends is required")
    if amount <= 0:
        raise HTTPException(status_code=400, detail="Amount must be greater than zero")
    if not added_by:
        raise HTTPException(status_code=400, detail="Username of the user who added the expense is required")

    # Split expense among friends
    share = amount / len(friends)
    for friend in friends:
        # Save split expense for each friend
        data = {'username': [friend], 'date': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")], 'amount': [share],
                'added_by': [added_by]}
        df = pd.DataFrame(data)
        file_path = f'data/{added_by}_split_expenses_{friend}.parquet'
        if os.path.exists(file_path):
            table = pa.Table.from_pandas(df)
            existing_table = pq.read_table(file_path)
            updated_table = pa.concat_tables([existing_table, table])
            pq.write_table(updated_table, file_path)
        else:
            df.to_parquet(file_path, index=False)
    return {"message": "Expense split among friends successfully"}


def get_customer_by_username(username: str):
    df = pd.read_parquet('data/customer_details.parquet')
    customer_data = df[df['username'] == username]
    if customer_data.empty:
        return None
    customer = CustomerInDB(username=customer_data['username'].iloc[0],
                            hashed_password=customer_data['hashed_password'].iloc[0],
                            mobileNo=customer_data['mobileNo'].iloc[0], email=customer_data['email'].iloc[0],
                            fullName=customer_data['fullName'].iloc[0], userId=customer_data['userId'].iloc[0])

    return customer


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def hash_password(password):
    return pwd_context.hash(password)


@app.post("/signup/")
async def signup(customer: Customer):
    # Check if username already exists
    if username_exists(customer.username):
        return {"message": "Username already exists", "Response": "Failure"}

    # Check if phone number already exists
    if phone_number_exists(customer.mobileNo):
        return {"message": "Phone number already exists", "Response": "Failure"}

    # Generate a unique userId
    user_id = generate_user_id(customer.username)
    print(user_id)

    # Hash password
    hashed_password = hash_password(customer.password)

    # Create DataFrame with customer data
    data = {
        'userId': [user_id],
        'username': [customer.username],
        'hashed_password': [hashed_password],
        'email': [customer.email],
        'mobileNo': [customer.mobileNo],
        'fullName': [customer.fullName]
    }
    df = pd.DataFrame(data)

    file_path = 'data/customer_details.parquet'

    # Write data to Parquet file
    if os.path.exists(file_path):
        table = pa.Table.from_pandas(df)
        existing_table = pq.read_table(file_path)
        updated_table = pa.concat_tables([existing_table, table])
        try:
            pq.write_table(updated_table, file_path)
            # return {"message": "Customer signed up successfully", "Response": "Success"}
        except Exception as e:
            return {"message": "Not able to Sign Up", "Response": "Failure"}
    else:
        try:
            df.to_parquet(file_path)
            # return {"message": "Customer signed up successfully", "Response": "Success"}
        except Exception as e:
            return {"message": "Not able to Sign Up", "Response": "Failure"}

    # Create connections Parquet file for the user
    create_initially_connections_parquet(user_id, customer.username, customer.fullName)
    return {"message": "Customer signed up successfully", "Response": "Success"}


def generate_user_id(username):
    # Generate a random string of digits
    random_digits = ''.join(random.choices(string.digits, k=6))
    # Concatenate username with random digits
    user_id = f"{username}{random_digits}"
    return user_id


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


@app.post("/login/")
async def login(User: User):
    user = get_customer_by_username(User.username)
    if user is None:
        return {"message": "User not found", "Response": "Failure"}
    elif not verify_password(User.password, user.hashed_password):
        return {"message": "Invalid credentials", "Response": "Failure"}
    # return {"message": "Login successful"}
    # Generate JWT token
    user_data = {"userId": user.userId}
    access_token = create_access_token(user_data)

    # access_token = create_access_token(data={"sub": user.username})
    return {"access_token": access_token, "token_type": "bearer", "Response": "Success", "CustomerData":
        {"userId": user.userId, "username": user.username, "email": user.email, "mobileNo": user.mobileNo,
         "fullName": user.fullName}}


@app.get("/protected/")
async def protected_route():
    return {"message": "Hello, you've accessed a protected route!"}


# API to add a friend
@app.post("/add_friend/")
async def add_friend(payload: dict):
    # Check if payload is valid
    if "friendUsername" not in payload or "mobileNo" not in payload or "userId" not in payload or "friendUserId" not in payload:
        raise HTTPException(status_code=400, detail="Invalid payload")

    friendUsername = payload["friendUsername"]
    friendFullName = payload["fullName"]
    mobileNo = payload["mobileNo"]
    userId = payload["userId"]
    friendUserId = payload["friendUserId"]
    TotalSplittedAmount = payload.get("TotalSplittedAmount", 0)
    CalculatedMoney = payload.get("CalculatedMoney", 0)
    amountFromFriend = payload.get("amountFromFriend", 0)

    # Check if friend's username and mobile number exist
    if not username_exists(friendUsername):
        raise HTTPException(status_code=400, detail="Friend's username does not exist")
    if not phone_number_exists(mobileNo):
        raise HTTPException(status_code=400, detail="Mobile number does not exist")

    # Add friend to user's connections
    create_connections_parquet(userId)
    file_path = f'data/MyConnection_{userId}.parquet'
    df = pd.read_parquet(file_path)
    if friendUserId not in df['friendUserId'].values:
        # Create a new DataFrame with the friend data
        new_data = {'friendUsername': [friendUsername], 'friendUserId': [friendUserId], 'expenseAddedBy': [userId],
                    'TotalSplittedAmount': [TotalSplittedAmount], 'amountFromFriend': [amountFromFriend],
                    'CalculatedMoney': [CalculatedMoney], 'fullName': [friendFullName]}
        new_df = pd.DataFrame(new_data)

        # Concatenate the new DataFrame with the existing DataFrame
        df = pd.concat([df, new_df], ignore_index=True)
        df.to_parquet(file_path, index=False)

    return {"message": f"Friend '{friendUsername}' added successfully"}


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


# Function to create Parquet file for user's connections
def create_initially_connections_parquet(userId: str, username: str, fullName: str):
    file_path = f'data/MyConnection_{userId}.parquet'
    df = pd.DataFrame(
        columns=['friendUsername', 'friendUserId', 'expenseAddedBy', 'TotalSplittedAmount', 'amountFromFriend',
                 'CalculatedMoney'])

    df.loc[0] = [username, userId, username, 0, 0, 0]  # Add user's own details as the first entry
    df.to_parquet(file_path, index=False)


# Function to create parquet file for user's connections
def create_connections_parquet(userId: str):
    file_path = f'data/MyConnection_{userId}.parquet'
    if not os.path.exists(file_path):
        df = pd.DataFrame(
            columns=['fullName', 'friendUsername', 'friendUserId', 'expenseAddedBy', 'TotalSplittedAmount',
                     'amountFromFriend', 'CalculatedMoney'])
        df.to_parquet(file_path, index=False)


# API to add an expense
@app.post("/add_expense/")
async def add_expense(payload: dict):
    # Check if payload is valid
    if "userId" not in payload or "TotalSplittedAmount" not in payload or "FriendsBetweenSplitting" not in payload:
        raise HTTPException(status_code=400, detail="Invalid payload")

    added_by = payload["userId"]
    expense_amount = payload["TotalSplittedAmount"]
    friends = payload["FriendsBetweenSplitting"]

    # Check if friend's username exists in user's connections
    file_path = f'data/MyConnection_{added_by}.parquet'
    if os.path.exists(file_path):
        # Update expense for each friend
        for friend_userId in friends:
            try:
                expense_amount = float(expense_amount)
                update_friend_expense(added_by, friend_userId, expense_amount / len(friends))
            except ValueError:
                print("Expense amount is not a valid number")

        return {"message": "Expense added successfully", "Response": "Success"}
    else:
        return {"message": "You haven't added any friends yet", "Response": "Failure"}


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


# @app.post("/get_friend_expenses/")
# async def get_friend_expenses(payload: dict):
#     userId = payload.get("userId")
#     if not userId:
#         return {"message": "User ID is required in the payload", "Response": "Failure"}
#
#     file_path = f'data/MyConnection_{userId}.parquet'
#     if os.path.exists(file_path):
#         df = pd.read_parquet(file_path)
#
#         CustomerData = df[
#             ['friendUsername', 'friendUserId', 'expenseAddedBy', 'TotalSplittedAmount', 'amountFromFriend']].to_dict(
#             orient='records')
#         return {"customerData": CustomerData, "Response": "Success"}
#     else:
#         return {"message": "No expenses found for the user", "Response": "Failure"}

@app.post("/get_friend_expenses/")
async def get_friend_expenses(payload: dict):
    userId = payload.get("userId")
    if not userId:
        return {"message": "User ID is required in the payload", "Response": "Failure"}

    # Load MyConnection_{userId}.parquet
    file_path = f'data/MyConnection_{userId}.parquet'
    if os.path.exists(file_path):
        df = pd.read_parquet(file_path)

        # Load customer details
        customer_details_df = pd.read_parquet('data/customer_details.parquet')

        # Merge dataframes on 'friendUserId'
        merged_df = pd.merge(df, customer_details_df, left_on='friendUserId', right_on='userId', how='left')

        # Select required columns and convert to dictionary
        CustomerData = merged_df[
            ['friendUsername', 'friendUserId', 'expenseAddedBy', 'TotalSplittedAmount', 'amountFromFriend', 'email',
             'fullName', 'mobileNo']].to_dict(orient='records')

        return {"customerData": CustomerData, "Response": "Success"}
    else:
        return {"message": "No expenses found for the user", "Response": "Failure"}


@app.post("/settle_amount/")
async def settle_amount(payload: dict):
    userId = payload.get("userId")
    friendUserId = payload.get("friendUserId")
    if not userId or not friendUserId:
        return {"message": "User ID and friend user Id are required in the payload", "Response": "Failure"}

    file_path = f'data/MyConnection_{userId}.parquet'
    if os.path.exists(file_path):
        df = pd.read_parquet(file_path)
        if friendUserId in df['friendUserId'].values:
            # Settle the amount to 0
            df.loc[df['friendUserId'] == friendUserId, 'TotalSplittedAmount'] = 0
            df.loc[df['friendUserId'] == friendUserId, 'amountFromFriend'] = 0
            df.loc[df['friendUserId'] == friendUserId, 'CalculatedMoney'] = 0
            df.to_parquet(file_path, index=False)
            return {"message": f"Amount settled with friend '{friendUserId}' successfully"}
        else:
            return {"message": f"Friend '{friendUserId}' not found in the user's connections"}
    else:
        return {"message": "No expenses found for the user"}


# # API to get all users with friendship status
# @app.post("/get_all_users/")
# async def get_all_users(request_payload: dict):
#     # Check if user ID exists
#     userId = request_payload.get("userId")
#     print(userId)
#     if not os.path.exists(f'data/customer_details.parquet'):
#         raise HTTPException(status_code=404, detail="User ID not found")
#
#     # Read customer details
#     customer_details_df = pd.read_parquet('data/customer_details.parquet')
#
#     # Check if user has any connections
#     connections_file_path = f'data/MyConnection_{userId}.parquet'
#     if os.path.exists(connections_file_path):
#         connections_df = pd.read_parquet(connections_file_path)
#         existing_friends = set(connections_df['friendUserId'])
#     else:
#         existing_friends = set()
#
#     # Check if user has any pending requests
#     pending_requests_file_path = 'data/PendingRequests.parquet'
#     if os.path.exists(pending_requests_file_path):
#         pending_requests_df = pd.read_parquet(pending_requests_file_path)
#         requested_users = set(pending_requests_df[pending_requests_df['requestedUserId'] == userId]['requesterUserId'])
#         requester_users = set(pending_requests_df[pending_requests_df['requesterUserId'] == userId]['requestedUserId'])
#         pending_requests_users = requested_users.union(requester_users)
#     else:
#         pending_requests_users = set()
#
#     # Prepare response
#     users = []
#     for index, row in customer_details_df.iterrows():
#         print(customer_details_df)
#         user_id = row['userId']
#         username = row['username']
#         mobileNo = row['mobileNo']
#         fullName = row['fullName']
#         email = row['email']
#         friendship_status = 'not_friend'
#
#         if user_id in existing_friends:
#             friendship_status = 'added_friend'
#         elif user_id in pending_requests_users:
#             friendship_status = 'requested_friend'
#
#         user_info = {
#             "userId": user_id,
#             "username": username,
#             "fullName": fullName,
#             "mobileNo": mobileNo,
#             "email": email,
#             "friendshipStatus": friendship_status
#         }
#         users.append(user_info)
#
#     return users


# API to get all users with friendship status
@app.post("/get_all_users/")
async def get_all_users(request_payload: dict):
    # Check if user ID exists
    userId = request_payload.get("userId")
    if not os.path.exists(f'data/customer_details.parquet'):
        raise HTTPException(status_code=404, detail="User ID not found")

    # Read customer details
    customer_details_df = pd.read_parquet('data/customer_details.parquet')

    # Check if user has any connections
    connections_file_path = f'data/MyConnection_{userId}.parquet'
    if os.path.exists(connections_file_path):
        connections_df = pd.read_parquet(connections_file_path)
        existing_friends = set(connections_df['friendUserId'])
    else:
        existing_friends = set()

    # Check if user has any pending requests
    pending_requests_file_path = 'data/PendingRequests.parquet'
    if os.path.exists(pending_requests_file_path):
        pending_requests_df = pd.read_parquet(pending_requests_file_path)
        requested_users = set(pending_requests_df[pending_requests_df['requestedUserId'] == userId]['requesterUserId'])
        requester_users = set(pending_requests_df[pending_requests_df['requesterUserId'] == userId]['requestedUserId'])
        pending_requests_users = requested_users.union(requester_users)
    else:
        pending_requests_users = set()

    # Prepare response
    users = []
    for index, row in customer_details_df.iterrows():
        user_id = row['userId']
        username = row['username']
        mobileNo = row['mobileNo']
        fullName = row['fullName']
        email = row['email']
        friendship_status = 'not_friend'

        if user_id in existing_friends:
            friendship_status = 'added_friend'
        elif (user_id in
              pending_requests_users):
            if user_id in requested_users:
                friendship_status = 'pending_request'
            else:
                friendship_status = 'requested_friend'

        user_info = {
            "userId": user_id,
            "username": username,
            "fullName": fullName,
            "mobileNo": mobileNo,
            "email": email,
            "friendshipStatus": friendship_status
        }
        users.append(user_info)

    return users


@app.post("/send_friend_request/")
async def send_friend_request(request_payload: dict):
    requesterUserId = request_payload.get("requesterUserId")
    requestedUserId = request_payload.get("requestedUserId")

    # Check if both requester and requested user IDs exist
    if not os.path.exists(f'data/MyConnection_{requesterUserId}.parquet') or \
            not os.path.exists(f'data/MyConnection_{requestedUserId}.parquet'):
        return {"message": "User ID not found", "Response": "Failure"}

    # Create Parquet file for pending requests if not exists
    create_pending_requests_parquet()

    # Add the friend request to the pending requests Parquet file
    pending_requests_df = pd.read_parquet('data/PendingRequests.parquet')
    new_request_df = pd.DataFrame({'requesterUserId': [requesterUserId], 'requestedUserId': [requestedUserId]})
    pending_requests_df = pd.concat([pending_requests_df, new_request_df], ignore_index=True)
    pending_requests_df.to_parquet('data/PendingRequests.parquet', index=False)

    return {"message": "Friend request sent successfully"}


# Function to create parquet file for pending friend requests
def create_pending_requests_parquet():
    file_path = 'data/PendingRequests.parquet'
    if not os.path.exists(file_path):
        df = pd.DataFrame(columns=['requesterUserId', 'requestedUserId'])
        df.to_parquet(file_path, index=False)


# API to accept a friend request
@app.post("/accept_friend_request/")
async def accept_friend_request(accept_payload: dict):
    requesterUserId = accept_payload.get("requesterUserId")
    requestedUserId = accept_payload.get("requestedUserId")

    # Check if both requester and requested user IDs exist
    if not os.path.exists(f'data/MyConnection_{requesterUserId}.parquet') or \
            not os.path.exists(f'data/MyConnection_{requestedUserId}.parquet'):
        return {"message": "User ID not found", "Response": "Failure"}

    # Create Parquet file for pending requests if not exists
    create_pending_requests_parquet()

    # Check if there's a pending friend request
    pending_requests_df = pd.read_parquet('data/PendingRequests.parquet')
    print(pending_requests_df.to_string())
    request_exists = (pending_requests_df['requesterUserId'] == requesterUserId) & \
                     (pending_requests_df['requestedUserId'] == requestedUserId)
    print(pending_requests_df)
    print(requesterUserId)
    print(requestedUserId)
    if request_exists.any():
        # Add friend to user's connections
        create_connections_parquet(requestedUserId)
        create_connections_parquet(requesterUserId)
        requesterDetails = get_user_details(requesterUserId)
        requestedDetails = get_user_details(requestedUserId)

        add_friend_to_connections(requestedDetails, requesterUserId)
        add_friend_to_connections(requesterDetails, requestedUserId)

        # Remove the accepted request from pending requests
        pending_requests_df = pending_requests_df[~request_exists]
        pending_requests_df.to_parquet('data/PendingRequests.parquet', index=False)

        return {"message": "Friend request accepted successfully"}
    else:
        raise HTTPException(status_code=404, detail="Friend request not found")


def add_friend_to_connections(data, userid):
    file_path = f'data/MyConnection_{userid}.parquet'
    df = pd.read_parquet(file_path)
    print(df)
    print(data)
    print(userid)
    print(data.get("userId"))
    if data.get("userId") not in df['friendUserId'].values:
        # new_data = {'friendUsername': [data.username], 'friendUserId': [data.userId], 'expenseAddedBy': [userid],
        #             'TotalSplittedAmount': [0], 'amountFromFriend': [0], 'CalculatedMoney': [0]}
        new_data = {'friendUsername': [data.get("username")], 'friendUserId': [data.get("userId")],
                    'expenseAddedBy': [userid],
                    'TotalSplittedAmount': [0], 'amountFromFriend': [0], 'CalculatedMoney': [0]}
        new_df = pd.DataFrame(new_data)
        df = pd.concat([df, new_df], ignore_index=True)
        df.to_parquet(file_path, index=False)


if __name__ == "__main__":
    # uvicorn.run(app, host="0.0.0.0", port=8080)
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), reload=True)
