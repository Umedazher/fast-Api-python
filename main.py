from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import pandas as pd
import uvicorn as uvicorn
import pyarrow as pa
import pyarrow.parquet as pq
import os
from starlette.middleware.cors import CORSMiddleware
from authentication.auth import oauth2_scheme
from authentication.jwt import create_access_token, decode_token
from authentication.password import verify_password, hash_password
from commonBo import save_expense_history, get_customer_by_username, username_exists, phone_number_exists, \
    get_user_details, create_pending_requests_parquet, add_friend_to_connections, generate_user_id, \
    create_connections_parquet, create_initially_connections_parquet, update_friend_expense
from models.user import User, ExpensePayload, SplitExpensePayload, Customer, CustomerInDB
from datetime import datetime
from typing import List

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
async def add_friend(payload: dict, token: str = Depends(oauth2_scheme)):
    user_data = decode_token(token)
    username = user_data.get("username")

    # Check if payload is valid
    if "friendUsername" not in payload or "mobileNo" not in payload or "userId" not in payload or "friendUserId" not in payload:
        return {"message": "Invalid payload", "Response": "Failure"}

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
        return {"message": "Friend's username does not exist", "Response": "Failure"}
    if not phone_number_exists(mobileNo):
        return {"message": "Mobile number does not exist", "Response": "Failure"}

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





# API to add an expense
@app.post("/add_expense/")
async def add_expense(payload: dict, token: str = Depends(oauth2_scheme)):
    user_data = decode_token(token)
    username = user_data.get("username")

    # Check if payload is valid
    if "userId" not in payload or "TotalSplittedAmount" not in payload or "FriendsBetweenSplitting" not in payload:
        return {"message": "Invalid payload", "Response": "Failure"}

    added_by = payload["userId"]
    expense_amount = payload["TotalSplittedAmount"]
    friends = payload["FriendsBetweenSplitting"]

    await save_expense_history(payload)
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




@app.post("/get_friend_expenses/")
async def get_friend_expenses(payload: dict, token: str = Depends(oauth2_scheme)):
    user_data = decode_token(token)
    username = user_data.get("username")

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
async def settle_amount(payload: dict, token: str = Depends(oauth2_scheme)):
    user_data = decode_token(token)
    username = user_data.get("username")

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
            return {"message": f"Amount settled with friend '{friendUserId}' successfully", "Response": "Success"}
        else:
            return {"message": f"Friend '{friendUserId}' not found in the user's connections", "Response": "Failure"}
    else:
        return {"message": "No expenses found for the user", "Response": "Failure"}


# API to get all users with friendship status
@app.post("/get_all_users/")
async def get_all_users(request_payload: dict, token: str = Depends(oauth2_scheme)):
    user_data = decode_token(token)
    username = user_data.get("username")
    # Check if user ID exists
    userId = request_payload.get("userId")
    if not os.path.exists(f'data/customer_details.parquet'):
        return {"message": "User ID not found", "Response": "Failure"}

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
async def send_friend_request(request_payload: dict, token: str = Depends(oauth2_scheme)):
    user_data = decode_token(token)
    username = user_data.get("username")

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

    return {"message": "Friend request sent successfully","Response": "Success"}


# API to accept a friend request
@app.post("/accept_friend_request/")
async def accept_friend_request(accept_payload: dict, token: str = Depends(oauth2_scheme)):
    user_data = decode_token(token)
    username = user_data.get("username")

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
        return {"message": "Friend request not found", "Response": "Failure"}


@app.post("/My_Spends/")
async def get_expense_history(payload: dict):
    userId = payload.get("userId")
    file_path = 'data/expense_history.parquet'
    if not os.path.exists(file_path):
        return {"message": "Expense history data not found", "Response": "Failure"}

    df = pd.read_parquet(file_path)
    user_expenses = df[df['SavingUser'] == userId]

    if user_expenses.empty:
        return {"message": "Expense history data not found", "Response": "Failure"}

    return user_expenses.to_dict(orient='records')

@app.post("/expense_history/")
async def get_expense_history(payload: dict):
    userId = payload.get("userId")
    file_path = 'data/expense_history.parquet'
    if not os.path.exists(file_path):
        return {"message": "Expense history data not found", "Response": "Failure"}

    df = pd.read_parquet(file_path)
    user_expenses = df[df['Friends'] == userId]

    if user_expenses.empty:
        return {"message": "Expense history data not found", "Response": "Failure"}

    return user_expenses.to_dict(orient='records')


# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8080)
#     # uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), reload=True)
