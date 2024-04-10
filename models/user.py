# models/user.py
from pydantic import BaseModel
from typing import List
class User(BaseModel):
    username: str
    password: str


class Customer(BaseModel):
    username: str
    password: str
    email: str
    mobileNo:str
    fullName:str

class ExpensePayload(BaseModel):
    username: str
    amount: float
    expenseName:str

class SplitExpensePayload(BaseModel):
    amount: float
    friends: List[str]
    added_by: str

class CustomerInDB(BaseModel):
    username: str
    userId:str
    hashed_password: str
    email: str
    mobileNo: str
    fullName: str