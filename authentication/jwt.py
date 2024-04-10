# authentication/jwt.py
import jwt
from jwt import PyJWTError
from datetime import datetime,timedelta
from fastapi import Depends, HTTPException, status

SECRET_KEY = "mytracker#123@84"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 2

# def create_access_token(data: dict):
#     return jwt.encode(data, SECRET_KEY, algorithm=ALGORITHM)
def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# Function to decode JWT token
def decode_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        print(payload)

        return payload
    except PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )