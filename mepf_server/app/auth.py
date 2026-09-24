# -*- coding: utf-8 -*-
import os
import hmac
from fastapi import Header, HTTPException

API_KEY_ENV = "MEPF_API_KEY"

def require_api_key(x_api_key: str = Header(default="")):
    expected = os.environ.get(API_KEY_ENV, "").strip()
    if not expected:
        raise HTTPException(status_code=503, detail="Server API key is not configured (MEPF_API_KEY).")
    if not x_api_key or not hmac.compare_digest(x_api_key, expected):
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True
