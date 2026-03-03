# application.py (repo root)
import os
import sys

print("[BOOT] application.py starting")
print("[BOOT] cwd:", os.getcwd())
print("[BOOT] sys.path[0:5]:", sys.path[:5])

# Prefer importing the WSGI callable named "application"
from dash_app.app import application  # must succeed

print("[BOOT] imported dash_app.app OK; application =", application)
