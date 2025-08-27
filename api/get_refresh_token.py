# api/get_refresh_token.py
import os
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube.readonly",
]

CLIENT_ID = os.getenv("YT_CLIENT_ID") or input("YT_CLIENT_ID: ").strip()
CLIENT_SECRET = os.getenv("YT_CLIENT_SECRET") or input("YT_CLIENT_SECRET: ").strip()

client_config = {
    "installed": {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "redirect_uris": ["http://localhost"],
    }
}

flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
creds = flow.run_local_server(port=0, access_type="offline", prompt="consent", include_granted_scopes=True)

print("\n=== COPY THIS REFRESH TOKEN ===")
print(creds.refresh_token or "(no refresh token returned)")
print("================================\n")
