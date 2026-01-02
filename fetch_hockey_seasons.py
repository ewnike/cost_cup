import requests
import pandas as pd

URL = "https://evolving-hockey.com/stats/pbp_query/session/853e43fa250088d4888c629c15aea8b9/dataobj/pbp_query_table_output?w=&nonce=88e8ca3ee2cb01a4"


headers = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://evolving-hockey.com",
    "Referer": "https://evolving-hockey.com/stats/pbp_query/?_inputs_&dir_pbp_query=%22PBP%20Past%20Games%22",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
    "Cookie": "__utmz=...; PHPSESSID=...; ...",  # from the *same* curl as this URL
}

# PASTE YOUR FULL BODY HERE, UNTOUCHED, INCLUDING start=0&length=100
BASE_PAYLOAD = (
    # "draw=1&columns%5B0%5D%5Bdata%5D=0&columns%5B0%5D%5Bname%5D=season&..."
    # literally every columns[...] param...
    "draw=1&columns%5B0%5D%5Bdata%5D=0&columns%5B0%5D%5Bname%5D=season&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=true&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=1&columns%5B1%5D%5Bname%5D=game_id&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=true&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=2&columns%5B2%5D%5Bname%5D=game_date&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=true&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=3&columns%5B3%5D%5Bname%5D=session&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=true&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=4&columns%5B4%5D%5Bname%5D=event_index&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=true&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=5&columns%5B5%5D%5Bname%5D=game_period&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=true&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B6%5D%5Bdata%5D=6&columns%5B6%5D%5Bname%5D=game_seconds&columns%5B6%5D%5Bsearchable%5D=true&columns%5B6%5D%5Borderable%5D=true&columns%5B6%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B6%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B7%5D%5Bdata%5D=7&columns%5B7%5D%5Bname%5D=clock_time&columns%5B7%5D%5Bsearchable%5D=true&columns%5B7%5D%5Borderable%5D=true&columns%5B7%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B7%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B8%5D%5Bdata%5D=8&columns%5B8%5D%5Bname%5D=event_type&columns%5B8%5D%5Bsearchable%5D=true&columns%5B8%5D%5Borderable%5D=true&columns%5B8%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B8%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B9%5D%5Bdata%5D=9&columns%5B9%5D%5Bname%5D=event_description&columns%5B9%5D%5Bsearchable%5D=true&columns%5B9%5D%5Borderable%5D=true&columns%5B9%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B9%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B10%5D%5Bdata%5D=10&columns%5B10%5D%5Bname%5D=event_detail&columns%5B10%5D%5Bsearchable%5D=true&columns%5B10%5D%5Borderable%5D=true&columns%5B10%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B10%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B11%5D%5Bdata%5D=11&columns%5B11%5D%5Bname%5D=event_zone&columns%5B11%5D%5Bsearchable%5D=true&columns%5B11%5D%5Borderable%5D=true&columns%5B11%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B11%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B12%5D%5Bdata%5D=12&columns%5B12%5D%5Bname%5D=event_team&columns%5B12%5D%5Bsearchable%5D=true&columns%5B12%5D%5Borderable%5D=true&columns%5B12%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B12%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B13%5D%5Bdata%5D=13&columns%5B13%5D%5Bname%5D=event_player_1&columns%5B13%5D%5Bsearchable%5D=true&columns%5B13%5D%5Borderable%5D=true&columns%5B13%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B13%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B14%5D%5Bdata%5D=14&columns%5B14%5D%5Bname%5D=event_player_2&columns%5B14%5D%5Bsearchable%5D=true&columns%5B14%5D%5Borderable%5D=true&columns%5B14%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B14%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B15%5D%5Bdata%5D=15&columns%5B15%5D%5Bname%5D=event_player_3&columns%5B15%5D%5Bsearchable%5D=true&columns%5B15%5D%5Borderable%5D=true&columns%5B15%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B15%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B16%5D%5Bdata%5D=16&columns%5B16%5D%5Bname%5D=event_length&columns%5B16%5D%5Bsearchable%5D=true&columns%5B16%5D%5Borderable%5D=true&columns%5B16%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B16%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B17%5D%5Bdata%5D=17&columns%5B17%5D%5Bname%5D=coords_x&columns%5B17%5D%5Bsearchable%5D=true&columns%5B17%5D%5Borderable%5D=true&columns%5B17%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B17%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B18%5D%5Bdata%5D=18&columns%5B18%5D%5Bname%5D=coords_y&columns%5B18%5D%5Bsearchable%5D=true&columns%5B18%5D%5Borderable%5D=true&columns%5B18%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B18%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B19%5D%5Bdata%5D=19&columns%5B19%5D%5Bname%5D=num_on&columns%5B19%5D%5Bsearchable%5D=true&columns%5B19%5D%5Borderable%5D=true&columns%5B19%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B19%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B20%5D%5Bdata%5D=20&columns%5B20%5D%5Bname%5D=num_off&columns%5B20%5D%5Bsearchable%5D=true&columns%5B20%5D%5Borderable%5D=true&columns%5B20%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B20%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B21%5D%5Bdata%5D=21&columns%5B21%5D%5Bname%5D=players_on&columns%5B21%5D%5Bsearchable%5D=true&columns%5B21%5D%5Borderable%5D=true&columns%5B21%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B21%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B22%5D%5Bdata%5D=22&columns%5B22%5D%5Bname%5D=players_off&columns%5B22%5D%5Bsearchable%5D=true&columns%5B22%5D%5Borderable%5D=true&columns%5B22%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B22%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B23%5D%5Bdata%5D=23&columns%5B23%5D%5Bname%5D=home_on_1&columns%5B23%5D%5Bsearchable%5D=true&columns%5B23%5D%5Borderable%5D=true&columns%5B23%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B23%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B24%5D%5Bdata%5D=24&columns%5B24%5D%5Bname%5D=home_on_2&columns%5B24%5D%5Bsearchable%5D=true&columns%5B24%5D%5Borderable%5D=true&columns%5B24%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B24%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B25%5D%5Bdata%5D=25&columns%5B25%5D%5Bname%5D=home_on_3&columns%5B25%5D%5Bsearchable%5D=true&columns%5B25%5D%5Borderable%5D=true&columns%5B25%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B25%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B26%5D%5Bdata%5D=26&columns%5B26%5D%5Bname%5D=home_on_4&columns%5B26%5D%5Bsearchable%5D=true&columns%5B26%5D%5Borderable%5D=true&columns%5B26%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B26%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B27%5D%5Bdata%5D=27&columns%5B27%5D%5Bname%5D=home_on_5&columns%5B27%5D%5Bsearchable%5D=true&columns%5B27%5D%5Borderable%5D=true&columns%5B27%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B27%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B28%5D%5Bdata%5D=28&columns%5B28%5D%5Bname%5D=home_on_6&columns%5B28%5D%5Bsearchable%5D=true&columns%5B28%5D%5Borderable%5D=true&columns%5B28%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B28%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B29%5D%5Bdata%5D=29&columns%5B29%5D%5Bname%5D=home_on_7&columns%5B29%5D%5Bsearchable%5D=true&columns%5B29%5D%5Borderable%5D=true&columns%5B29%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B29%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B30%5D%5Bdata%5D=30&columns%5B30%5D%5Bname%5D=away_on_1&columns%5B30%5D%5Bsearchable%5D=true&columns%5B30%5D%5Borderable%5D=true&columns%5B30%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B30%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B31%5D%5Bdata%5D=31&columns%5B31%5D%5Bname%5D=away_on_2&columns%5B31%5D%5Bsearchable%5D=true&columns%5B31%5D%5Borderable%5D=true&columns%5B31%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B31%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B32%5D%5Bdata%5D=32&columns%5B32%5D%5Bname%5D=away_on_3&columns%5B32%5D%5Bsearchable%5D=true&columns%5B32%5D%5Borderable%5D=true&columns%5B32%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B32%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B33%5D%5Bdata%5D=33&columns%5B33%5D%5Bname%5D=away_on_4&columns%5B33%5D%5Bsearchable%5D=true&columns%5B33%5D%5Borderable%5D=true&columns%5B33%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B33%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B34%5D%5Bdata%5D=34&columns%5B34%5D%5Bname%5D=away_on_5&columns%5B34%5D%5Bsearchable%5D=true&columns%5B34%5D%5Borderable%5D=true&columns%5B34%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B34%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B35%5D%5Bdata%5D=35&columns%5B35%5D%5Bname%5D=away_on_6&columns%5B35%5D%5Bsearchable%5D=true&columns%5B35%5D%5Borderable%5D=true&columns%5B35%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B35%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B36%5D%5Bdata%5D=36&columns%5B36%5D%5Bname%5D=away_on_7&columns%5B36%5D%5Bsearchable%5D=true&columns%5B36%5D%5Borderable%5D=true&columns%5B36%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B36%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B37%5D%5Bdata%5D=37&columns%5B37%5D%5Bname%5D=home_goalie&columns%5B37%5D%5Bsearchable%5D=true&columns%5B37%5D%5Borderable%5D=true&columns%5B37%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B37%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B38%5D%5Bdata%5D=38&columns%5B38%5D%5Bname%5D=away_goalie&columns%5B38%5D%5Bsearchable%5D=true&columns%5B38%5D%5Borderable%5D=true&columns%5B38%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B38%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B39%5D%5Bdata%5D=39&columns%5B39%5D%5Bname%5D=home_team&columns%5B39%5D%5Bsearchable%5D=true&columns%5B39%5D%5Borderable%5D=true&columns%5B39%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B39%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B40%5D%5Bdata%5D=40&columns%5B40%5D%5Bname%5D=away_team&columns%5B40%5D%5Bsearchable%5D=true&columns%5B40%5D%5Borderable%5D=true&columns%5B40%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B40%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B41%5D%5Bdata%5D=41&columns%5B41%5D%5Bname%5D=home_skaters&columns%5B41%5D%5Bsearchable%5D=true&columns%5B41%5D%5Borderable%5D=true&columns%5B41%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B41%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B42%5D%5Bdata%5D=42&columns%5B42%5D%5Bname%5D=away_skaters&columns%5B42%5D%5Bsearchable%5D=true&columns%5B42%5D%5Borderable%5D=true&columns%5B42%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B42%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B43%5D%5Bdata%5D=43&columns%5B43%5D%5Bname%5D=home_score&columns%5B43%5D%5Bsearchable%5D=true&columns%5B43%5D%5Borderable%5D=true&columns%5B43%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B43%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B44%5D%5Bdata%5D=44&columns%5B44%5D%5Bname%5D=away_score&columns%5B44%5D%5Bsearchable%5D=true&columns%5B44%5D%5Borderable%5D=true&columns%5B44%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B44%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B45%5D%5Bdata%5D=45&columns%5B45%5D%5Bname%5D=game_score_state&columns%5B45%5D%5Bsearchable%5D=true&columns%5B45%5D%5Borderable%5D=true&columns%5B45%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B45%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B46%5D%5Bdata%5D=46&columns%5B46%5D%5Bname%5D=game_strength_state&columns%5B46%5D%5Bsearchable%5D=true&columns%5B46%5D%5Borderable%5D=true&columns%5B46%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B46%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B47%5D%5Bdata%5D=47&columns%5B47%5D%5Bname%5D=home_zone&columns%5B47%5D%5Bsearchable%5D=true&columns%5B47%5D%5Borderable%5D=true&columns%5B47%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B47%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B48%5D%5Bdata%5D=48&columns%5B48%5D%5Bname%5D=pbp_distance&columns%5B48%5D%5Bsearchable%5D=true&columns%5B48%5D%5Borderable%5D=true&columns%5B48%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B48%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B49%5D%5Bdata%5D=49&columns%5B49%5D%5Bname%5D=event_distance&columns%5B49%5D%5Bsearchable%5D=true&columns%5B49%5D%5Borderable%5D=true&columns%5B49%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B49%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B50%5D%5Bdata%5D=50&columns%5B50%5D%5Bname%5D=event_angle&columns%5B50%5D%5Bsearchable%5D=true&columns%5B50%5D%5Borderable%5D=true&columns%5B50%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B50%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B51%5D%5Bdata%5D=51&columns%5B51%5D%5Bname%5D=home_zonestart&columns%5B51%5D%5Bsearchable%5D=true&columns%5B51%5D%5Borderable%5D=true&columns%5B51%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B51%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B52%5D%5Bdata%5D=52&columns%5B52%5D%5Bname%5D=face_index&columns%5B52%5D%5Bsearchable%5D=true&columns%5B52%5D%5Borderable%5D=true&columns%5B52%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B52%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B53%5D%5Bdata%5D=53&columns%5B53%5D%5Bname%5D=pen_index&columns%5B53%5D%5Bsearchable%5D=true&columns%5B53%5D%5Borderable%5D=true&columns%5B53%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B53%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B54%5D%5Bdata%5D=54&columns%5B54%5D%5Bname%5D=shift_index&columns%5B54%5D%5Bsearchable%5D=true&columns%5B54%5D%5Borderable%5D=true&columns%5B54%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B54%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B55%5D%5Bdata%5D=55&columns%5B55%5D%5Bname%5D=pred_goal&columns%5B55%5D%5Bsearchable%5D=true&columns%5B55%5D%5Borderable%5D=true&columns%5B55%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B55%5D%5Bsearch%5D%5Bregex%5D=false&start=0&length=100&search%5Bvalue%5D=&search%5Bregex%5D=false&search%5BcaseInsensitive%5D=true&search%5Bsmart%5D=true&escape=false"
    # "&columns%5B55%5D%5Bdata%5D=55&columns%5B55%5D%5Bname%5D=pred_goal"
    # "&columns%5B55%5D%5Bsearchable%5D=true&columns%5B55%5D%5Borderable%5D=true"
    # "&columns%5B55%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B55%5D%5Bsearch%5D%5Bregex%5D=false"
    # "&start=0&length=100&search%5Bvalue%5D=&search%5Bregex%5D=false"
    # "&search%5BcaseInsensitive%5D=true&search%5Bsmart%5D=true&escape=false"
)


def make_payload(start: int, length: int) -> str:
    # Just mutate the start/length substring; do NOT parse/re-encode
    return BASE_PAYLOAD.replace("start=0&length=100", f"start={start}&length={length}")


# def make_payload(start: int, length: int) -> str:
#     # Make sure this matches the exact substring in BASE_PAYLOAD
#     return BASE_PAYLOAD.replace("start=0&length=50", f"start={start}&length={length}")


columns = [
    "season",
    "game_id",
    "game_date",
    "session",
    "event_index",
    "game_period",
    "game_seconds",
    "clock_time",
    "event_type",
    "event_description",
    "event_detail",
    "event_zone",
    "event_team",
    "event_player_1",
    "event_player_2",
    "event_player_3",
    "event_length",
    "coords_x",
    "coords_y",
    "num_on",
    "num_off",
    "players_on",
    "players_off",
    "home_on_1",
    "home_on_2",
    "home_on_3",
    "home_on_4",
    "home_on_5",
    "home_on_6",
    "home_on_7",
    "away_on_1",
    "away_on_2",
    "away_on_3",
    "away_on_4",
    "away_on_5",
    "away_on_6",
    "away_on_7",
    "home_goalie",
    "away_goalie",
    "home_team",
    "away_team",
    "home_skaters",
    "away_skaters",
    "home_score",
    "away_score",
    "game_score_state",
    "game_strength_state",
    "home_zone",
    "pbp_distance",
    "event_distance",
    "event_angle",
    "home_zonestart",
    "face_index",
    "pen_index",
    "shift_index",
    "pred_goal",
]

# columns = [
#     "row_num",
#     "player",
#     "team_num",
#     "position",
#     "game_id",
#     "game_date",
#     "season",
#     "session",
#     "team",
#     "opponent",
#     "is_home",
#     "game_period",
#     "shift_num",
#     "seconds_start",
#     "seconds_end",
#     "seconds_duration",
#     "shift_start",
#     "shift_end",
#     "duration",
#     "shift_mod",
# ]

all_rows = []
page_size = 100
start = 0

while True:
    payload = make_payload(start, page_size)
    print(f"Requesting rows starting at {start}…")

    resp = requests.post(URL, headers=headers, data=payload)
    resp.raise_for_status()
    js = resp.json()
    print("JSON keys:", list(js.keys()))

    if "error" in js:
        print("Server reported error:", js["error"])
        raise SystemExit("Server-side error – usually malformed payload.")

    # DataTables-style response check
    if not {"draw", "recordsTotal", "recordsFiltered", "data"} <= set(js.keys()):
        print("Unexpected JSON structure:", js)
        raise SystemExit("Not the PBP data endpoint. Check URL/nonce/cookies.")

    print("recordsTotal:", js["recordsTotal"], "recordsFiltered:", js["recordsFiltered"])
    rows = js["data"]
    print("  got", len(rows), "rows")

    if not rows:
        break

    all_rows.extend(rows)
    start += page_size

    if start >= js["recordsTotal"]:
        break

print("Total rows collected:", len(all_rows))

df = pd.DataFrame(all_rows, columns=columns)
print("Final DataFrame rows:", len(df))
df.to_csv("pbp_20242025C.csv", index=False)
