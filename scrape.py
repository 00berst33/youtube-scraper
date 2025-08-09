import googleapiclient.discovery
import googleapiclient.errors
import time
import pandas as pd
import sqlite3

api_key = "type_API_key_here"


def search_videos(query, max_results=50, num_iter=1, search_id="vaccine_videos"):
    # Create YouTube API client
    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)

    # Initialize variable for page token
    next_page_token = None

    # Initialize list to store video info
    video_info = []

    while True:
        try:
            # Search for videos of interest
            search = youtube.search().list(
                part="snippet",
                videoCategoryId="27", # 27 education, 28 science and technology,
                maxResults=max_results,
                pageToken=next_page_token,
                publishedAfter="2017-01-01T00:00:00.0Z",
                publishedBefore="2024-01-01T00:00:00.0Z",
                q=query,
                relevanceLanguage="en",
                type="video"
            ).execute()

            # Get video IDs for relevant videos
            video_ids = [item["id"]["videoId"] for item in search["items"]]

            # Iterate over each video found from the search
            for video_id in video_ids:
                # Get current video's data of interest
                video_stats = youtube.videos().list(
                    part="snippet, statistics",
                    id=video_id
                ).execute()
                video_stats = video_stats["items"][0]

                title = video_stats["snippet"]["title"]
                likes = video_stats["statistics"].get("likeCount", 0)
                views = video_stats["statistics"].get("viewCount", 0)
                time_published = video_stats["snippet"]["publishedAt"]

                # Add current info to list
                video_info.append({
                    "video_id": video_id,
                    "title": title,
                    "likes": likes,
                    "views": views,
                    "time_published": time_published
                })

            # Update page token
            next_page_token = search.get("nextPageToken")

            # Update number of iterations performed so far
            num_iter -= 1

            # Break loop if there are no more results, or if the provided maximum is reached
            if not next_page_token or num_iter < 1:
                break

            # Pause to respect API limits
            time.sleep(1)

        # Break out of loop if an error is encountered
        except googleapiclient.errors.HttpError as e:
            print(f"An error occurred: {e}")
            break

    # Save to SQLite database
    save_to_sqlite(video_info, (search_id + ".db"))

    # Get data frame and save as .csv file
    df = get_sqlite_db((search_id + ".db"))
    df.to_csv((search_id + ".csv"), index=False)


def save_to_sqlite(video_info, db_file):
    try:
        # Connect to database
        with sqlite3.connect(db_file) as connection:
            cursor = connection.cursor()

            # Create table if it doesn't already exist
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS videos (
                video_id TEXT PRIMARY KEY,
                title TEXT,
                likes INTEGER,
                views INTEGER,
                time_published TEXT
            )
            ''')

            # Insert or replace data in SQLite table
            for video in video_info:
                cursor.execute('''
                INSERT OR REPLACE INTO videos(video_id, title, likes, views, time_published)
                VALUES(?,?,?,?,?) 
                ''', (video["video_id"], video["title"], video["likes"], video["views"], video["time_published"]))

            # Commit changes to database, and check first few rows
            connection.commit()
            print(get_sqlite_db(db_file, num_rows=5))
    except sqlite3.Error as e:
        print(e)


def get_sqlite_db(db_file, num_rows=None):
    try:
        # Connect to database
        with sqlite3.connect(db_file) as connection:
            cursor = connection.cursor()

            # Determine query
            if num_rows:
                query = f"SELECT * FROM videos LIMIT {num_rows}"
            else:
                query = "SELECT * FROM videos"

            # Fetch desired rows of database
            cursor.execute(query)
            rows = cursor.fetchall()

            # Fetch column names of database
            col_names = [description[0] for description in cursor.description]

            # Put results in dataframe and return
            df = pd.DataFrame(rows, columns=col_names)
            return df
    except sqlite3.Error as e:
        print(e)


if True:
    # search_videos(query="vaccines", max_results=50, num_iter=15, search_id="vaccine_search2")
    # search_videos(query="vaccination", max_results=50, num_iter=15, search_id="vaccine_search")

    # search_videos(query="diet", max_results=50, num_iter=5, search_id="diet_search")
    search_videos(query="vaccines", max_results=50, num_iter=5, search_id="test")
