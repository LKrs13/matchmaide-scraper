import asyncio
import csv
import logging
import os
import argparse

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential
from TikTokApi import TikTokApi

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

COMMENTS_FILE = "video_comments.csv"
MAX_VIDEOS_PER_USER = 9
MAX_COMMENTS_PER_VIDEO = 5


def setup_comments_file():
    """Set up the comments CSV file if it doesn't exist."""
    if not os.path.exists(COMMENTS_FILE):
        with open(COMMENTS_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "username",
                    "video_id",
                    "comments_topics",
                    "comments_sentiment",
                    "comment_count",
                ]
            )
        logger.info(f"Created {COMMENTS_FILE} with headers")


def get_influencers():
    """Read influencer data from the CSV file."""
    try:
        influencers_df = pd.read_csv("tiktok_influencers.csv")
        logger.info(f"Loaded {len(influencers_df)} influencers from CSV")
        return influencers_df
    except Exception as e:
        logger.error(f"Failed to read influencers CSV: {e}")
        return pd.DataFrame()


def save_to_comments_file(username, video_id, comments):
    """Save comment analysis data to CSV file."""
    try:
        with open(COMMENTS_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([username, video_id, comments])
        logger.info(f"Saved comment analysis for video {video_id} by {username}")
    except Exception as e:
        logger.error(f"Failed to save comment analysis for {video_id}: {e}")


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
async def process_video_comments(username, video):
    """Process and save comments for a specific video."""
    video_id = video.id
    logger.info(f"Processing comments for video {video_id} by {username}")

    try:
        comments = []
        comment_count = 0
        async for comment in video.comments(count=MAX_COMMENTS_PER_VIDEO):
            comments.append(comment.text)
            comment_count += 1
            if comment_count >= MAX_COMMENTS_PER_VIDEO:
                break
            # Add small delay between comment fetches
            await asyncio.sleep(2)

        if comments:
            await asyncio.to_thread(
                save_to_comments_file,
                username,
                video_id,
                comments,
            )

        logger.info(f"Saved {comment_count} comments for video {video_id}")
        return comments, None, comment_count

    except Exception as e:
        logger.error(f"Error processing comments for video {video_id}: {e}")
        raise


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
async def process_influencer_comments(api: TikTokApi, username: str):
    """Process comments for all videos of a specific influencer."""
    logger.info(f"Processing comments for influencer: {username}")
    try:
        # Add delay before getting user
        await asyncio.sleep(5)
        logger.info(f"Fetching user data for {username}")
        user = api.user(username=username)
        
        video_count = 0
        async for video in user.videos(count=MAX_VIDEOS_PER_USER):
            video_count += 1
            logger.info(f"Processing video {video_count}/{MAX_VIDEOS_PER_USER} for {username}")
            await process_video_comments(username, video)
            # Add delay between videos
            await asyncio.sleep(5)

    except Exception as e:
        logger.error(f"Failed to process comments for influencer {username}: {e}")
        raise


async def main():
    """Main function to run the TikTok comment scraper."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='TikTok comment scraper')
    parser.add_argument('--start-from', type=int, default=0,
                      help='Index of the influencer to start from (0-based)')
    args = parser.parse_args()

    logger.info("Starting TikTok comment scraper")
    setup_comments_file()
    influencers_df = get_influencers()

    if influencers_df.empty:
        logger.error("No influencers found. Exiting.")
        return

    # Slice the dataframe to start from the specified index
    if args.start_from > 0:
        if args.start_from >= len(influencers_df):
            logger.error(f"Start index {args.start_from} is out of range. Total influencers: {len(influencers_df)}")
            return
        influencers_df = influencers_df.iloc[args.start_from:]
        logger.info(f"Starting from influencer at index {args.start_from}")

    async with TikTokApi() as api:
        logger.info("Creating TikTok API session")
        await api.create_sessions(headless=False, num_sessions=1, sleep_after=3)
        for _, row in influencers_df.iterrows():
            try:
                await process_influencer_comments(api, row["username"])
                # Add longer delay between influencers
                logger.info("Waiting 60 seconds before next influencer...")
                await asyncio.sleep(60)  # Wait 60 seconds between influencers
            except Exception as e:
                logger.error(f"Failed to process influencer {row['username']}: {e}")
                # If we hit an error, wait longer before trying the next one
                logger.info("Error occurred, waiting 120 seconds before next attempt...")
                await asyncio.sleep(120)  # Wait 2 minutes before next attempt
                continue

    logger.info("TikTok comment scraping completed")


if __name__ == "__main__":
    asyncio.run(main())
