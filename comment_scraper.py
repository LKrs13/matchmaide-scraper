import asyncio
import csv
import logging
import os

import pandas as pd
from TikTokApi import TikTokApi
from TikTokApi.api.video import Video

from comments.analyze_comments import analyze_comments

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Constants
COMMENTS_FILE = "video_comments.csv"
MAX_VIDEOS_PER_USER = 30
MAX_COMMENTS_PER_VIDEO = 30


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


def save_to_comments_file(
    username, video_id, comments_topics, comments_sentiment, comment_count
):
    """Save comment analysis data to CSV file."""
    try:
        with open(COMMENTS_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [username, video_id, comments_topics, comments_sentiment, comment_count]
            )
        logger.info(f"Saved comment analysis for video {video_id} by {username}")
    except Exception as e:
        logger.error(f"Failed to save comment analysis for {video_id}: {e}")


async def process_video_comments(username, video):
    """Process and analyze comments for a specific video."""
    video_id = video.id
    logger.info(f"Processing comments for video {video_id} by {username}")

    try:
        # Collect comments
        comments = []
        comment_count = 0
        async for comment in video.comments(count=MAX_COMMENTS_PER_VIDEO):
            comments.append(comment.text)
            comment_count += 1

        # Analyze comments
        comments_topics, comments_sentiment = await asyncio.to_thread(
            analyze_comments, comments
        )

        # Save results
        await asyncio.to_thread(
            save_to_comments_file,
            username,
            video_id,
            comments_topics,
            comments_sentiment,
            comment_count,
        )

        logger.info(f"Analyzed {comment_count} comments for video {video_id}")
        return comments_topics, comments_sentiment, comment_count

    except Exception as e:
        logger.error(f"Error processing comments for video {video_id}: {e}")
        return ["-1"], 0, 0


async def process_influencer_comments(api: TikTokApi, username: str):
    """Process comments for all videos of a specific influencer."""
    logger.info(f"Processing comments for influencer: {username}")
    try:
        user = api.user(username=username)
        tasks = []
        video_count = 0

        async for video in user.videos(count=MAX_VIDEOS_PER_USER):
            tasks.append(process_video_comments(username, video))
            video_count += 1

        results = await asyncio.gather(*tasks)
        total_comments = sum(result[2] for result in results if result)

        logger.info(
            f"Processed comments for {video_count} videos ({total_comments} total comments) from {username}"
        )
    except Exception as e:
        logger.error(f"Failed to process comments for influencer {username}: {e}")


async def main():
    """Main function to run the TikTok comment scraper."""
    logger.info("Starting TikTok comment scraper")
    setup_comments_file()
    influencers_df = get_influencers()

    if influencers_df.empty:
        logger.error("No influencers found. Exiting.")
        return

    async with TikTokApi() as api:
        logger.info("Creating TikTok API session")
        await api.create_sessions(headless=False, num_sessions=1, sleep_after=3)

        for _, row in influencers_df.iterrows():
            await process_influencer_comments(api, row["username"])

    logger.info("TikTok comment scraping completed")


if __name__ == "__main__":
    asyncio.run(main())
