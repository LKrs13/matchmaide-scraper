import asyncio
import concurrent
import csv
import logging
import os

import pandas as pd
import pyktok as pyk
import speech_recognition as sr
from moviepy.video.io.VideoFileClip import VideoFileClip
from TikTokApi import TikTokApi
from TikTokApi.api.video import Video

from video.videoma import classify_video

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

TRANSCRIPT_FILE = "transcript.csv"
VIDEO_DATA_FILE = "video_data.csv"
PROCESS_POOL = concurrent.futures.ProcessPoolExecutor(max_workers=1)
MAX_VIDEOS_PER_USER = 30
MS_TOKEN = "gaASJOVO5GywrRBvIjTpowwp9LOO9mNFigIsNRC_mCnywpIR-6TJS_JZrYcyP7QQyB89ZiX9cAfpgDoVuuT2oG6uOxVvcDItEtWJnaeSJ6Y3Uo0Ww6vFmvLmQJ3HySuZ2UcatpitHMO5HFFfN1AtjsYX"


def setup_transcript_file():
    """Set up the transcript CSV file if it doesn't exist."""
    if not os.path.exists(TRANSCRIPT_FILE):
        with open(TRANSCRIPT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["username", "video_id", "transcript", "video_niche_content"]
            )
        logger.info(f"Created {TRANSCRIPT_FILE} with headers")


def get_influencers():
    """Read influencer data from the CSV file."""
    try:
        influencers_df = pd.read_csv("tiktok_influencers.csv")
        logger.info(f"Loaded {len(influencers_df)} influencers from CSV")
        return influencers_df
    except Exception as e:
        logger.error(f"Failed to read influencers CSV: {e}")
        return pd.DataFrame()


def save_to_transcript_file(username, video_id, transcript, video_niche_content):
    """Save transcript data to CSV file."""
    try:
        with open(TRANSCRIPT_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([username, video_id, transcript, video_niche_content])
        logger.info(f"Saved transcript for video {video_id} by {username}")
    except Exception as e:
        logger.error(f"Failed to save transcript for {video_id}: {e}")


def clean_up_files(video_path, audio_path=None):
    """Delete video and audio files."""
    video_path = os.path.join(os.getcwd(), video_path)
    audio_path = os.path.join(os.getcwd(), audio_path) if audio_path else None
    try:
        if os.path.exists(video_path):
            os.remove(video_path)
            logger.info(f"Deleted file: {video_path}")
        if audio_path and os.path.exists(audio_path):
            os.remove(audio_path)
            logger.info(f"Deleted file: {audio_path}")
    except Exception as e:
        logger.error(f"Failed to clean up files: {e}")


def download_video(username, video_id):
    """Download a TikTok video."""
    video_path = f"@{username}_video_{video_id}.mp4"
    try:
        video_url = (
            f"https://www.tiktok.com/@{username}/video/{video_id}"
            "?is_copy_url=1&is_from_webapp=v1"
        )
        response = pyk.save_tiktok(video_url, True, VIDEO_DATA_FILE)
        logger.info(f"Downloaded video {video_id} by {username}")
        return video_path
    except Exception as e:
        logger.error(
            f"Failed to download video {video_id}: {e}", stack_info=True, exc_info=True
        )
        return None


def extract_audio(video_path, video_id):
    """Extract audio from video file."""
    logger.info(f"Extracting audio from {video_path}")
    audio_path = f"{video_id}.wav"
    try:
        video_clip = VideoFileClip(video_path)
        video_clip.audio.write_audiofile(audio_path, codec="pcm_s16le", logger=None)
        video_clip.close()
        logger.info(f"Extracted audio to {audio_path}")
        return audio_path
    except Exception as e:
        logger.error(f"Failed to extract audio from {video_path}: {e}")
        return None


def transcribe_audio(audio_path, video_id):
    """Convert audio to text using speech recognition."""
    try:
        recognizer = sr.Recognizer()
        with sr.AudioFile(audio_path) as source:
            audio = recognizer.record(source)
            transcript = recognizer.recognize_google(audio)
            logger.info(f"Transcribed audio for {video_id}")
            return transcript
    except Exception as e:
        logger.error(f"Speech recognition failed for {video_id}: {str(e)}")
        return ""


async def process_video(username, video):
    video_id = video.id
    logger.info(f"Processing video {video_id} by {username}")
    video_path = await asyncio.to_thread(download_video, username, video_id)
    if not video_path:
        return
    transcript_text = ""
    try:
        audio_path = await asyncio.to_thread(extract_audio, video_path, video_id)
        if audio_path:
            transcript_text = await asyncio.to_thread(
                transcribe_audio, audio_path, video_id
            )
        loop = asyncio.get_running_loop()
        video_niche_content = await loop.run_in_executor(
            PROCESS_POOL, classify_video, video_path
        )
        await asyncio.to_thread(
            save_to_transcript_file,
            username,
            video_id,
            transcript_text,
            video_niche_content,
        )
        await asyncio.to_thread(clean_up_files, video_path, audio_path)
    except Exception as e:
        logger.error(f"Error processing video {video_id}: {e}")
        await asyncio.to_thread(clean_up_files, video_path)


async def process_influencer(api: TikTokApi, username: str):
    logger.info(f"Processing influencer: {username}")
    try:
        user = api.user(username=username)
        tasks = []
        video_count = 0
        video: Video = None
        async for video in user.videos(count=MAX_VIDEOS_PER_USER):
            tasks.append(process_video(username, video))
            video_count += 1
        await asyncio.gather(*tasks)
        logger.info(f"Processed {video_count} videos for {username}")
    except Exception as e:
        logger.error(f"Failed to process influencer {username}: {e}")


async def main():
    """Main function to run the TikTok scraper."""
    logger.info("Starting TikTok scraper")
    setup_transcript_file()
    influencers_df = get_influencers()
    if influencers_df.empty:
        logger.error("No influencers found. Exiting.")
        return
    async with TikTokApi() as api:
        logger.info("Creating TikTok API session")
        await api.create_sessions(
            headless=False,
            num_sessions=1,
            sleep_after=3,
        )
        for _, row in influencers_df.iterrows():
            await process_influencer(api, row["username"])
    logger.info("TikTok scraping completed")


if __name__ == "__main__":
    asyncio.run(main())
