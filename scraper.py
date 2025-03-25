import asyncio
import concurrent
import csv
import json
import logging
import os
import time
import glob
from datetime import datetime

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
CHECKPOINT_FILE = "scraper_checkpoint.json"
MAX_VIDEOS_PER_USER = 5
MS_TOKEN = "gaASJOVO5GywrRBvIjTpowwp9LOO9mNFigIsNRC_mCnywpIR-6TJS_JZrYcyP7QQyB89ZiX9cAfpgDoVuuT2oG6uOxVvcDItEtWJnaeSJ6Y3Uo0Ww6vFmvLmQJ3HySuZ2UcatpitHMO5HFFfN1AtjsYX"

# Global checkpoint data
checkpoint = {
    "last_completed_influencer": None,
    "currently_processing_influencer": None,  # Track the influencer being processed when killed
    "processed_videos": set(),
    "failed_influencers": set(),
    "last_update": None,
}


def setup_transcript_file():
    """Set up the transcript CSV file if it doesn't exist."""
    if not os.path.exists(TRANSCRIPT_FILE):
        with open(TRANSCRIPT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["username", "video_id", "transcript", "video_niche_content"]
            )
        logger.info(f"Created {TRANSCRIPT_FILE} with headers")


def load_checkpoint():
    """Load checkpoint data if it exists."""
    global checkpoint

    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                saved_checkpoint = json.load(f)

                # Convert the list of processed videos back to a set
                if "processed_videos" in saved_checkpoint:
                    saved_checkpoint["processed_videos"] = set(
                        saved_checkpoint["processed_videos"]
                    )
                
                # Convert failed_influencers from list to set
                if "failed_influencers" in saved_checkpoint:
                    saved_checkpoint["failed_influencers"] = set(
                        saved_checkpoint["failed_influencers"]
                    )

                checkpoint.update(saved_checkpoint)

            logger.info(
                f"Loaded checkpoint - Last influencer: {checkpoint['last_completed_influencer']}, "
                f"Processed videos: {len(checkpoint['processed_videos'])}"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")

    logger.info("No checkpoint found, starting from the beginning")
    return False


def save_checkpoint():
    """Save current progress to checkpoint file."""
    global checkpoint

    # Update the timestamp
    checkpoint["last_update"] = datetime.now().isoformat()

    try:
        # Convert sets to lists for JSON serialization
        serializable_checkpoint = checkpoint.copy()
        serializable_checkpoint["processed_videos"] = list(
            checkpoint["processed_videos"]
        )
        serializable_checkpoint["failed_influencers"] = list(
            checkpoint["failed_influencers"]
        )

        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(serializable_checkpoint, f)

        logger.info(
            f"Saved checkpoint - Last influencer: {checkpoint['last_completed_influencer']}, "
            f"Processed videos: {len(checkpoint['processed_videos'])}"
        )
    except Exception as e:
        logger.error(f"Failed to save checkpoint: {e}")


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

        # Add to processed videos in checkpoint
        checkpoint["processed_videos"].add(video_id)
        # Save checkpoint every time we process a video to ensure progress isn't lost
        save_checkpoint()
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

    # Skip if we've already processed this video
    if video_id in checkpoint["processed_videos"]:
        logger.info(f"Skipping already processed video {video_id} by {username}")
        return

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
        video_niche_content = await asyncio.to_thread(classify_video, video_path)
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
        # Mark this influencer as currently being processed
        checkpoint["currently_processing_influencer"] = username
        save_checkpoint()

        user = api.user(username=username)
        tasks = []
        video_count = 0
        video: Video = None
        async for video in user.videos(count=MAX_VIDEOS_PER_USER):
            tasks.append(process_video(username, video))
            video_count += 1
            if video_count >= MAX_VIDEOS_PER_USER:
                break
        await asyncio.gather(*tasks)
        logger.info(f"Processed {video_count} videos for {username}")

        # Mark this influencer as completed
        checkpoint["last_completed_influencer"] = username
        checkpoint["currently_processing_influencer"] = None  # Clear the currently processing influencer
        # Remove from failed influencers if it was there
        checkpoint["failed_influencers"].discard(username)
        save_checkpoint()
    except Exception as e:
        logger.error(f"Failed to process influencer {username}: {e}")
        # Add to failed influencers
        checkpoint["failed_influencers"].add(username)
        checkpoint["currently_processing_influencer"] = None  # Clear the currently processing influencer
        save_checkpoint()


def cleanup_remaining_files():
    """Clean up any remaining video and audio files in the current directory."""
    try:
        # Clean up video files
        video_patterns = ["@*_video_*.mp4", "*.mp4"]
        for pattern in video_patterns:
            for video_file in glob.glob(pattern):
                try:
                    os.remove(video_file)
                    logger.info(f"Cleaned up remaining video file: {video_file}")
                except Exception as e:
                    logger.error(f"Failed to clean up video file {video_file}: {e}")

        # Clean up audio files
        audio_patterns = ["*.wav"]
        for pattern in audio_patterns:
            for audio_file in glob.glob(pattern):
                try:
                    os.remove(audio_file)
                    logger.info(f"Cleaned up remaining audio file: {audio_file}")
                except Exception as e:
                    logger.error(f"Failed to clean up audio file {audio_file}: {e}")

    except Exception as e:
        logger.error(f"Error during cleanup: {e}")


async def main():
    """Main function to run the TikTok scraper."""
    logger.info("Starting TikTok scraper")
    setup_transcript_file()

    # Load checkpoint data if it exists
    load_checkpoint()

    influencers_df = get_influencers()
    influencers_df = influencers_df[910:]
    if influencers_df.empty:
        logger.error("No influencers found. Exiting.")
        return

    try:
        async with TikTokApi() as api:
            logger.info("Creating TikTok API session")
            await api.create_sessions(headless=False, num_sessions=1, sleep_after=3)

            # Determine where to start from
            start_processing = False
            last_influencer = checkpoint["last_completed_influencer"]
            currently_processing = checkpoint["currently_processing_influencer"]

            for _, row in influencers_df.iterrows():
                current_username = row["username"]

                # Skip failed influencers and the influencer that was being processed when killed
                if current_username in checkpoint["failed_influencers"]:
                    logger.info(f"Skipping previously failed influencer: {current_username}")
                    continue
                if current_username == currently_processing:
                    logger.info(f"Skipping influencer that was being processed when scraper was killed: {current_username}")
                    continue

                # If we have a checkpoint and haven't reached it yet, skip
                if last_influencer and not start_processing:
                    if current_username == last_influencer:
                        # We found the last processed influencer, so the next one is where we start
                        start_processing = True
                        logger.info(f"Resuming from after influencer: {last_influencer}")
                        continue
                # If we don't have a checkpoint or we've reached the checkpoint, process normally
                else:
                    start_processing = True

                if start_processing:
                    await process_influencer(api, current_username)

            logger.info("TikTok scraping completed")
    finally:
        # Clean up any remaining files when the scraper exits
        cleanup_remaining_files()


if __name__ == "__main__":
    asyncio.run(main())
