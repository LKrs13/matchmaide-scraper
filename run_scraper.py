#!/usr/bin/env python3
import argparse
import logging
import os
import signal
import subprocess
import sys
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("scraper_monitor.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("scraper_monitor")

# Global flag to track if we're shutting down
shutting_down = False


def signal_handler(sig, frame):
    """Handle interrupt signals gracefully"""
    global shutting_down
    logger.info(f"Received signal {sig}, shutting down...")
    shutting_down = True


def run_with_restart(max_restarts=5, cooldown_period=5):
    """
    Run the scraper process and restart it if it crashes

    Args:
        max_restarts: Maximum number of restart attempts (-1 for infinite)
        cooldown_period: Time to wait between restarts (in seconds)
    """
    restarts = 0
    start_time = time.time()

    while not shutting_down:
        # Check if we've reached the maximum number of restarts
        if max_restarts >= 0 and restarts >= max_restarts:
            logger.error(f"Reached maximum restart limit ({max_restarts}). Giving up.")
            break

        # Start the scraper process
        logger.info(f"Starting scraper (attempt {restarts + 1})")
        process = subprocess.Popen([sys.executable, "scraper.py"])

        # Wait for the process to complete
        exit_code = process.wait()

        # If shutting down, break out of the loop
        if shutting_down:
            break

        # Check how the process exited
        if exit_code == 0:
            logger.info("Scraper completed successfully!")
            break
        else:
            run_time = time.time() - start_time
            logger.warning(
                f"Scraper process exited with code {exit_code} after running for {run_time:.2f} seconds"
            )

            # Increment restart counter and wait before restarting
            restarts += 1
            logger.info(f"Waiting {cooldown_period} seconds before restart...")

            # Wait for cooldown period, but check for shutdown signal
            cooldown_start = time.time()
            while time.time() - cooldown_start < cooldown_period:
                if shutting_down:
                    break
                time.sleep(0.5)

            logger.info(
                f"Restarting scraper (attempt {restarts + 1} of {max_restarts if max_restarts >= 0 else 'unlimited'})"
            )


def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Run and monitor the TikTok scraper process"
    )
    parser.add_argument(
        "--max-restarts",
        type=int,
        default=-1,
        help="Maximum number of restart attempts. Default is infinite (-1)",
    )
    parser.add_argument(
        "--cooldown",
        type=int,
        default=5,
        help="Seconds to wait between restart attempts. Default is 5 seconds",
    )
    args = parser.parse_args()

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Log start time
    logger.info(f"Scraper monitor starting at {datetime.now()}")
    logger.info(
        f"Max restarts: {args.max_restarts if args.max_restarts >= 0 else 'unlimited'}"
    )
    logger.info(f"Cooldown period: {args.cooldown} seconds")

    try:
        run_with_restart(args.max_restarts, args.cooldown)
    except Exception as e:
        logger.error(f"Monitor failed with error: {e}")

    logger.info("Scraper monitor shutting down")


if __name__ == "__main__":
    main()
