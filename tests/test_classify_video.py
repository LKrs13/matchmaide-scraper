import os
import time

from video.i3d import classify_video


def test_local_video():
    # Path to your local video file - update this path
    video_path = "/Users/rayanesahi/Desktop/@sayfjoumat_video_7106677267585076485 5.mp4"

    # Make sure file exists
    if not os.path.exists(video_path):
        print(f"Error: Video file not found at {video_path}")
        return

    print(f"Testing classification on {video_path}")
    start_time = time.time()

    # Call classify_video function
    results = classify_video(video_path)

    elapsed_time = time.time() - start_time
    print(f"Classification completed in {elapsed_time:.2f} seconds")
    print("Results:", results)


if __name__ == "__main__":
    test_local_video()
