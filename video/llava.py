import os

import cv2


def query_llava_model(image_path, prompt="What is in this image?"):
    logger.info(f"Querying Llava model with image: {image_path}")
    try:
        with open(image_path, "rb") as img_file:
            image_bytes = img_file.read()
        image_base64 = base64.b64encode(image_bytes).decode("utf-8")
        payload = {"model": "llava", "prompt": prompt, "images": [image_base64]}
        response = requests.post(OLLAMA_API_ENDPOINT, json=payload)
        if response.status_code == 200:
            result = response.json()
            logger.info("Llava model query successful")
            return result.get("response", "No response content")
        else:
            logger.error(f"Llava query failed with status code: {response.status_code}")
            return f"Error: HTTP {response.status_code}"
    except Exception as e:
        logger.error(f"Error querying Llava model: {e}")
        return f"Error: {str(e)}"


def analyze_video_with_llava(
    video_path, frame_sample_rate=30, prompt="Describe what is happening in this scene"
):
    """
    Analyze video content using the llava model to understand what's happening in the video.
    Samples frames at regular intervals and sends them to the llava model for analysis.

    Args:
        video_path: Path to the video file
        frame_sample_rate: Sample one frame every N frames
        prompt: The prompt to send to the llava model

    Returns:
        A summary of what's happening in the video
    """
    logger.info(f"Analyzing video content with llava model: {video_path}")

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        logger.error(f"Unable to open video file: {video_path}")
        return "Error: Unable to open video file"

    # Get video information
    fps = cap.get(cv2.CAP_PROP_FPS)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    duration = total_frames / fps if fps > 0 else 0

    logger.info(
        f"Video stats: {fps} FPS, {total_frames} frames, {duration:.2f} seconds"
    )

    # Set up for frame sampling
    frame_count = 0
    frames_analyzed = 0
    llava_descriptions = []

    # Create a temporary directory for storing frames
    temp_dir = os.path.join(os.getcwd(), "temp_frames")
    os.makedirs(temp_dir, exist_ok=True)

    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                break

            # Sample one frame every N frames
            if frame_count % frame_sample_rate == 0:
                # Save frame as image
                frame_path = os.path.join(temp_dir, f"frame_{frame_count}.jpg")
                cv2.imwrite(frame_path, frame)

                # Get timestamp for the frame
                timestamp = frame_count / fps if fps > 0 else 0
                timestamp_str = f"{int(timestamp // 60):02d}:{int(timestamp % 60):02d}"

                # Query llava model with the frame
                custom_prompt = f"{prompt} (Timestamp: {timestamp_str})"
                description = query_llava_model(frame_path, custom_prompt)

                if description and not description.startswith("Error:"):
                    llava_descriptions.append(f"[{timestamp_str}] {description}")
                    logger.info(f"Processed frame {frame_count} at {timestamp_str}")
                else:
                    logger.warning(
                        f"Failed to get description for frame {frame_count}: {description}"
                    )

                frames_analyzed += 1

                # Remove the temporary frame file
                if os.path.exists(frame_path):
                    os.remove(frame_path)

            frame_count += 1

            # Safety check - if video is extremely long, limit analysis
            if frames_analyzed >= 10:  # Analyze at most 10 frames per video
                logger.info(f"Reached maximum frame analysis limit (10 frames)")
                break

    except Exception as e:
        logger.error(f"Error during llava video analysis: {e}")

    finally:
        # Clean up
        cap.release()

        # Remove temp directory if empty
        try:
            os.rmdir(temp_dir)
        except:
            pass

    # Return the combined descriptions
    if llava_descriptions:
        summary = "\n".join(llava_descriptions)
        logger.info(f"Completed llava analysis with {frames_analyzed} frames")
        return summary
    else:
        return "No content could be analyzed in the video"
