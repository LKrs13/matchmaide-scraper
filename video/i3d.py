import csv
import os

import cv2
import numpy as np
import tensorflow as tf
import tensorflow_hub as hub

# Load the labels from the labels directory
current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(current_dir)
labels_path = os.path.join(project_dir, "labels", "kinetics_400_labels.csv")

# Load labels from CSV file
labels = []
with open(labels_path, "r") as f:
    csv_reader = csv.reader(f)
    for row in csv_reader:
        if row:  # Make sure the row is not empty
            labels.append(row[1])  # Assuming label is in the first column
labels.pop(0)

VIDEO_MODEL = hub.load("https://tfhub.dev/deepmind/i3d-kinetics-400/1").signatures[
    "default"
]


def classify_video(video_path, max_frames=64, top_k=5):
    """
    Classify the content of a video using the I3D model.

    Args:
        video_path (str): Path to the video file.
        max_frames (int): Maximum number of frames to sample from the video.
        top_k (int): Number of top predictions to return.

    Returns:
        tuple: Top-k class indices, their corresponding probabilities, and labels.
    """
    # Open the video file
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise ValueError("Unable to open video file")

    # Get the total number of frames
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    if total_frames == 0:
        raise ValueError("Video has no frames")

    # Determine how many frames to use
    if total_frames <= max_frames:
        num_frames = total_frames
        indices = list(range(total_frames))
    else:
        num_frames = max_frames
        indices = [int(i * total_frames / num_frames) for i in range(num_frames)]

    # Extract and preprocess frames
    frames = []
    for idx in indices:
        cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
        ret, frame = cap.read()
        if not ret:
            raise ValueError("Error reading frame")
        # Convert BGR to RGB
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        # Resize to 224x224 as required by the model
        frame = cv2.resize(frame, (224, 224))
        # Normalize to [0, 1]
        frame = frame.astype(np.float32) / 255.0
        frames.append(frame)

    # Release the video capture object
    cap.release()

    # Stack frames into a tensor
    video_tensor = np.stack(frames, axis=0)  # Shape: [num_frames, 224, 224, 3]
    video_tensor = np.expand_dims(
        video_tensor, axis=0
    )  # Shape: [1, num_frames, 224, 224, 3]

    # Perform inference
    logits = VIDEO_MODEL(tf.convert_to_tensor(video_tensor))["default"]
    probabilities = tf.nn.softmax(logits)
    top_k_values, top_k_indices = tf.math.top_k(probabilities, k=top_k)

    # Convert to numpy arrays
    top_k_values = top_k_values.numpy()[0]
    top_k_indices = top_k_indices.numpy()[0]

    # Get the labels for the top-k indices
    top_k_labels = [labels[i] for i in top_k_indices]

    return top_k_indices, top_k_values, top_k_labels
