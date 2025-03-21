import torch
import torchvision.io as io
from transformers import VideoMAEForVideoClassification, VideoMAEImageProcessor

# Load pretrained model and processor
processor = VideoMAEImageProcessor.from_pretrained(
    "MCG-NJU/videomae-base-finetuned-kinetics"
)
model = VideoMAEForVideoClassification.from_pretrained(
    "MCG-NJU/videomae-base-finetuned-kinetics"
)


def classify_video(video_path):
    # Load and preprocess video
    video, _, _ = io.read_video(video_path, pts_unit="sec")

    # Sample frames uniformly
    num_frames = 16
    indices = torch.linspace(0, video.shape[0] - 1, num_frames).long()
    frames = video[indices]

    # Preprocess frames
    inputs = processor(list(frames.numpy()), return_tensors="pt")

    # Perform inference
    with torch.no_grad():
        outputs = model(**inputs)
        logits = outputs.logits

    # Get the top 5 predicted labels with scores
    top_k = 5
    top_k_scores, top_k_indices = logits.topk(top_k)
    top_k_scores = top_k_scores.squeeze().tolist()
    top_k_indices = top_k_indices.squeeze().tolist()
    top_k_labels = [model.config.id2label[idx] for idx in top_k_indices]

    # Combine labels and scores
    top_k_predictions = [
        {"label": label, "score": score}
        for label, score in zip(top_k_labels, top_k_scores)
    ]

    # Convert predictions to a CSV-parsable string
    predictions_str = ",".join(
        [f"{pred['label']}:{pred['score']}" for pred in top_k_predictions]
    )

    print(f"VideoMA classified video with predictions: {predictions_str}")

    return predictions_str
