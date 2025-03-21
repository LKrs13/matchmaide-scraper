def analyze_video_niche_content(video_path):
    """
    Analyze the video content by sampling frames and using MobileNetV2
    to predict scene labels. Aggregates the confidence scores across frames
    and returns the 5 labels with the highest average confidence.
    """
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        logger.error(f"Unable to open video file: {video_path}")
        return "error"
    logger.info(f"Analyzing video content: {video_path}")
    prediction_scores = {}
    frame_count = 0
    frames_analyzed = 0
    while True:
        ret, frame = cap.read()
        if not ret:
            break
        # Sample one frame every 30 frames (~1 second at 30fps)
        if frame_count % 30 == 0:
            img = cv2.resize(frame, (224, 224))
            img = np.expand_dims(img, axis=0)
            img = preprocess_input(img)
            preds = MODEL.predict(img)
            decoded = decode_predictions(preds, top=5)[0]
            logger.info(f"Predictions for frame {frame_count}: {decoded}")
            for pred in decoded:
                label = pred[1]
                confidence = pred[2]
                # Sum up the confidence scores for each label across frames
                prediction_scores[label] = prediction_scores.get(label, 0) + confidence
            frames_analyzed += 1
        frame_count += 1
    cap.release()
    for label in prediction_scores:
        prediction_scores[label] /= frames_analyzed
    top_predictions = sorted(
        prediction_scores.items(), key=lambda x: x[1], reverse=True
    )[:5]
    top_labels = [label for label, _ in top_predictions]
    logger.info(f"Top 5 predicted labels: {top_labels}")
    return ",".join(top_labels)
