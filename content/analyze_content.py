def analyze_content(videos):
    captions = [video.as_dict["desc"] for video in videos]

    vectorizer = CountVectorizer(max_df=0.95, min_df=2, stop_words="english")
    dtm = vectorizer.fit_transform(captions)
    lda = LatentDirichletAllocation(n_components=5, random_state=0)
    lda.fit(dtm)
    dominant_topics = [
        [vectorizer.get_feature_names()[i] for i in topic.argsort()[-10:]]
        for topic in lda.components_
    ]
