import asyncio
from collections import Counter

from googletrans import Translator
from langdetect import detect
from nltk.corpus import stopwords
from nltk.tag import pos_tag
from nltk.tokenize import word_tokenize
from textblob import TextBlob

translator = Translator()


async def translate_to_english(text):
    try:
        # Detect the source language
        source_lang = detect(text)
        print(f"Detected language: {source_lang}")

        # Translate to English
        translation = await translator.translate(text, dest="en")
        return translation.text
    except Exception as e:
        return f"Error: {str(e)}"


async def get_topic_words(text, num_words=5):
    # Translate text to English using TextBlob
    translated_text = await translate_to_english(text)

    # Convert to lowercase and tokenize
    words = word_tokenize(translated_text.lower())

    # Get English stopwords
    stop_words = set(stopwords.words("english"))

    # Filter words: keep only nouns (NN) and remove stopwords
    tagged_words = pos_tag(words)
    meaningful_words = [
        word
        for word, pos in tagged_words
        if pos.startswith("NN") and word.isalnum() and word not in stop_words
    ]

    # Count word frequencies
    word_counts = Counter(meaningful_words)

    # Get the most common words
    top_words = word_counts.most_common(num_words)

    return [word for word, count in top_words]


def analyze_comments(comments_list):
    """
    Analyze a list of comments to extract sentiment and topic words.

    Args:
        comments_list: List of comment strings

    Returns:
        tuple: (list of topic words, average sentiment polarity score)
    """
    if not comments_list or len(comments_list) == 0:
        return ["-1"], 0

    # Combine all comments into one text for topic analysis
    all_comments_text = " ".join(comments_list)

    # Calculate sentiment for each comment
    sentiments = []
    for comment in comments_list:
        if comment and isinstance(comment, str):
            blob = TextBlob(comment)
            sentiments.append(blob.sentiment.polarity)

    # Calculate average sentiment (if there are valid sentiments)
    avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0

    # Get topic words asynchronously
    try:
        # Run get_topic_words in an event loop
        topics = asyncio.run(get_topic_words(all_comments_text))
        if not topics:
            topics = ["-1"]
    except Exception as e:
        print(f"Error getting topic words: {str(e)}")
        topics = ["-1"]

    return topics, avg_sentiment


text = "The quick brown fox jumps over the lazy dog. The dog barks at the fox."

if __name__ == "__main__":
    main_words = asyncio.run(get_topic_words(text))
    print(main_words)

    # Test analyze_comments
    test_comments = [
        "This video is amazing! I love it.",
        "Not very interesting content.",
        "The best TikTok I've seen all day!",
    ]
    topics, sentiment = analyze_comments(test_comments)
    print(f"Topics: {topics}")
    print(f"Average sentiment: {sentiment}")
