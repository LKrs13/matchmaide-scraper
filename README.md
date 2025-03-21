# Matchmaide Scraper

## Project Overview
Matchmaide Scraper is a Python-based project designed to analyze and extract insights from video content and associated metadata. It leverages advanced video analysis techniques and integrates with machine learning models to provide detailed descriptions and classifications of video content.

## Features
- **Main Feature**: `scraper.py` - The primary script for scraping and processing video data.
- Analyze video content using the `llava` model.
- Extract and process metadata from videos.
- Classify video content using pre-trained models.
- Analyze comments and content for deeper insights.
- Support for TikTok influencer data analysis.

## Installation
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd matchmaide-scraper
   ```
2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage
### Running the Scraper
The main feature of the project is the `scraper.py` script. Run it as follows:
```bash
python scraper.py
```
This script downloads TikTok videos, extracts metadata, and processes the video data. The metadata is saved in the `video_data.csv` file, which contains details such as video URLs, descriptions, and other relevant information.

### Analyzing Comments
The `comment_scraper.py` script scrapes comments from TikTok videos of influencers listed in the `tiktok_influencers.csv` file. It processes the comments, analyzes their topics and sentiment, and saves the results in `video_comments.csv`. 

To run the script:
```bash
python comment_scraper.py
```

**How it works:**
- Reads influencer usernames from `tiktok_influencers.csv`.
- Scrapes up to 30 videos per influencer and up to 30 comments per video.
- Analyzes comment topics and sentiment using the `analyze_comments` function.
- Saves the processed data to `video_comments.csv`.

### Analyzing Videos
Use the `analyze_video_with_llava` function in `video/llava.py` to analyze video content. Example:
```python
from video.llava import analyze_video_with_llava

result = analyze_video_with_llava("path/to/video.mp4")
print(result)
```

### Running Tests
Run the test suite to ensure everything is working correctly:
```bash
pytest tests/
```

## File Structure
```
matchmaide-scraper/
├── comments/          # Comment analysis scripts
├── content/           # Content analysis scripts
├── labels/            # Label datasets for classification
├── tests/             # Test cases for the project
├── video/             # Video analysis modules
├── requirements.txt   # Project dependencies
├── scraper.py         # Main scraper script for downloading and processing TikTok videos
├── comment_scraper.py  # Script for scraping and analyzing TikTok video comments
├── tiktok_influencers.csv  # TikTok influencer data
├── video_data.csv     # Generated metadata for downloaded videos
├── video_comments.csv  # Generated file containing analyzed comment data
```

## Dependencies
The project requires the following Python libraries:
- OpenCV
- NumPy
- Pandas
- PyTest
- Logging

Install all dependencies using the `requirements.txt` file.

## Contributing
Contributions are welcome! Please follow these steps:
1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Submit a pull request with a detailed description of your changes.

## License
This project is licensed under the MIT License. See the LICENSE file for details.