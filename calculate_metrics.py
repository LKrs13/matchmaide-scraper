from datetime import datetime, timedelta


async def calculate_metrics(username, api, months=3):
    user = api.user(username=username)
    user_info = await user.info()

    # Fetch user videos for the last 3 months
    videos = []
    three_months_ago = datetime.now() - timedelta(days=90)
    async for video in user.videos(count=100):  # Adjust count as necessary
        if video.create_time and video.create_time > three_months_ago:
            videos.append(video)

    total_likes = total_comments = total_shares = total_views = 0
    video_count = len(videos)

    print("VIDEO DICT:\n", videos[0].as_dict)

    for video in videos:
        stats = video.stats
        total_likes += stats.get("diggCount", 0)
        total_comments += stats.get("commentCount", 0)
        total_shares += stats.get("shareCount", 0)
        total_views += stats.get("playCount", 0)

    avg_engagement_rate = (
        (total_likes + total_comments + total_shares) / total_views
        if total_views
        else 0
    )
    avg_comments_per_post = total_comments / video_count if video_count else 0
    avg_shares_per_post = total_shares / video_count if video_count else 0
    avg_views_per_video = total_views / video_count if video_count else 0

    return {
        "username": username,
        "avg_engagement_rate": avg_engagement_rate,
        "avg_comments_per_post": avg_comments_per_post,
        "avg_shares_per_post": avg_shares_per_post,
        "avg_views_per_video": avg_views_per_video,
    }
