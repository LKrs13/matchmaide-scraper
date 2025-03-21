import sys

from video.llava import query_llava_model


def main():
    """
    Test function to query Llava model with an image file.

    Usage: python test_llava.py <image_path> [prompt]
    If prompt is not provided, it defaults to "What is in this picture?"
    """
    # Check if an image path was provided
    if len(sys.argv) < 2:
        print("Usage: python test_llava.py <image_path> [prompt]")
        return

    # Get the image path from command line argument
    image_path = sys.argv[1]

    # Get custom prompt if provided, or use default
    prompt = "What is in this picture?" if len(sys.argv) < 3 else sys.argv[2]

    print(f"Querying Llava with image: {image_path}")
    print(f"Using prompt: {prompt}")
    print("-" * 50)

    # Get the response from Llava
    response = query_llava_model(image_path, prompt)

    print("Response from Llava:")
    print("-" * 50)
    print(response)


if __name__ == "__main__":
    main()
