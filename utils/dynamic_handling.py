## This file contains helper functions for dynamic handling of images and colors. file: dynamic_handling.py - Created BY Smoke [https://github.com/Varietyz/].

# Helpers for dynamic coloring and image display
from colorsys import rgb_to_hsv, hsv_to_rgb

def get_dynamic_color(image):
    """
    Analyze the image and return a color based on the most dominant hue,
    excluding pixels that are near white or black.
    
    Process:
      1. Downsize the image to speed up processing.
      2. Convert each pixel from RGB to HSV.
      3. Filter out pixels with very low saturation (i.e. nearly white/gray)
         or very low value (i.e. nearly black).
      4. Count the occurrence of each hue.
      5. If no valid hue is found, return the default yellow.
      6. Otherwise, convert the dominant hue (with full saturation and brightness)
         back to an RGB color.
    """
    # Ensure the image is in RGB mode.
    image = image.convert('RGB')
    # Downsize for performance.
    small_img = image.resize((100, 100))
    pixels = list(small_img.getdata())

    hue_counts = {}
    # Define thresholds for saturation and brightness
    MIN_SATURATION = 0.3  # Ignore unsaturated (grayish/white) pixels.
    MIN_VALUE = 0.2       # Ignore very dark pixels.

    for r, g, b in pixels:
        h, s, v = rgb_to_hsv(r / 255, g / 255, b / 255)
        # Skip pixels that are nearly white/gray (unsaturated) or very dark.
        if s < MIN_SATURATION or v < MIN_VALUE:
            continue
        hue = int(h * 360)
        hue_counts[hue] = hue_counts.get(hue, 0) + 1

    if not hue_counts:
        # If no dominant hue is found, default to yellow.
        return (175, 175, 175)

    dominant_hue = max(hue_counts, key=hue_counts.get)
    # For intense blue and purple hues, reduce saturation and increase brightness.
    if 210 <= dominant_hue <= 330:
        adjusted_s = 0.5   # Lower saturation for blue/purple.
        adjusted_v = 0.9   # Increase brightness for better contrast.
    else:
        adjusted_s = 0.8
        adjusted_v = 1

    r, g, b = hsv_to_rgb(dominant_hue / 360, adjusted_s, adjusted_v)
    return (int(r * 255), int(g * 255), int(b * 255))

def get_coin_image_id(quantity):
    """
    Returns the coin image id based on the coin quantity.
    
    Mapping:
      1    -> 995.png
      2    -> 996.png
      3    -> 997.png
      4    -> 998.png
      5    -> 999.png
      10   -> 1000.png
      50   -> 1001.png
      100  -> 1002.png
      1000 -> 1003.png
      10000-> 1004.png

    For quantities that do not exactly match a key, returns the image corresponding
    to the highest key that is less than or equal to the quantity.
    """
    mapping = {
        1: 995,
        2: 996,
        3: 997,
        4: 998,
        5: 999,
        10: 1000,
        50: 1001,
        100: 1002,
        1000: 1003,
        10000: 1004,
    }
    # Find all keys less than or equal to the quantity.
    possible = [k for k in mapping.keys() if quantity >= k]
    if not possible:
        return mapping[1]
    best = max(possible)
    return mapping[best]

def get_value_color(numCoins):
    """
    Return a color based on the coin value thresholds:
      - If numCoins >= 1,000,000,000, return (102, 152, 255)  [Hex 0x6698FF]
      - Else if numCoins >= 10,000,000, return (0, 255, 128)     [Hex 0x00FF80]
      - Else if numCoins >= 100,000, return (255, 255, 255)        [Hex 0xFFFFFF]
      - Else if numCoins > 0, return (255, 255, 0)                 [Hex 0xFFFF00]
      - Else, return (255, 0, 0)                                  [Hex 0xFF0000]
    """
    if numCoins >= 1_000_000_000:
        return (102, 152, 255) # OSRS Billions Blue
    elif numCoins >= 10_000_000:
        return (0, 255, 128) # OSRS Millions Green
    elif numCoins >= 100_000:
        return (255, 255, 255) # OSRS 100K White
    elif numCoins > 0:
        return (255, 255, 0) # OSRS Standard Yellow 
    else:
        return (255, 0, 0) # No Value Red

