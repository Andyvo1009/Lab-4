from typing import Tuple
import numpy as np

class Normalize:
    def __init__(self, mean, std):
        self.mean = np.array(mean)  # Shape: (3,)
        self.std = np.array(std)    # Shape: (3,)

    def transform(self, image):
        try:
            image = np.array(image, dtype=np.float32)
            # Ensure image is in range [0, 1] before applying mean/std
            image = image / 255.0
            # Broadcast mean and std across the image (32, 32, 3)
            image = (image - self.mean) / self.std
            return image  # Shape: (32, 32, 3)
        except Exception as e:
            print(f"Normalize error: {e}, image shape: {image.shape}")
            raise
