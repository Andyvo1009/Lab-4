from typing import List
import numpy as np

class Transforms:
    def __init__(self, transforms : List) -> None:
        self.transforms = transforms

    def transform(self, image):
        image = np.array(image)  # Ensure input is a NumPy array
        for t in self.transforms:
            image = t.transform(image)
        return image  # Shape: (32, 32, 3)