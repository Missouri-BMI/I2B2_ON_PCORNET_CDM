import random
import math

class RandomNormalGenerator:

    @staticmethod
    def random_normal(mean=0.0, stddev=1.0, threshold=10):
        while True:
            u = random.random() * 2 - 1
            v = random.random() * 2 - 1
            s = u**2 + v**2

            if s != 0.0 and s < 1.0:
                s = math.sqrt(-2 * math.log(s) / s)
        
                if stddev * s * u > threshold: 
                    return mean + threshold
                elif stddev * s * u < -1 * threshold:
                    return mean - threshold
                else:
                    return mean + stddev * s * u