from trainer import SparkConfig, Trainer
from models import SVM
import pyspark
from transforms import  RandomHorizontalFlip, Normalize
from transforms import Transforms
transforms = Transforms([
    RandomHorizontalFlip(p=0.345), 
    Normalize(
        mean=(0.4913997551666284, 0.48215855929893703, 0.4465309133731618), 
        std=(0.24703225141799082, 0.24348516474564, 0.26158783926049628)
    )
])

if __name__ == "__main__":
    spark_config = SparkConfig()
    # Add resource settings
    spark_config.receivers = 4  # Increase parallelism
    spark_config.batch_interval = 10  # Increase batch interval to reduce load

    svm = SVM(loss="squared_hinge", penalty="l2")
    trainer = Trainer(svm, "train", spark_config, transforms)
    trainer.train()
    trainer.predict()