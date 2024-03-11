from enum import Enum


class Environments(Enum):
    """Available Environments"""

    LOCAL = "LOCAL"
    PREPROD = "PREPROD"
    PROD = "PROD"


class Buckets(Enum):
    """S3 buckets"""

    LOCAL = "/tmp/deltalake"
    PREPROD = "s3://<path-to-my-preprod-s3>/deltalake"
    PROD = "s3://<path-to-my-prod-s3>/deltalake"


class Databases(Enum):
    """Available Databases"""

    LOCAL = "my_database"
    PREPROD = "my_database_preprod"
    PROD = "my_database"


class SaveMode(Enum):
    """Write mode"""

    OVERWRITE = "overwrite"
    APPEND = "append"
