"""AWS credentials helper for Bedrock access.

Auth uses the standard boto3 credential chain. Configure via env vars
(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, optional AWS_SESSION_TOKEN),
AWS_PROFILE, or an attached IAM role. Set AWS_REGION to a region where
the Bedrock models you want to evaluate are available and enabled for
your account. Optionally set BEDROCK_ENDPOINT_URL to point at a custom
endpoint (e.g. a private proxy or VPC endpoint).
"""

from __future__ import annotations

import os

from botocore.config import Config as BotocoreConfig


def get_aws_credentials(config: BotocoreConfig | None = None) -> dict:
    """Return kwargs for ChatBedrockConverse using the default boto3 chain."""
    kwargs: dict = {}
    if region := (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")):
        kwargs["region_name"] = region
    if endpoint_url := os.getenv("BEDROCK_ENDPOINT_URL"):
        kwargs["endpoint_url"] = endpoint_url
    if config is not None:
        kwargs["config"] = config
    return kwargs
