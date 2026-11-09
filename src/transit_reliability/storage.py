"""Paths that can be local folders or s3:// locations.

Spark reads and writes local folders here. For s3:// inputs and outputs the command line copies
files in and out with boto3, which works the same against real AWS and LocalStack (set
AWS_ENDPOINT_URL for LocalStack). On EMR, Spark can use s3:// paths directly instead.
"""

from __future__ import annotations

import os
import pathlib
import urllib.request
import zipfile


def is_s3(path: str) -> bool:
    return path.startswith("s3://")


def split_s3(uri: str) -> tuple[str, str]:
    bucket, _, prefix = uri[len("s3://"):].partition("/")
    return bucket, prefix.strip("/")


def s3_client():
    import boto3

    return boto3.client("s3")


def download_prefix(uri: str, local_dir: str) -> str:
    bucket, prefix = split_s3(uri)
    client = s3_client()
    paginator = client.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            relative = item["Key"][len(prefix):].lstrip("/") if prefix else item["Key"]
            target = pathlib.Path(local_dir) / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(bucket, item["Key"], str(target))
            count += 1
    if count == 0:
        raise FileNotFoundError(f"No files under {uri}")
    return local_dir


def upload_dir(local_dir: str, uri: str) -> None:
    bucket, prefix = split_s3(uri)
    client = s3_client()
    for path in pathlib.Path(local_dir).rglob("*"):
        if path.is_file() and not path.name.startswith(".") and not path.name.endswith(".crc"):
            key = "/".join(filter(None, [prefix, str(path.relative_to(local_dir))]))
            client.upload_file(str(path), bucket, key)


def local_input(path: str, workdir: str, name: str) -> str:
    """A local folder holding the input: downloads s3:// inputs first."""
    if is_s3(path):
        return download_prefix(path, os.path.join(workdir, name))
    return path


def gtfs_folder(source: str, workdir: str) -> str:
    """A folder of GTFS .txt files from a folder, a .zip file, an http(s) URL, or s3:// zip."""
    if os.path.isdir(source):
        return source
    zip_path = source
    if source.startswith(("http://", "https://")):
        zip_path = os.path.join(workdir, "gtfs.zip")
        request = urllib.request.Request(source, headers={"User-Agent": "transit-reliability"})
        with urllib.request.urlopen(request, timeout=120) as response, open(zip_path, "wb") as out:
            out.write(response.read())
    elif is_s3(source):
        bucket, key = split_s3(source)
        zip_path = os.path.join(workdir, "gtfs.zip")
        s3_client().download_file(bucket, key, zip_path)
    target = os.path.join(workdir, "gtfs")
    with zipfile.ZipFile(zip_path) as archive:
        archive.extractall(target)
    return target
