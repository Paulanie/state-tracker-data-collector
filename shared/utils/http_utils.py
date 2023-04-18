import io
import os.path
import re
import tempfile
import zipfile

from requests import RequestException, get


def download_file(url: str, output_directory: str = None, auto_extract: bool = False) -> str:
    output_directory = output_directory or tempfile.gettempdir()
    req = get(url, stream=True)
    if "Content-Disposition" in req.headers.keys():
        filename = re.findall("filename=(.+)", req.headers["Content-Disposition"])[0]
    else:
        filename = os.path.basename(url)

    file_path = os.path.join(output_directory, filename)
    with open(file_path, "wb") as f:
        for chunk in req.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

    if filename.endswith("zip") and auto_extract:
        zip_file = zipfile.ZipFile(file_path)
        zip_file.extractall(os.path.join(output_directory, os.path.splitext(filename)[0]))
        return os.path.join(output_directory, os.path.splitext(filename)[0])

    return file_path
