import html
from bs4 import BeautifulSoup


def decode_html_french_string(string: str) -> str:
    return BeautifulSoup(html.unescape(string).replace("\xa0", u" "), features="html.parser").get_text()
