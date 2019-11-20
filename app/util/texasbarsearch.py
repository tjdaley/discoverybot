import json
import re
import requests

BEGINNING_TAG = '<div class="BNeawe vvjwJb AP7Wnd">'
ENDING_TAG = '</div></div></div></div></div></div></div></div>'
BASE_URL = 'https://google.com/search?safe=off&q={}+site:texasbar.com'
INFO_REGEX = r'\|\s+(.+)-.+Bar Card Number:\s(\d{8}).+Date:\s+(.{10}).+:\s+([a-zA-Z\s,]+)\.\s+(.+)'  # NOQA


class TexasBarSearch(object):
    """
    Encapsulates our Texas Bar attorney search.
    """
    def __init__(self):
        self.cache = {}

    def find(self, bar_number: str) -> dict:
        """
        Search for an attorney by bar number.

        Args:
            bar_number (str): Bar number to search for

        Returns:
            (dict): Description of attorney or None
        """
        if bar_number in self.cache:
            return self.cache[bar_number]

        result = None
        url = BASE_URL.format(bar_number)
        page = requests.get(url).content.decode('latin1')
        start = page.find(BEGINNING_TAG)
        end = page.find(ENDING_TAG, start)
        excerpt = page[start:end+len(ENDING_TAG)]
        plain_text = remove_tags(excerpt)

        match = re.search(INFO_REGEX, plain_text)
        if match:
            located_bar_number = match.group(2).strip()
            result = {
                'name': match.group(1).strip(),
                'bar_number': located_bar_number,
                'license_date': match.group(3).strip(),
                'primary_practice': match.group(4).strip(),
                'address': match.group(5).strip(),
            }
            self.cache[located_bar_number] = result
        return result


def remove_tags(s: str) -> str:
    """
    Remove tags and replace them with tilde ("~") characters.

    Args:
        s (str): String to clean up.
    Returns:
        (str): Cleaned string.
    """
    in_tag = False
    plain_text = ''
    field = ''
    for character in s:
        if in_tag:
            if character == '>':
                in_tag = False
        else:
            if character == '<':
                in_tag = True
                if field > '':
                    plain_text += '~' + field
                    field = ''
            else:
                field += character
    return plain_text


def main():
    searcher = TexasBarSearch()
    bar_numbers = ['24055537', '24059643', '24059537']
    for bar_number in bar_numbers:
        atty = searcher.find(bar_number)
        print(json.dumps(atty, indent=4))


if __name__ == '__main__':
    main()
