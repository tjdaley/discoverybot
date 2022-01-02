"""
objectionresponder.py - Determine how to respond to a discovery objection.

Copyright (c) 2022 by Thomas J. Daley. All Rights Reserved.
"""

import csv
import pandas as pd
from fuzzywuzzy import fuzz


CONFIDENCE_THRESHOLD = 15


class ObjectionResponder(object):
    """
    Respond to discovery objections
    """
    def __init__(self):
        """
        Initialze the instance by loading template objections and responses
        """
        self.objections = self.load_objections()
        self.responses = self.load_responses()

    def get_response(self, objection: str):
        """
        Find a response to this objection. The response is a list of response_codes which can be
        decoded using ObjectionResponder.decode_response().

        Args:
            objection (str): Text of objection from opposing counsel.

        Returns:
            (list): Response codes for suggested responses to this objection
            (int): Confidence in accuracy of response
        """
        max_score = 0
        responses = []
        predicted_objection_key = ''

        # Find the best match.
        for index, template_objection in self.objections.iterrows():
            score = fuzz.token_set_ratio(objection, template_objection['Objection'])
            if score > max_score:
                max_score = score
                predicted_objection_key = template_objection['Key']

        # If the best match meets our confidence threshhold, then send it back to the caller.
        if max_score > CONFIDENCE_THRESHOLD:
            # Dereference a synonym objection patter. A pattern that is a synonym for an objection
            # has the same objection key, prefxied by 'S:'. This allows us to have one responses value
            # for all synonyms.
            if (predicted_objection_key.startswith('S:')):
                predicted_objection_key = predicted_objection_key[2:]

            # Locate the predicted objection
            predicted_objection = self.objections.loc[self.objections['Key'] == predicted_objection_key]
            responses = predicted_objection['Responses'].values.tolist()

        # Check for bullshit qualifiers.
        if 'to the extent' in objection.lower():
            responses.append('B')

        return responses, max_score

    def get_responses(self, objections: str, delimiter: str = '\n'):
        """
        Find responses to a stack of objections. The response is a list of response_codes which can be
        decoded using ObjectionResponder.decode_response().

        Args:
            objections (str): Text of objections from opposing counsel. Can be a list or delimited string.
            delimiter (str): String that delimits multiple objections if *objections* is not a list.

        Returns:
            (list): Response codes for suggested responses to these objections
        """
        responses = []

        if not isinstance(objections, list):
            objections = objections.split(delimiter)

        for objection in objections:
            found_responses, max_score = self.get_response(objection)
            if max_score > CONFIDENCE_THRESHOLD:
                responses.extend(found_responses)

        return responses

    def decode_response(self, response_codes: list) -> list:
        """
        Provides a list of responses based on the list of response_codes.

        Args:
            response_codes (list): List of response codes to decode

        Returns:
            (list): List of responses corresponding to the response codes
        """
        if not isinstance(response_codes, list):
            response_codes = [response_codes]

        unique_codes = list(set(response_codes))

        responses = [self.responses.get(response_code, "No response available") for response_code in unique_codes]
        return responses

    def load_objections(self):
        """
        Loads the patten objections into a Pandas dataframe.

        Args:
            None

        Returns:
            (dataframe): Pandas dataframe with objection patterns
        """
        objection_patterns = pd.read_csv('data/objection_patterns.csv')
        return objection_patterns

    def load_responses(self) -> dict:
        """
        Responses are in a CSV file with these columns:
            0: response_code, which is an index into the response_code value in the objections dataset.
            1: Text of the response

        Args:
            None

        Returns:
            (dict): Dictionary of responses indexed by response_code
        """
        with open('data/responses.csv', mode='r') as inp:
            reader = csv.reader(inp)
            responses = {row[0]: row[1] for row in reader}
        return responses


def main():
    objection_responder = ObjectionResponder()
    objection = input("Objection: ")
    while objection:
        response_codes = objection_responder.get_responses(objection, '|')
        response_texts = objection_responder.decode_response(response_codes)
        print("Responses".center(80, "*"))
        for response_text in response_texts:
            print("\n", response_text)
        objection = input("\n\nObjection: ")


if __name__ == "__main__":
    main()
