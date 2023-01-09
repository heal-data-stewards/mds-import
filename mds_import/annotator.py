# Annotate an MDS data dictionary using a variety of node normalization techniques.
import json
import logging
import os
import shutil
import urllib.parse
from urllib3.util.retry import Retry

import requests
from requests.adapters import HTTPAdapter

# Data dictionary path.
DD_INPUT_DIR = 'data/dictionaries'

# Data dictionary output path.
ANNOTATIONS_OUTPUT_DIR = 'data/annotated'

# Configuration: get the Nemo-Serve URL and figure out the annotate path.
NEMOSERVE_URL = os.getenv('NEMOSERVE_URL', 'https://med-nemo.apps.renci.org/')
NEMOSERVE_ANNOTATE_ENDPOINT = urllib.parse.urljoin(NEMOSERVE_URL, '/annotate/')
NEMOSERVE_MODEL_NAME = "token_classification"

# Configuration: get the SAPBERT URL and figure out the annotate path.
SAPBERT_URL = os.getenv('SAPBERT_URL', 'https://med-nemo-sapbert.apps.renci.org/')
SAPBERT_ANNOTATE_ENDPOINT = urllib.parse.urljoin(SAPBERT_URL, '/annotate/')
SAPBERT_MODEL_NAME = "sapbert"

# Allow multiple attempts.
session = requests.Session()
adapter = HTTPAdapter(max_retries=Retry(total=4, backoff_factor=1, allowed_methods=None, status_forcelist=[429, 500, 502, 503, 504]))
session.mount("http://", adapter)
session.mount("https://", adapter)


# Annotate some text.
def annotate_text(text):
    # Make a request to Nemo-Serve to annotate this text.
    request = {
        "text": text,
        "model_name": NEMOSERVE_MODEL_NAME
    }
    logging.debug(f"Request: {request}")
    response = session.post(NEMOSERVE_ANNOTATE_ENDPOINT, json=request)
    logging.debug(f"Response: {response.content}")
    if not response.ok:
        logging.error(f"Received error from {NEMOSERVE_ANNOTATE_ENDPOINT}: {response}")
        return []
    annotated = response.json()
    logging.info(f" - Nemo result: {annotated}")

    # For each annotation, query it with SAPBERT.
    count_sapbert_annotations = 0
    track_sapbert = []
    track_token_classification = annotated['denotations']
    for token in track_token_classification:
        text = token['text']

        assert text, f"Token {token} does not have any text!"

        logging.debug(f"Querying SAPBERT with {token['text']}")
        request = {
            "text": token['text'],
            "model_name": SAPBERT_MODEL_NAME
        }
        response = session.post(SAPBERT_ANNOTATE_ENDPOINT, json=request)
        logging.debug(f"Response from SAPBERT: {response.content}")
        if not response.ok:
            logging.error(f"Received error from {SAPBERT_ANNOTATE_ENDPOINT}: {response}")
            return []

        result = response.json()
        logging.debug(f"Response as JSON: {result}")
        assert result, f"Could not annotate text {token['text']} in Sapbert: {response}"

        # We're only interested in the closest match for now.
        if len(result) > 0:
            closest_match = result[0]

            denotation = dict(token)
            denotation['obj'] = f"MESH:{closest_match['curie']} ({closest_match['label']}, score: {closest_match['distance_score']})"
            count_sapbert_annotations += 1
            # This is fine for PubAnnotator format (I think?), but PubAnnotator editors
            # don't render this.
            # denotation['label'] = result[0]
            track_sapbert.append(
                denotation
            )

    return track_sapbert

# Annotate all data dictionaries and store them in data/annotated
def annotate_dds():
    # Turn on logging.
    logging.basicConfig(level=logging.INFO)

    # Delete the downloaded files.
    shutil.rmtree(ANNOTATIONS_OUTPUT_DIR, ignore_errors=True)
    os.makedirs(ANNOTATIONS_OUTPUT_DIR, exist_ok=True)

    # Iterate over data dictionaries.
    # If we need to recurse through subdirectories, we should use pathlib.Path instead.
    count_files = 0
    count_fields = 0
    count_unannotated_fields = 0
    unannotated_fields = []
    count_annotations = 0
    for filename in os.listdir(DD_INPUT_DIR):
        if not filename.lower().endswith('.json'):
            logging.info(f"Skipping {filename}")
            continue

        count_files += 1
        with open(os.path.join(DD_INPUT_DIR, filename), 'r') as fp:
            doc = json.load(fp)

        dd_outer = doc.get('data_dictionary', dict())

        # One data dictionary has a list here. This is probably a bug, but we might as well handle the
        # case for now.
        if isinstance(dd_outer, list):
            fields = dd_outer
        else:
            fields = dd_outer.get('data_dictionary', dict())

        logging.info(f"{filename} contains {len(fields)} fields.")

        for field in fields:
            count_fields += 1
            name = field.get('name', '')
            desc = field.get('description', '')
            type_format = field.get('type', '')
            if 'format' in field:
                type_format += ':' + field['format']
            logging.info(f" - {name} ({type_format}): {desc}")

            encodings = field.get('encodings', dict())
            encodings_as_str = ""
            for key, value in encodings.items():
                encoding_as_str = f"{key}: {value}"
                encodings_as_str += encoding_as_str + "\n"
                logging.info(f"   - {encoding_as_str}")

            text_to_annotate = f"{name}: {desc}\n{encodings_as_str}"

            try:
                annotations = annotate_text(text_to_annotate)
                if len(annotations) == 0:
                    unannotated_fields.append(f'{filename}:{name}')
                    logging.info(f" -0- no annotations found for {name} with text '{text_to_annotate}'")
                for annot in annotations:
                    count_annotations += 1
                    logging.info(f" + {annot['text']}: {annot['obj']}")
            except Exception as e:
                logging.error(f"Could not annotate '{text_to_annotate}': {e}")

            logging.info("")

    logging.info(f"Completed annotating {count_fields} fields from {count_files} files with a total of {count_annotations} annotations.")
    logging.info(f"{len(unannotated_fields)} fields had no annotations: {unannotated_fields}")

