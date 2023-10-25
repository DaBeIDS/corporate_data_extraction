import json
import argparse
import glob
import os
import time
from datetime import timedelta
from flask import Flask, Response, request
import shutil
import traceback
from data_extractor.code.utils.s3_communication import S3Communication

from .components import Extractor
from .config import config
from .components import Curator


app = Flask(__name__)


def create_directory(directory_name):
    os.makedirs(directory_name, exist_ok=True)
    for filename in os.listdir(directory_name):
        file_path = os.path.join(directory_name, filename)
        try:
            os.unlink(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))


@app.route("/liveness")
def liveness():
    return Response(response={}, status=200)


@app.route('/extract/')
def run_extraction():
    # args = json.loads(request.args['payload'])

    # Local args dictionary
    args = {"project_name": 'TEST'}
    args.update({"s3_usage": False})
    extraction_settings = {}
    extraction_settings.update({"use_extractions": False})
    extraction_settings.update({"seed": 42})
    extraction_settings.update({"min_paragraph_length": 20})
    extraction_settings.update({"annotation_folder": None})
    extraction_settings.update({"skip_extracted_files": False})
    extraction_settings.update({"store_extractions": True})
    args.update({"extraction": extraction_settings})

    # Load the extraction settings from args
    project_name = args["project_name"]
    extraction_settings = args['extraction']

    # Create path strings for the different folders
    base_data_project_folder = config.DATA_FOLDER / project_name
    pdf_folder = base_data_project_folder / 'interim' / 'pdfs'
    base_interim_folder = base_data_project_folder / 'interim' / 'ml'
    extraction_folder = base_interim_folder / 'extraction'
    annotation_folder = base_interim_folder / 'annotations'

    # Create the folders if they do not already exist
    os.makedirs(extraction_folder, exist_ok=True)
    os.makedirs(annotation_folder, exist_ok=True)
    os.makedirs(pdf_folder, exist_ok=True)
    
    s3_usage = args["s3_usage"]
    if s3_usage:
        s3_settings = args["s3_settings"]
        project_prefix = s3_settings['prefix'] + "/" + project_name + '/data'
        # init s3 connector
        s3c_main = S3Communication(
            s3_endpoint_url=os.getenv(s3_settings['main_bucket']['s3_endpoint']),
            aws_access_key_id=os.getenv(s3_settings['main_bucket']['s3_access_key']),
            aws_secret_access_key=os.getenv(s3_settings['main_bucket']['s3_secret_key']),
            s3_bucket=os.getenv(s3_settings['main_bucket']['s3_bucket_name']),
        )
        s3c_interim = S3Communication(
            s3_endpoint_url=os.getenv(s3_settings['interim_bucket']['s3_endpoint']),
            aws_access_key_id=os.getenv(s3_settings['interim_bucket']['s3_access_key']),
            aws_secret_access_key=os.getenv(s3_settings['interim_bucket']['s3_secret_key']),
            s3_bucket=os.getenv(s3_settings['interim_bucket']['s3_bucket_name']),
        )
        if extraction_settings['use_extractions']:
            s3c_main.download_files_in_prefix_to_dir(project_prefix + '/output/TEXT_EXTRACTION', 
                                                     extraction_folder)
        s3c_interim.download_files_in_prefix_to_dir(project_prefix + '/interim/ml/annotations', 
                                                     annotation_folder)
        if args['mode'] == 'train':
            s3c_main.download_files_in_prefix_to_dir(project_prefix + '/input/pdfs/training', 
                                                     pdf_folder)
        else:
            s3c_main.download_files_in_prefix_to_dir(project_prefix + '/input/pdfs/inference', 
                                                     pdf_folder)
    
    pdfs = glob.glob(os.path.join(pdf_folder, "*.pdf"))
    if len(pdfs) == 0:
        msg = "No pdf files found in the pdf directory ({})".format(pdf_folder)
        return Response(msg, status=500)

    """
    # TODO Why do we need annotation at all? Actually extraction does not need that!
    annotation_files = glob.glob(os.path.join(annotation_folder, "*.csv"))
    if len(annotation_files) == 0:
        msg = "No annotations.csv file found on S3."
        return Response(msg, status=500)
    elif len(annotation_files) > 2:
        msg = "Multiple annotations.csv files found on S3."
        return Response(msg, status=500)
    """

    # Update the settings in config to the user settings
    config.STAGE = 'extract'
    config.SEED = extraction_settings["seed"]
    config.PDFTextExtractor_kwargs['min_paragraph_length'] = extraction_settings["min_paragraph_length"]
    config.PDFTextExtractor_kwargs['annotation_folder'] = extraction_settings["annotation_folder"]
    config.PDFTextExtractor_kwargs['skip_extracted_files'] = extraction_settings["skip_extracted_files"]

    # Create an extractor class element with the newly updated extraction settings
    ext = Extractor(config.EXTRACTORS)
    try:
        t1 = time.time()
        ext.run_folder(pdf_folder, extraction_folder)
        t2 = time.time()
    except Exception as e:
        msg = "Error during extraction\nException:" + str(e)
        return Response(msg, status=500)

    extracted_files = os.listdir(extraction_folder)
    if len(extracted_files) == 0:
        msg = "Extraction Failed. No file was found in the extraction directory ({})"\
            .format(extraction_folder)
        return Response(msg, status=500)

    failed_to_extract = ""
    for pdf in pdfs:
        pdf = os.path.basename(pdf)
        pdf = pdf.split(".pdf")[0]
        if not any([pdf in e for e in extracted_files]):
            failed_to_extract += pdf + "\n"

    msg = "Extraction finished successfully."
    if len(failed_to_extract) > 0:
        msg += "The following pdf files, however,  did not get extracted:\n" + failed_to_extract
        
    if s3_usage:
        s3c_interim.upload_files_in_dir_to_prefix(extraction_folder,
                                                  project_prefix + '/interim/ml/extraction')
        # clear folder
        create_directory(extraction_folder)
        create_directory(annotation_folder)
        create_directory(pdf_folder)
    time_elapsed = str(timedelta(seconds=t2 - t1))
    msg += "\nTime elapsed:{}".format(time_elapsed)
    return Response(msg, status=200)


@app.route('/curate/')
def run_curation():
    #args = json.loads(request.args['payload'])

    # Local args dictionary
    args = {"project_name": 'TEST'}
    args.update({"s3_usage": False})
    curation_settings = {}
    curation_settings.update({"retrieve_paragraph": False})
    curation_settings.update({"neg_pos_ratio": 1})
    curation_settings.update({"columns_to_read": ["company", "source_file", "source_page", "kpi_id", "year", "answer", "data_type", "relevant_paragraphs"]})
    curation_settings.update({"company_to_exclude": []})
    curation_settings.update({"create_neg_samples": True})
    curation_settings.update({"min_length_neg_sample": 50})
    curation_settings.update({"seed": 41})
    args.update({"curation": curation_settings})

    # Load the extraction settings from args
    project_name = args["project_name"]
    curation_settings = args['curation']

    BASE_DATA_PROJECT_FOLDER = config.DATA_FOLDER / project_name
    BASE_INTERIM_FOLDER = BASE_DATA_PROJECT_FOLDER / 'interim' / 'ml'
    extraction_folder = BASE_INTERIM_FOLDER / 'extraction'
    config.CURATION_FOLDER = BASE_INTERIM_FOLDER / 'curation'
    annotation_folder = BASE_INTERIM_FOLDER / 'annotations'
    config.KPI_FOLDER = BASE_DATA_PROJECT_FOLDER / 'interim' / 'kpi_mapping'

    os.makedirs(extraction_folder, exist_ok=True)
    os.makedirs(config.CURATION_FOLDER, exist_ok=True)
    os.makedirs(annotation_folder, exist_ok=True)
    os.makedirs(config.KPI_FOLDER, exist_ok=True)
    
    s3_usage = args["s3_usage"]
    if s3_usage:
        s3_settings = args["s3_settings"]
        project_prefix = s3_settings['prefix'] + "/" + project_name + '/data'
        # init s3 connector
        s3c_main = S3Communication(
            s3_endpoint_url=os.getenv(s3_settings['main_bucket']['s3_endpoint']),
            aws_access_key_id=os.getenv(s3_settings['main_bucket']['s3_access_key']),
            aws_secret_access_key=os.getenv(s3_settings['main_bucket']['s3_secret_key']),
            s3_bucket=os.getenv(s3_settings['main_bucket']['s3_bucket_name']),
        )
        s3c_interim = S3Communication(
            s3_endpoint_url=os.getenv(s3_settings['interim_bucket']['s3_endpoint']),
            aws_access_key_id=os.getenv(s3_settings['interim_bucket']['s3_access_key']),
            aws_secret_access_key=os.getenv(s3_settings['interim_bucket']['s3_secret_key']),
            s3_bucket=os.getenv(s3_settings['interim_bucket']['s3_bucket_name']),
        )
        s3c_main.download_files_in_prefix_to_dir(project_prefix + '/input/kpi_mapping', config.KPI_FOLDER)
        s3c_interim.download_files_in_prefix_to_dir(project_prefix + '/interim/ml/extraction', extraction_folder)
        s3c_main.download_files_in_prefix_to_dir(project_prefix + '/input/annotations',
                                                 annotation_folder)

    #shutil.copyfile(os.path.join(config.KPI_FOLDER, "kpi_mapping.csv"), "/app/code/kpi_mapping.csv")

    config.STAGE = 'curate'
    config.TextCurator_kwargs['retrieve_paragraph'] = curation_settings['retrieve_paragraph']
    config.TextCurator_kwargs['neg_pos_ratio'] = curation_settings['neg_pos_ratio']
    config.TextCurator_kwargs['columns_to_read'] = curation_settings['columns_to_read']
    config.TextCurator_kwargs['company_to_exclude'] = curation_settings['company_to_exclude']
    config.TextCurator_kwargs['min_length_neg_sample'] = curation_settings['min_length_neg_sample']
    config.SEED = curation_settings['seed']

    #try:
    if len(config.CURATORS) != 0:
        cur = Curator(config.CURATORS)
        cur.run(extraction_folder, annotation_folder, config.CURATION_FOLDER)
    #except Exception as e:
        #msg = "Error during curation\nException:" + str(repr(e)) + traceback.format_exc()
        #return Response(msg, status=500)
    
    if s3_usage:
        s3c_interim.upload_files_in_dir_to_prefix(config.CURATION_FOLDER, 
                                                  project_prefix + '/interim/ml/curation')
        # clear folder
        create_directory(config.KPI_FOLDER)
        create_directory(extraction_folder)
        create_directory(annotation_folder)
        create_directory(config.CURATION_FOLDER)
    
    return Response("Curation OK", status=200)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='inference server')
    # Add the arguments
    parser.add_argument('--port',
                        type=int,
                        default=4000,
                        help='port to use for the extract server')
    args_server = parser.parse_args()
    port = args_server.port
    app.run(host="0.0.0.0", port=port)
