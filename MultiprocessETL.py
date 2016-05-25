import os
import gzip
import pygeoip
import time
import datetime
import user_agents
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import boto3
from ast import literal_eval


def error_logger(log_path, error_type, source_path, current_path=""):
    with open("log/" + log_path, 'a') as f:
        line = "Type: " + error_type + ", Source: " + source_path + ", File: " + current_path
        if current_path != "":
            line += ", Current Path: " + current_path
        f.write(line + "\n")


def success_logger(log_path, source_path):
    with open("log/" + log_path, 'a') as f:
        f.write(source_path + "\n")


class Loader:

    def __init__(self, output_key, output_path):
        self.output_key = output_key
        self.output_path = output_path

    def run(self, job):
        try:
            self.output_key.key = self.output_path + job.output_key_name
            self.output_key.set_contents_from_filename(job.path)
            os.remove(job.path)
            success_logger("Success.txt", job.source_key.name)
        except:
            error_logger("Errors.txt", "Impossible to upload", job.source_key.name, job.path)


class Transformer:

    def __init__(self, to_upload_path, localisation_path, job_queue):
        self.to_upload_path = to_upload_path
        self.localisation_data = pygeoip.GeoIP(localisation_path)
        self.job_queue = job_queue

    def get_localisation_from_ip(self):
        self.ip = None
        ips = self.ips.replace(" ", "")
        ips_in_list = ips.split(',')
        for ip in ips_in_list:
            try:
                data = self.localisation_data.record_by_name(ip)
                self.ip = ip
                self.country = data['country_name']
                self.city = data['city']
                self.longitude = data['longitude']
                self.latitude = data['latitude']
                break
            except:
                continue
        if self.ip is None:
            self.latitude = None
            self.longitude = None
            self.country = None
            self.city = None
            error_logger("Errors.txt", "Unknown Localisation from IP", self.job.source_key.name, self.job.path)

    def get_ua_data(self):
        try:
            self.ua = user_agents.parse(self.ua_string)
            self.user_browser = self.ua.browser.family
            self.user_os = self.ua.os.family
            self.is_mobile = self.ua.is_mobile
        except:
            self.ua = None
            self.user_browser = None
            self.user_os = None
            self.is_mobile = None
            error_logger("Errors.txt", "Wrong User Agent Data", self.job.source_key.name, self.job.path)

    def get_timestamp(self):
        try:
            self.datetime = self.date + " " + self.time
            current_datetime = datetime.datetime.strptime(self.datetime, "%Y-%m-%d %H:%M:%S").timetuple()
            self.timestamp = time.mktime(current_datetime)
        except:
            self.datetime = None
            self.timestamp = None
            error_logger("Errors.txt", "Probleme With Timestamp", self.job.source_key.name, self.job.path)

    def get_new_path(self):
        try:
            date_in_list = self.date.split("-")
            year, month, day = date_in_list[0], date_in_list[1], date_in_list[2]
            basename = self.job.source_key.name.split('/')[-1].split('.')[0]
            self.output_key_name = os.path.join(self.to_upload_path, year, month, day, basename + ".json.gz")
        except:
            self.output_key_name = None
            error_logger("Errors.txt", "Wrong New File Path", self.job.source_key.name, self.job.path)

    def get_json_dict(self):
        self.json_dict = {"url": self.url,
                          "timestamp": self.timestamp,
                          "user_id": self.user_id,
                          "ip": self.ips,
                          "location": {"latitude": self.latitude,
                                       "longitude": self.longitude,
                                       "country": self.country,
                                       "city": self.city
                                       },
                          "user_agent": {"mobile": self.is_mobile,
                                         "os_family": self.user_os,
                                         "string": self.ua_string,
                                         "browser_family": self.user_browser
                                         }
                          }

    def process_line(self):
        self.line_in_list = self.line.split("\t")
        if len(self.line_in_list) != 6:
            self.line_in_list = None
            error_logger("Errors.txt", "Wrong Line length", self.job.source_key.name, self.job.path)
        else:
            self.date = self.line_in_list[0]
            self.time = self.line_in_list[1]
            self.user_id = self.line_in_list[2]
            self.url = self.line_in_list[3]
            self.ips = self.line_in_list[4]
            self.ua_string = self.line_in_list[5]
            self.get_ua_data()
            self.get_localisation_from_ip()
            self.get_timestamp()
            self.get_json_dict()

    def run(self, job):
        self.job = job
        self.output = ""
        with gzip.open(job.path, 'rb') as raw_file:
            self.line_nb = 0
            try:
                self.line = raw_file.readline()[:-1]
            except:
                error_logger("Errors.txt", "Not a Gzip file", job.source_key.name, job.path)
                return
            while self.line != "":
                self.process_line()
                self.output += str(self.json_dict) + "\n"
                self.line = raw_file.readline()[:-1]
            self.get_new_path()
            if self.output_key_name is not None:
                current_name = to_upload_path + os.path.basename(self.job.path)
                with gzip.open(current_name, 'wb') as f:
                    f.write(self.output)  # This is done at the end as we cannot append file & want to reduce transfers
                new_job = Job("Upload", current_name, job.source_key, "High", self.output_key_name)
                self.job_queue.add(new_job)


class Extractor:

    def __init__(self, to_process_path, job_queue):
        self.to_process_path = to_process_path
        self.job_queue = job_queue

    def run(self, job):
        output_path = self.to_process_path + os.path.basename(job.path)
        try:
            with open(output_path, 'wb') as f:
                job.source_key.get_contents_to_file(f)
            new_job = Job("Transform", output_path, job.source_key, "Medium")
            self.job_queue.add(new_job)
        except:
            error_logger("Errors.txt", error_type="Impossible to download", source_path=job.source_key.name)


class Job:
    def __init__(self, job_type, path, source_key, priority, output_key_name=""):
        self.type = job_type
        self.path = path
        self.source_key = source_key
        self.priority = priority
        self.output_key_name = output_key_name

    def __repr__(self):
        return "(" + self.type + ", " + self.path + ", " + str(self.source_key.name) + ", " + self.priority\
               + ", " + str(self.output_key_name) + ")"


class JobQueue:

    def __init__(self):
        self.high = []
        self.medium = []
        self.low = []

    def add(self, job):
        if job.priority == "High":
            self.high.append(job)
        elif job.priority == "Medium":
            self.medium.append(job)
        elif job.priority == "Low":
            self.low.append(job)
        else:
            self.low.append(job)

    def get(self):
        if len(self.high) > 0:
            return self.high.pop(0)
        elif len(self.medium) > 0:
            return self.medium.pop(0)
        elif len(self.low) > 0:
            return self.low.pop(0)

    def __repr__(self):
        return "High: " + str(self.high) + "\nMedium: " + str(self.medium) + "\nLow: " + str(self.low)


class MultiprocessETL:

    def __init__(self, aws_login, etl_paths, localisation_path):
        self.set_connection(aws_login)
        self.etl_paths = etl_paths
        aws_input_path, to_process_path, to_upload_path, aws_output_path = etl_paths
        self.job_queue = JobQueue()
        self.fill_aws_folder_to_queue(aws_input_path)
        self.extractor = Extractor(to_process_path, self.job_queue)
        self.transformer = Transformer(to_upload_path, localisation_path, self.job_queue)
        self.loader = Loader(self.output_key, aws_output_path)

    def set_connection(self, aws_login):
        key_id, secret_key, bucket_name, sqs_url = aws_login
        self.aws = S3Connection(key_id, secret_key)
        self.bucket = self.aws.get_bucket(bucket_name)
        self.bucket_file_list = self.bucket.get_all_keys()
        self.output_key = Key(self.bucket)
        self.sqs = boto3.resource('sqs')
        self.aws_queue = self.sqs.Queue(sqs_url)

    def get_job_from_aws_queue(self):
        msg = self.aws_queue.receive_messages()[0]
        # msg_receipt_handle = msg.receipt_handle
        # msg_id = msg.message_id
        msg_as_str = msg.body
        msg_as_dict = literal_eval(msg_as_str)
        bucket = msg_as_dict["Records"][0]["s3"]["bucket"]["name"]
        key_str = msg_as_dict["Records"][0]["s3"]["object"]["key"]
        if bucket == self.bucket.name:
            file_key = Key(self.bucket)
            file_key.key = key_str
            self.fill_file_to_queue(file_key)
        msg.delete()

    def fill_aws_folder_to_queue(self, current_path):
        for file_key in self.bucket.list(current_path):
            if file_key.name[-1] == "/":
                if file_key.name == current_path or file_key.name == current_path + "/":
                    pass  # Dont reprocess the same folder that is in the list
                else:
                    self.fill_aws_folder_to_queue(file_key.name)
            else:
                self.fill_file_to_queue(file_key)

    def fill_file_to_queue(self, file_key):
        if file_key.name[-3:] == ".gz":
            new_job = Job("Extract", file_key.name, file_key, "Low")
            self.job_queue.add(new_job)
        else:
            error_logger("Errors.txt", "File extension: %s not supported" % file_key.name.split(".")[-1], file_key.name)

    def run(self):
        try:
            while True:
                job = self.job_queue.get()
                if job is None:
                    self.get_job_from_aws_queue()
                elif job.type == "Extract":
                    self.extractor.run(job)
                elif job.type == "Transform":
                    self.transformer.run(job)
                elif job.type == "Upload":
                    self.loader.run(job)
                else:
                    error_logger("Errors.txt", "Unkown job %s" % job.type, job.source_key.name, job.path)
        except KeyboardInterrupt:
            "Watch has ended"
            return

if __name__ == "__main__":
    key_id = ""
    secret_key = ""
    bucket_name = ""
    sqs_url = ''
    aws_login = (key_id, secret_key, bucket_name, sqs_url)

    aws_input_path = "data/"  # no "/" before, and one after for bucket.list function
    to_process_path = "to_process/"
    to_upload_path = "to_upload/"
    aws_output_path = "processed/Alfred/"
    etl_paths = (aws_input_path, to_process_path, to_upload_path, aws_output_path)

    localisation_path = "GeoLiteCity.dat"

    my_ETL = MultiprocessETL(aws_login, etl_paths, localisation_path)
    my_ETL.run()
