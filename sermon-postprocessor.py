#!/usr/bin/env python3

import argparse
import datetime
import functools
import time
import uuid
import wave

import boto3
import requests

AWS_ACCESS_KEY_ID = None
AWS_SECRET_ACCESS_KEY = None
BUCKET = None

def extract_start_end(audio_file):
    """
    Extracts the first and last two minutes of an audio file. Two temporary
    files are returned that contain the start and end clips.
    """
    wav = wave.open(audio_file, "rb")

    clip_frames = 120 * wav.getframerate()
    frames = wav.readframes(clip_frames)

    first = wave.open("first.wav", "wb")
    first.setparams(wav.getparams()) 
    first.writeframes(frames)
    first.close() 

    end_clip_index = wav.getnframes() - clip_frames - 1
    wav.setpos(end_clip_index)
    frames = wav.readframes(clip_frames)

    second  = wave.open("second.wav", "wb")
    second.setparams(wav.getparams())
    second.writeframes(frames)
    second.close()

    end_clip_start_time = float(end_clip_index) / wav.getframerate()

    wav.close()

    return ("first.wav", "second.wav", end_clip_start_time)

def trim_audio_file(audio_file, start, end):
    in_wav = wave.open(audio_file, "rb")

    start_frame = int(start * in_wav.getframerate())
    end_frame = int(end * in_wav.getframerate())
    frames = end_frame - start_frame

    out_wav = wave.open("output.wav", "wb")
    out_wav.setparams(in_wav.getparams())
    in_wav.setpos(start_frame)
    out_wav.writeframes(in_wav.readframes(frames))

    in_wav.close()
    out_wav.close()

def wait_for_jobs(jobs):
    transcribe = boto3.client("transcribe", aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name="us-east-2")
    failed = []
    status = {job: False for job in jobs}
    while not functools.reduce(lambda x, y: x and y, status.values()):
        remaining = [job for job in status.keys() if not status[job]]

        for job in remaining:
            current_status = transcribe.get_transcription_job(TranscriptionJobName=job)
            current_status = current_status['TranscriptionJob']['TranscriptionJobStatus']
            if current_status == "COMPLETED":
                status[job] = True
            elif current_status == "FAILED":
                status[job] = True
                failed.append(job)

            if False in status.values():
                time.sleep(5)
    return failed

def get_earliest_time_for_job(job):
    transcribe = boto3.client("transcribe", aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name="us-east-2")
    job_info = transcribe.get_transcription_job(TranscriptionJobName=job)
    uri = job_info["TranscriptionJob"]["Transcript"]["TranscriptFileUri"]
    response = requests.get(uri)
    if response.status_code != requests.codes.ok:
        response.raise_for_status()
    response = response.json()
    words = []
    for item in response["results"]["items"]:
        if item["type"] == "pronunciation":
            words.append(item)
    return float(words[0]["start_time"])

def get_latest_time_for_job(job):
    transcribe = boto3.client("transcribe", aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name="us-east-2")
    job_info = transcribe.get_transcription_job(TranscriptionJobName=job)
    uri = job_info["TranscriptionJob"]["Transcript"]["TranscriptFileUri"]
    response = requests.get(uri)
    if response.status_code != requests.codes.ok:
        response.raise_for_status()
    response = response.json()
    words = []
    for item in response["results"]["items"]:
        if item["type"] == "pronunciation":
            words.append(item)
    return float(words[-1]["end_time"])

def s3_upload(f, dest):
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name="us-east-2")
    content = open(f, "rb")
    s3.put_object(Bucket="sermon-postprocessor", Key=dest, Body=content)

def start_transcription(uri):
    job_name = str(uuid.uuid4())
    transcribe = boto3.client("transcribe", aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name="us-east-2")
    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={"MediaFileUri": uri},
        MediaFormat="wav",
        LanguageCode="en-US")
    return job_name

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="post process raw sermon audio")    
    ap.add_argument("raw")
    args = ap.parse_args()

    # First extract the start and end
    print("extracting start and end clips...")
    start_clip, end_clip, end_clip_start_time = extract_start_end(args.raw)

    # Upload the clips to S3
    print("uploading start and end to s3...")
    s3_upload(start_clip, "start_clip.wav")
    s3_upload(end_clip, "end_clip.wav")

    # Start transcription jobs
    print("starting start and end transcription...")
    begin_clip_job = start_transcription()
    end_clip_job = start_transcription()

    # Wait for the jobs to complete
    print("waiting for transcription jobs to complete...")
    wait_for_jobs([begin_clip_job, end_clip_job])

    # Get the timecodes from the transcription.
    start_time = get_earliest_time_for_job(begin_clip_job) - 2.5 
    end_time = end_clip_start_time + get_latest_time_for_job(end_clip_job) + 2.5
    print("Sermon start @ {}s".format(start_time))
    print("Sermon end @ {}s".format(end_time))

    trim_audio_file(args.raw, start_time, end_time)
