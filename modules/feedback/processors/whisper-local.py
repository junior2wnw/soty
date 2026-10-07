"""Offline, fixed local ASR helper. Private result goes to a private file, not logs."""
import argparse
import json
import os
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--input', required=True)
parser.add_argument('--output', required=True)
parser.add_argument('--model', required=True)
parser.add_argument('--language', choices=['ru', 'en'], required=True)
args = parser.parse_args()
os.environ['HF_HUB_OFFLINE'] = '1'
os.environ['TRANSFORMERS_OFFLINE'] = '1'
from faster_whisper import WhisperModel

try:
    media, output, model_dir = Path(args.input), Path(args.output), Path(args.model)
    if not media.is_file() or media.stat().st_size > 1048576 or not model_dir.is_dir() or output.exists():
        raise ValueError('processor_input_invalid')
    model = WhisperModel(str(model_dir), device='cpu', compute_type='int8', cpu_threads=4,
                         num_workers=1, local_files_only=True)
    segments, info = model.transcribe(str(media), language=args.language, beam_size=5,
                                      vad_filter=True, condition_on_previous_text=False)
    text, count = '', 0
    for segment in segments:
        count += 1
        text += (' ' if text else '') + segment.text.strip()
        if count > 256 or len(text.encode('utf-8')) > 32768:
            raise ValueError('processor_output_limit')
    if not 0 <= info.duration <= 120.1:
        raise ValueError('processor_audio_duration')
    result = {'schema': 'soty.feedback.derived-text.v1', 'engine': 'faster-whisper-local',
              'language': args.language, 'text': text, 'durationSeconds': round(info.duration, 3)}
    with output.open('x', encoding='utf-8') as target:
        json.dump(result, target, ensure_ascii=False, separators=(',', ':'))
    print('local-feedback-asr-complete')
except Exception:
    print('local-feedback-asr-failed')
    raise SystemExit(1)
