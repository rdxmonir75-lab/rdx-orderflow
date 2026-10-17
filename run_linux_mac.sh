#!/usr/bin/env bash
set -e
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
[ -f .env ] || cp .env.example .env
set -a; [ ! -f .env ] || source .env; set +a
python -m uvicorn app:app --host 127.0.0.1 --port 8000
