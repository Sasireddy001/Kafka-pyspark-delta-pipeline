$ErrorActionPreference = "Stop"
$env:PYTHONPATH = "$PSScriptRoot\..\src"
python -m pipeline.streaming_job
