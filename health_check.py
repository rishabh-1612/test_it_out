import psutil
import socket
import datetime
from flask import Blueprint, jsonify
import asyncio

from Config import SCALE_CPU_THRESHOLD, SCALE_MEMORY_THRESHOLD, SCALE_THREADS_THRESHOLD


health_bp = Blueprint("health", __name__)


def should_scale(cpu_usage: float, mem_usage: float, active_threads: int):
    return (
        cpu_usage > SCALE_CPU_THRESHOLD
        or mem_usage > SCALE_MEMORY_THRESHOLD
        or active_threads > SCALE_THREADS_THRESHOLD
    )


@health_bp.route("/")
def health_check():
    cpu_usage = psutil.cpu_percent(interval=1)
    mem = psutil.virtual_memory()
    mem_usage = mem.percent
    active_threads = len(psutil.Process().threads())
    cpu_count = psutil.cpu_count()
    load_avg_1m, load_avg_5m, load_avg_15m = map(lambda x: x/cpu_count, psutil.getloadavg())

    # timestamp = datetime.datetime.utcnow().isoformat()
    timestamp = datetime.datetime.now().isoformat()
    hostname = socket.gethostname()

    scale_recommendation = should_scale(cpu_usage, mem_usage, active_threads)

    response = {
        "timestamp": timestamp,
        "hostname": hostname,
        "cpu_count": cpu_count,
        "cpu_percent": cpu_usage,
        "load_average": {
            "1min": load_avg_1m,
            "5min": load_avg_5m,
            "15min": load_avg_15m
        },
        "memory_percent": mem_usage,
        "active_threads": active_threads,
        "scale_needed": scale_recommendation,
    }

    status_code = 200 if not scale_recommendation else 206
    return jsonify(response), status_code