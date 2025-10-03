"""Monitor and control DropTracker services via GNU screen.

Exposes a Python API and CLI to:
- list services and their status
- start/stop/restart an individual service
- tail recent logs of a service

Intended to be called from PHP (use --json) or shell.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


APP_DIR = "/store/droptracker/disc"
VENV_ACTIVATE = "/store/droptracker/disc/venv/bin/activate"
LOG_DIR = "/store/droptracker/disc/logs"


def _ensure_dirs() -> None:
    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)


_ensure_dirs()


@dataclass(frozen=True)
class ServiceSpec:
    name: str
    screen_name: str
    exec_cmd: str
    working_dir: str = APP_DIR
    venv_activate: str = VENV_ACTIVATE
    port: Optional[int] = None
    log_file: Optional[str] = None

    def log_path(self) -> str:
        file_name = self.log_file or f"{self.name}.log"
        return str(Path(LOG_DIR) / file_name)


def _registry() -> Dict[str, ServiceSpec]:
    return {
        "core": ServiceSpec(
            name="core",
            screen_name="DTcore",
            exec_cmd="python3 main.py",
            port=8080,
            log_file="core.log",
        ),
        "updater": ServiceSpec(
            name="updater",
            screen_name="DT-pu",
            exec_cmd="python3 player_total_update.py",
            port=21474,
            log_file="updater.log",
        ),
        "webhooks": ServiceSpec(
            name="webhooks",
            screen_name="DT-webhooks",
            exec_cmd="python3 webhook_bot.py",
            log_file="webhooks.log",
        ),
        "lootboards": ServiceSpec(
            name="lootboards",
            screen_name="DT-lootboards",
            exec_cmd="python3 _board_generator.py",
            log_file="lootboards.log",
        ),
        "api": ServiceSpec(
            name="api",
            screen_name="DT-api",
            exec_cmd="python3 new_api.py",
            log_file="api.log",
        ),
        "hof": ServiceSpec(
            name="hof",
            screen_name="DT-hof",
            exec_cmd="python3 -m bots.hall_of_fame",
            log_file="hof.log",
        ),
        "heartbeat": ServiceSpec(
            name="heartbeat",
            screen_name="DT-heartbeat",
            exec_cmd="python3 heartbeat.py",
            log_file="heartbeat.log",
        ),
    }


SERVICES: Dict[str, ServiceSpec] = _registry()


def _run(cmd: List[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def _run_shell(command: str) -> subprocess.CompletedProcess:
    return subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)


def _screen_list() -> str:
    p = _run(["screen", "-ls"])
    return p.stdout + p.stderr


def _screen_exists(screen_name: str) -> bool:
    out = _screen_list()
    return re.search(rf"\b{re.escape(screen_name)}\b", out, flags=re.IGNORECASE) is not None


def _screen_pid(screen_name: str) -> Optional[int]:
    out = _screen_list()
    for line in out.splitlines():
        if "." in line and screen_name.lower() in line.lower():
            left = line.strip().split(".", 1)[0]
            try:
                return int(left)
            except ValueError:
                pass
    return None


def _port_in_use(port: Optional[int]) -> bool:
    if port is None:
        return False
    r = _run_shell(f"lsof -t -i:{port} 2>/dev/null | head -n1")
    return r.stdout.strip() != ""


def _kill_port_process(port: Optional[int]) -> Optional[int]:
    if port is None:
        return None
    r = _run_shell(f"lsof -t -i:{port} 2>/dev/null | head -n1")
    pid = r.stdout.strip()
    if not pid:
        return None
    try:
        int_pid = int(pid)
    except ValueError:
        return None
    _run_shell(f"kill -9 {int_pid} 2>/dev/null || sudo kill -9 {int_pid} 2>/dev/null")
    return int_pid


def _start_in_screen(spec: ServiceSpec) -> Tuple[bool, str]:
    if _screen_exists(spec.screen_name):
        return True, f"Screen '{spec.screen_name}' already running"
    if _port_in_use(spec.port):
        _kill_port_process(spec.port)
    cmd = (
        f"cd {shlex.quote(spec.working_dir)} && "
        f"source {shlex.quote(spec.venv_activate)} && "
        f"exec {spec.exec_cmd}"
    )
    screen_cmd = [
        "screen",
        "-dmS",
        spec.screen_name,
        "-L",
        "-Logfile",
        spec.log_path(),
        "bash",
        "-lc",
        cmd,
    ]
    p = _run(screen_cmd)
    if p.returncode == 0 and _screen_exists(spec.screen_name):
        return True, f"Started '{spec.name}'"
    return False, (p.stderr or "Failed to start service")


def _stop_screen(screen_name: str) -> Tuple[bool, str]:
    if not _screen_exists(screen_name):
        return True, f"Screen '{screen_name}' not running"
    p = _run(["screen", "-S", screen_name, "-X", "quit"])
    if p.returncode == 0:
        return True, f"Stopped screen '{screen_name}'"
    pid = _screen_pid(screen_name)
    if pid:
        _run_shell(f"kill -9 {pid} 2>/dev/null || sudo kill -9 {pid} 2>/dev/null")
        return True, f"Killed screen '{screen_name}' (pid {pid})"
    return False, (p.stderr or "Failed to stop screen")


def list_services() -> List[str]:
    return sorted(SERVICES.keys())


def service_status(name: str) -> Dict[str, object]:
    spec = _get(name)
    running = _screen_exists(spec.screen_name)
    return {
        "name": spec.name,
        "screen": spec.screen_name,
        "running": running,
        "pid": _screen_pid(spec.screen_name),
        "port": spec.port,
        "log_file": spec.log_path(),
    }


def start_service(name: str) -> Dict[str, object]:
    spec = _get(name)
    ok, msg = _start_in_screen(spec)
    out = service_status(name)
    out.update({"ok": ok, "message": msg})
    return out


def stop_service(name: str) -> Dict[str, object]:
    spec = _get(name)
    ok, msg = _stop_screen(spec.screen_name)
    out = service_status(name)
    out.update({"ok": ok, "message": msg})
    return out


def restart_service(name: str) -> Dict[str, object]:
    stopped = stop_service(name)
    started = start_service(name)
    out = service_status(name)
    out.update({"stopped": stopped, "started": started})
    return out


def tail_logs(name: str, lines: int = 200) -> Dict[str, object]:
    spec = _get(name)
    log_file = spec.log_path()
    if not os.path.exists(log_file):
        return {"name": name, "log_file": log_file, "exists": False, "lines": []}
    p = _run(["tail", "-n", str(max(0, lines)), log_file])
    return {"name": name, "log_file": log_file, "exists": True, "lines": p.stdout.splitlines()}


def _get(name: str) -> ServiceSpec:
    if name not in SERVICES:
        raise KeyError(f"Unknown service '{name}'. Available: {', '.join(list_services())}")
    return SERVICES[name]


def cli_main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="monitor", description="DropTracker service monitor")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list", help="List services")

    p_stat = sub.add_parser("status", help="Show status for one or all services")
    p_stat.add_argument("service", nargs="?")
    p_stat.add_argument("--json", action="store_true")

    p_start = sub.add_parser("start", help="Start a service")
    p_start.add_argument("service")
    p_start.add_argument("--json", action="store_true")

    p_stop = sub.add_parser("stop", help="Stop a service")
    p_stop.add_argument("service")
    p_stop.add_argument("--json", action="store_true")

    p_restart = sub.add_parser("restart", help="Restart a service")
    p_restart.add_argument("service")
    p_restart.add_argument("--json", action="store_true")

    p_logs = sub.add_parser("logs", help="Tail recent logs for a service")
    p_logs.add_argument("service")
    p_logs.add_argument("-n", "--lines", type=int, default=200)
    p_logs.add_argument("--json", action="store_true")

    args = parser.parse_args(argv)

    try:
        if args.cmd == "list":
            print("\n".join(list_services()))
            return 0
        if args.cmd == "status":
            payload = service_status(args.service) if args.service else {n: service_status(n) for n in list_services()}
            return _emit(payload, getattr(args, "json", False))
        if args.cmd == "start":
            payload = start_service(args.service)
            return _emit(payload, getattr(args, "json", False), key_ok="ok")
        if args.cmd == "stop":
            payload = stop_service(args.service)
            return _emit(payload, getattr(args, "json", False), key_ok="ok")
        if args.cmd == "restart":
            payload = restart_service(args.service)
            ok = bool(payload.get("started", {}).get("ok"))
            return _emit(payload, getattr(args, "json", False), explicit_ok=ok)
        if args.cmd == "logs":
            payload = tail_logs(args.service, lines=args.lines)
            return _emit(payload, getattr(args, "json", False))
    except KeyError as e:
        print(str(e), file=sys.stderr)
        return 2
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return 0


def _emit(payload: Dict[str, object] | object, as_json: bool, key_ok: Optional[str] = None, explicit_ok: Optional[bool] = None) -> int:
    if as_json:
        print(json.dumps(payload, indent=2))
    else:
        if isinstance(payload, dict) and key_ok is None and explicit_ok is None and all(isinstance(v, dict) for v in payload.values()):
            for name in sorted(payload.keys()):
                p = payload[name]
                print(f"- {name}: running={p.get('running')} pid={p.get('pid')} screen={p.get('screen')} port={p.get('port')} log={p.get('log_file')}")
        else:
            print(json.dumps(payload, indent=2))
    if explicit_ok is not None:
        return 0 if explicit_ok else 1
    if key_ok is not None and isinstance(payload, dict):
        return 0 if bool(payload.get(key_ok)) else 1
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    return cli_main(argv)
