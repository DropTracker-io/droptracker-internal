import asyncio
import signal
import subprocess
import socket

from api.health_utils import health_check


shutdown_event = asyncio.Event()


def _signal_handler(signum, frame):
    print(f"Received signal {signum}, initiating graceful shutdown...")
    try:
        shutdown_event.set()
    except Exception:
        pass


def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown."""
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGHUP, _signal_handler)


async def cleanup_port(port: int, max_attempts: int = 10) -> bool:
    """Attempt to free a TCP port by terminating listeners and verifying availability."""
    for attempt in range(max_attempts):
        try:
            result = subprocess.run(['ss', '-tlnp'], capture_output=True, text=True)
            if result.returncode == 0:
                lines = result.stdout.split('\n')
                for line in lines:
                    if f':{port}' in line and 'LISTEN' in line:
                        import re
                        pid_match = re.search(r'pid=(\d+)', line)
                        if pid_match:
                            pid = pid_match.group(1)
                            try:
                                print(f"Killing process {pid} using port {port}")
                                subprocess.run(['kill', '-9', pid], check=False)
                            except Exception as e:
                                print(f"Could not kill process {pid}: {e}")

            subprocess.run(['fuser', '-k', f'{port}/tcp'], capture_output=True, check=False)

            try:
                lsof_result = subprocess.run(['lsof', '-ti', f'tcp:{port}', '-sTCP:LISTEN'], capture_output=True, text=True)
                if lsof_result.returncode == 0 and lsof_result.stdout.strip():
                    for pid in lsof_result.stdout.strip().split('\n'):
                        if pid.isdigit():
                            try:
                                print(f"Killing process {pid} (lsof) using port {port}")
                                subprocess.run(['kill', '-9', pid], check=False)
                            except Exception as e:
                                print(f"Could not kill process {pid} via lsof: {e}")
            except FileNotFoundError:
                pass

        except Exception as e:
            print(f"Error during port cleanup attempt {attempt + 1}: {e}")

        await asyncio.sleep(1)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(("127.0.0.1", port))
            sock.close()
            print(f"Port {port} is now available after attempt {attempt + 1}")
            return True
        except OSError:
            if attempt < max_attempts - 1:
                print(f"Port {port} still in use, retrying cleanup (attempt {attempt + 2}/{max_attempts})...")
                continue
            else:
                print(f"Port {port} still in use after all cleanup attempts")
                return False

    return False


def register_app_lifecycle(app):
    """Attach lightweight lifecycle hooks to the Quart app."""

    @app.before_serving
    async def _on_startup():
        # Optionally run a quick health check before serving
        try:
            ok = await health_check(app)
            if not ok:
                print("Startup health check failed; continuing to serve but flagged unhealthy.")
        except Exception as e:
            print(f"Startup hook error: {e}")

    @app.after_serving
    async def _on_shutdown():
        try:
            # Signal the background watcher waiters if any
            shutdown_event.set()
        except Exception:
            pass


__all__ = [
    "shutdown_event",
    "setup_signal_handlers",
    "cleanup_port",
    "register_app_lifecycle",
]


