import os
import asyncio

from api import create_app
from api.lifecycle import setup_signal_handlers, cleanup_port, register_app_lifecycle, shutdown_event
from monitor.sdnotifier import SystemdWatchdog
from api.health_utils import health_check


async def main():
    app = create_app()
    register_app_lifecycle(app)

    print("Starting DropTracker API server...")
    setup_signal_handlers()
    print("Signal handlers setup complete")

    watchdog = SystemdWatchdog()
    watchdog.set_health_check(lambda: health_check(app))
    print("Systemd watchdog initialized")

    try:
        async with watchdog:
            await watchdog.notify_ready()
            print("Systemd watchdog initialized and ready notification sent")

            port = int(os.environ.get("API_PORT", 31323))
            print(f"Checking for existing processes on port {port}...")
            port_available = await cleanup_port(port)
            if not port_available:
                print(f"Desired port {port} unavailable after cleanup attempts; exiting.")
                raise SystemExit(1)

            print("Creating Quart app task...")
            app_task = asyncio.create_task(app.run_task(host="127.0.0.1", port=port))
            print(f"Quart app task created on port {port}, waiting for completion or shutdown...")

            done, pending = await asyncio.wait(
                [app_task, asyncio.create_task(shutdown_event.wait())],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if shutdown_event.is_set():
                print("Shutdown requested, stopping API server...")
                if not app_task.done():
                    app_task.cancel()
                    try:
                        await app_task
                    except asyncio.CancelledError:
                        pass
            else:
                print("App task completed unexpectedly")

            print("API server shutting down gracefully...")
    finally:
        print("API server cleanup completed")


if __name__ == "__main__":
    asyncio.run(main())


