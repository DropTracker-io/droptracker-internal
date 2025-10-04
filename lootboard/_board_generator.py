import asyncio
import signal
import sys
from monitor.sdnotifier import SystemdWatchdog

"""
Lootboard Generator

This process runs as a systemd service to call the actual `board_generator.py` script every 2 minutes.
"""



# Global variables for systemd watchdog
watchdog = None
shutdown_event = asyncio.Event()

# Health check function for systemd watchdog
async def health_check():
    """Simple health check for the lootboard generator"""
    try:
        # Basic health check - service is running if we get here
        return True
    except Exception as e:
        print(f"Health check failed: {e}")
        return False

# Signal handlers for graceful shutdown
def signal_handler(signum, frame):
    """Handle shutdown signals"""
    print(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_event.set()

def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown"""
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)

async def board_loop():
    while not shutdown_event.is_set():
        try:
            print("Starting board generation process...")
            # Use asyncio subprocess to avoid blocking the watchdog
            process = await asyncio.create_subprocess_exec(
                "/store/droptracker/disc/venv/bin/python", ## Venv location
                "-m", "lootboard.board_generator", ## File to execute in subprocess
                stdout=asyncio.subprocess.PIPE, 
                stderr=asyncio.subprocess.PIPE,
                cwd="/store/droptracker/disc"  ## Working directory
            )
            
            # Wait for process with timeout, but don't block watchdog
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(), 
                    timeout=300  # 5 minute timeout
                )
                
                if process.returncode != 0:
                    print(f"Board generation failed with return code {process.returncode}")
                    print(f"Error output: {stderr.decode() if stderr else 'No error output'}")
                else:
                    print("Board generation completed successfully")
                    if stdout:
                        print(f"Output: {stdout.decode()}")
                        
            except asyncio.TimeoutError:
                print("Board generation timed out after 5 minutes, terminating process...")
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=10)
                except asyncio.TimeoutError:
                    print("Process didn't terminate gracefully, killing...")
                    process.kill()
                    await process.wait()
                    
        except Exception as e:
            print(f"Error in board generation: {e}")
        
        print("Board generation process completed & exited. Sleeping for 2 minutes")
        
        # Sleep with interruption check
        for _ in range(120):  # 2 minutes = 120 seconds
            if shutdown_event.is_set():
                break
            await asyncio.sleep(1)

async def main():
    """Main function with systemd watchdog integration"""
    global watchdog
    
    # Setup signal handlers
    setup_signal_handlers()
    
    # Initialize systemd watchdog
    watchdog = SystemdWatchdog()
    watchdog.set_health_check(health_check)
    
    try:
        async with watchdog:
            # Notify systemd that we're ready
            await watchdog.notify_ready()
            print("Systemd watchdog initialized and ready notification sent")
            
            # Start the board generation loop
            await board_loop()
            
            print("Lootboard generator shutting down gracefully...")
            
    except KeyboardInterrupt:
        print("Received keyboard interrupt")
    except Exception as e:
        print(f"Fatal error in main: {e}")
        raise
    finally:
        print("Lootboard generator cleanup completed")

if __name__ == "__main__":
    asyncio.run(main())
