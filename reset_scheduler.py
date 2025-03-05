#!/usr/bin/env python3
'''
Reset command scheduler
Executes reset commands for a single 24-hour period
'''
import subprocess
import asyncio
from datetime import datetime, timezone
import logging
from argparse import ArgumentParser
import pandas as pd
from astropy.time import Time
import os
import signal
import sys
import errno

from planner import get_plan_oneday  # Import directly from paste module

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('reset_scheduler.log'),
        logging.StreamHandler()
    ]
)

# Lock file directory - using existing directory
LOCK_DIR = '/home/gb/obstool/daq_auto/lock'

# Global variable to store the lock file path
CURRENT_LOCK_FILE = None

def signal_handler(sig, frame):
    """Handle Ctrl+C and other termination signals"""
    if CURRENT_LOCK_FILE and os.path.exists(CURRENT_LOCK_FILE):
        try:
            os.remove(CURRENT_LOCK_FILE)
            print(f"\nLock file removed: {CURRENT_LOCK_FILE}")
        except Exception as e:
            print(f"\nError removing lock file: {e}")
    
    print("\nScheduler terminated by user.")
    sys.exit(0)

def check_lock(target_name):
    """
    Check if a lock exists for the given target
    Returns True if locked, False otherwise
    """
    lock_file_path = os.path.join(LOCK_DIR, f"{target_name}.lock")
    
    # If lock file exists, check if it's stale
    if os.path.exists(lock_file_path):
        try:
            with open(lock_file_path, 'r') as f:
                lock_data = f.read().strip().split(',')
                
            if len(lock_data) >= 3:
                pid = int(lock_data[0])
                username = lock_data[1]
                timestamp = lock_data[2]
                
                # Check if process is still running
                try:
                    # Sending signal 0 checks if process exists without actually sending a signal
                    os.kill(pid, 0)
                    # Process exists, lock is valid
                    print(f"\nERROR: Target '{target_name}' is already being monitored.")
                    print(f"Lock held by user {username} (PID {pid}) since {timestamp}")
                    return True
                except OSError as e:
                    if e.errno == errno.ESRCH:
                        # Process doesn't exist, lock is stale
                        print(f"Removing stale lock for '{target_name}' (PID {pid} no longer exists)")
                        os.remove(lock_file_path)
                        return False
                    else:
                        # Other OSError (like permission denied), assume lock is valid
                        print(f"\nERROR: Target '{target_name}' is already being monitored.")
                        print(f"Lock held by user {username} (PID {pid}) since {timestamp}")
                        print(f"Cannot verify process status: {e}")
                        return True

        except Exception as e:
            # Error reading lock file, assume it's invalid and remove it
            print(f"Invalid lock file for '{target_name}', removing: {e}")
            try:
                os.remove(lock_file_path)
            except:
                pass
            return False
    
    return False

def create_lock(target_name):
    """
    Create a lock file for the given target
    Returns the lock file path if successful, None if failed
    """
    global CURRENT_LOCK_FILE
    
    # Check if lock already exists
    if check_lock(target_name):
        return None
    
    lock_file_path = os.path.join(LOCK_DIR, f"{target_name}.lock")
    
    try:
        # Create lock file with PID, username, and timestamp
        pid = os.getpid()
        try:
            username = os.getlogin()
        except:
            username = f"pid-{pid}"
        
        timestamp = datetime.now().isoformat()
        
        # Write lock info as CSV-like format for easy parsing
        with open(lock_file_path, 'w') as f:
            f.write(f"{pid},{username},{timestamp}")
        
        os.chmod(lock_file_path, 0o666)

        CURRENT_LOCK_FILE = lock_file_path
        print(f"Lock acquired for target '{target_name}'")
        return lock_file_path
    
    except Exception as e:
        print(f"Error creating lock file: {e}")
        return None

def remove_lock(lock_file_path):
    """Remove the lock file"""
    global CURRENT_LOCK_FILE
    
    if lock_file_path and os.path.exists(lock_file_path):
        try:
            os.remove(lock_file_path)
            CURRENT_LOCK_FILE = None
            logging.info(f"Lock removed: {lock_file_path}")
        except Exception as e:
            logging.error(f"Error removing lock file: {e}")

def create_reset_command(gb_number, target_name):
    """Common function to generate reset command"""
    return f"/home/gb/obstool/daq_client.py GB{gb_number} --targetname {target_name} --reset"

def get_schedule(target_name, elevation=70, sun_avoid=60):
    """
    Get schedule using get_plan_oneday function
    """
    try:
        # Get schedule for current time
        current_time = Time.now()
        schedule = get_plan_oneday(
            time=current_time,
            line=elevation,
            body_name=target_name,
            sun_avoid=sun_avoid
        )
        return schedule
    except Exception as e:
        logging.error(f"Error getting schedule: {e}")
        raise

def display_schedule_and_confirm(schedule, target_name):
    """
    Display schedule and ask for user confirmation
    """
    print("\n=== Scheduled Command Execution Schedule (24 hours) ===")
    print(f"Target celestial body: {target_name}")
    print("\nScheduled reset commands:")
    
    reset_commands = []
    for _, row in schedule.iterrows():
        event_time = pd.to_datetime(row['Time (UTC)'])
        action = row['Action']

        if 'Restart GB' in action:
            gb_number = action[10:12]
            cmd = create_reset_command(gb_number, target_name)
            print(f"Time: {event_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            print(f"Command: {cmd}")
            print("---")
            reset_commands.append((event_time, gb_number, cmd))
    
    if not reset_commands:
        print("No reset commands scheduled for execution within 24 hours.")
        return False, []
        
    while True:
        response = input("\nDo you want to start execution with this schedule? (Y/N): ").strip().upper()
        if response in ['Y', 'N']:
            return response == 'Y', reset_commands
        print("Please answer with 'Y' or 'N'.")

async def execute_delayed_sky_command(gb_number, delay_minutes=20):
    try:
        await asyncio.sleep(delay_minutes * 60)
        sky_cmd = f"/home/gb/obstool/daq_client.py GB{gb_number} --targetname sky"
        proc = await asyncio.create_subprocess_exec(
            *sky_cmd.split(),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            logging.error(f"Sky command failed: {stderr.decode()}")
        else:
            logging.info("Sky command executed successfully")
    except Exception as e:
        logging.error(f"Error in sky command execution: {e}")

def execute_reset_command(cmd, gb_number, delay_minutes=20):
    """
    Execute reset command for observation equipment
    """
    logging.info(f"Executing reset command: {cmd}")
    try:
        subprocess.run(cmd.split(), check=True)
        print("Reset command executed successfully")
        # Execute sky command asynchronously
        asyncio.create_task(execute_delayed_sky_command(gb_number, delay_minutes))
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing reset command: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

async def run_scheduler(target_name='moon', elevation=70, sun_avoid=60, delay_minutes=20):
    """
    Execute schedule for 24 hours
    """
    # Create lock for the target
    lock_file_path = create_lock(target_name)
    if not lock_file_path:
        print("\nCannot proceed with scheduling. Please choose a different target or wait.")
        sys.exit(1)
    
    try:
        # Get schedule
        logging.info(f"Getting 24-hour schedule for {target_name}")
        schedule = get_schedule(target_name, elevation, sun_avoid)
        
        # Display schedule and ask for confirmation
        confirmed, reset_commands = display_schedule_and_confirm(schedule, target_name)
        if not confirmed:
            logging.info("Schedule execution cancelled by user")
            return
        
        # Execute confirmed commands
        executed_commands = 0
        now = datetime.now(timezone.utc)
        
        for event_time, gb_number, cmd in reset_commands:
            # Skip past events
            if event_time < now:
                continue

            # Sleep until execution time
            sleep_seconds = (event_time - datetime.now(timezone.utc)).total_seconds()
            if sleep_seconds > 0:
                print(f"Waiting {sleep_seconds:.1f} seconds until next event")
                await asyncio.sleep(sleep_seconds)

            # Execute saved command
            execute_reset_command(cmd, gb_number, delay_minutes)
            print('Waiting for 30 sec before starting next DAQ to avoid using CPU power at the same time...')
            await asyncio.sleep(30)
            executed_commands += 1

        # Wait for all sky commands to complete (up to 20 minutes after last command)
        if executed_commands > 0:
            await asyncio.sleep((delay_minutes+3)*60)  # Wait for last sky command to complete

        logging.info(f"All scheduled commands executed. Total commands: {executed_commands}")
        print(f"\n24-hour schedule completed. Number of commands executed: {executed_commands}")

    except Exception as e:
        logging.error(f"Error in scheduler: {e}")
        print(f"\nAn error occurred: {e}")
    finally:
        # Always remove the lock when done
        remove_lock(lock_file_path)

def list_active_targets():
    """
    List all lock files regardless of process status
    This version works across different user accounts
    """
    lock_files = []
    
    if not os.path.exists(LOCK_DIR):
        print(f"\nLock directory {LOCK_DIR} does not exist.")
        return
    
    for filename in os.listdir(LOCK_DIR):
        if filename.endswith('.lock') and not filename.startswith('.'):
            target_name = filename[:-5]  # Remove .lock extension
            lock_file_path = os.path.join(LOCK_DIR, filename)
            
            try:
                with open(lock_file_path, 'r') as f:
                    lock_data = f.read().strip().split(',')
                
                if len(lock_data) >= 3:
                    pid = int(lock_data[0])
                    username = lock_data[1]
                    timestamp = lock_data[2]
                    
                    # Try to check if process exists, but don't rely on it
                    process_status = "Unknown"
                    try:
                        os.kill(pid, 0)
                        process_status = "Running"
                    except OSError:
                        # Can't determine status due to permissions or process not existing
                        pass
                    
                    lock_files.append((target_name, pid, username, timestamp, process_status))
            except Exception as e:
                print(f"Invalid lock file for '{target_name}': {e}")
    
    if lock_files:
        print("\n=== Observatory Lock Files ===")
        for target, pid, username, timestamp, status in lock_files:
            print(f"Target: {target}")
            print(f"  - Owner: {username}")
            print(f"  - Process ID: {pid}")
            print(f"  - Created: {timestamp}")
            print(f"  - Status: {status}")
            print("---")
    else:
        print("\nNo lock files found.")

def main():
    # Set up signal handlers for proper cleanup
    signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # kill command
    signal.signal(signal.SIGHUP, signal_handler) 
    
    parser = ArgumentParser(description='Schedule reset commands for 24-hour astronomical observations')
    parser.add_argument('--target', default='moon',
                       help='Target celestial body (default: moon)')
    parser.add_argument('--elevation', type=float, default=70,
                       help='Elevation angle for observations (default: 70)')
    parser.add_argument('--sun-avoid', type=float, default=60,
                       help='Sun avoidance angle (default: 60)')
    parser.add_argument('--list', action='store_true',
                       help='List currently active observation targets')
    
    args = parser.parse_args()
    
    # List active targets if requested
    if args.list:
        list_active_targets()
        return
    
    logging.info(f"Starting 24-hour reset scheduler for {args.target}")
    logging.info(f"Elevation angle: {args.elevation}°")
    logging.info(f"Sun avoidance angle: {args.sun_avoid}°")
    
    # Create and run async loop
    asyncio.run(run_scheduler(
        target_name=args.target,
        elevation=args.elevation,
        sun_avoid=args.sun_avoid,
        delay_minutes = 20
    ))

if __name__ == '__main__':
    main()
