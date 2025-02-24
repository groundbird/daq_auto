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
        return False
        
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
        # logging.info("Reset command executed successfully")
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
                # logging.info(f"Waiting {sleep_seconds:.1f} seconds until next event")
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

def main():
    parser = ArgumentParser(description='Schedule reset commands for 24-hour astronomical observations')
    parser.add_argument('--target', default='moon',
                       help='Target celestial body (default: moon)')
    parser.add_argument('--elevation', type=float, default=70,
                       help='Elevation angle for observations (default: 70)')
    parser.add_argument('--sun-avoid', type=float, default=60,
                       help='Sun avoidance angle (default: 60)')
    
    args = parser.parse_args()
    
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
