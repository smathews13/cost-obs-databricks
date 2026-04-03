#!/usr/bin/env python3
"""Databricks App logs client using /logz/batch endpoint."""

import json
import os
import sys
import time
from datetime import datetime
from typing import Optional, List, Dict, Any

from dotenv import load_dotenv

# Load environment variables from .env.local
load_dotenv('.env.local')

# Import our DatabricksAppClient
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dba_client import DatabricksAppClient


class LogzClient:
  """Client for fetching logs from Databricks App /logz/batch endpoint."""

  def __init__(self, app_url: Optional[str] = None):
    """Initialize with optional app URL.
    
    Args:
        app_url: Base URL of the Databricks app. If not provided, will be auto-detected from DATABRICKS_APP_NAME
    """
    # Use DatabricksAppClient for all API calls
    self.client = DatabricksAppClient(app_url)
    self.app_url = self.client.app_url
    self.batch_url = self.app_url + '/logz/batch'

  def fetch_logs(self, search_query: str = '', watch: bool = False, interval: int = 5) -> List[Dict[str, Any]]:
    """Fetch logs from the /logz/batch endpoint.
    
    Args:
        search_query: Optional search query to filter logs
        watch: If True, continuously fetch new logs
        interval: Interval between fetches when watching (seconds)
    
    Returns:
        List of log entries
    """
    try:
      # Use the DatabricksAppClient to make the request
      logs = self.client.get('/logz/batch')
      
      # Filter logs if search query provided
      if search_query and isinstance(logs, list):
        logs = [log for log in logs if search_query.lower() in log.get('message', '').lower()]
      
      return logs if isinstance(logs, list) else []
      
    except Exception as e:
      print(f'❌ Error fetching logs: {e}')
      return []

  def display_logs(self, logs: List[Dict[str, Any]], last_timestamp: Optional[int] = None) -> int:
    """Display logs in a formatted way.
    
    Args:
        logs: List of log entries to display
        last_timestamp: Only show logs newer than this timestamp
    
    Returns:
        The timestamp of the most recent log entry
    """
    if not logs:
      return last_timestamp or 0
    
    # Sort logs by timestamp
    logs = sorted(logs, key=lambda x: x.get('timestamp', 0))
    
    max_timestamp = last_timestamp or 0
    displayed_count = 0
    
    for log in logs:
      timestamp = log.get('timestamp', 0)
      
      # Skip logs we've already shown
      if last_timestamp and timestamp <= last_timestamp:
        continue
      
      source = log.get('source', 'UNKNOWN')
      message = log.get('message', '')
      
      # Format timestamp
      if timestamp:
        dt = datetime.fromtimestamp(timestamp)
        timestamp_str = dt.strftime('%H:%M:%S')
        max_timestamp = max(max_timestamp, timestamp)
      else:
        timestamp_str = '        '
      
      # Color code by source
      if source == 'SYSTEM':
        source_str = 'SYSTEM'
      elif source == 'APP':
        source_str = 'APP   '
      else:
        source_str = source[:6].ljust(6)
      
      print(f'[{timestamp_str}] {source_str}: {message}')
      displayed_count += 1
    
    return max_timestamp

  def stream_logs(self, search_query: str = '', duration: int = 0, interval: int = 5):
    """Stream logs by periodically fetching from batch endpoint.
    
    Args:
        search_query: Optional search query to filter logs
        duration: How long to stream logs in seconds (0 = forever)
        interval: How often to fetch new logs (seconds)
    """
    print(f'Fetching logs from: {self.batch_url}')
    if search_query:
      print(f"Search query: '{search_query}'")
    if duration > 0:
      print(f'Streaming for {duration} seconds (fetching every {interval}s)...')
    else:
      print(f'Streaming continuously (fetching every {interval}s, Ctrl+C to stop)...')
    print('-' * 50)
    
    start_time = time.time()
    last_timestamp = None
    total_displayed = 0
    
    try:
      while True:
        # Check if we should stop (duration limit)
        if duration > 0 and time.time() - start_time > duration:
          break
        
        # Fetch logs
        logs = self.fetch_logs(search_query)
        
        if logs:
          # Display only new logs
          new_timestamp = self.display_logs(logs, last_timestamp)
          
          # Count how many new logs were displayed
          if last_timestamp:
            new_logs = [l for l in logs if l.get('timestamp', 0) > last_timestamp]
            total_displayed += len(new_logs)
          else:
            total_displayed = len(logs)
          
          last_timestamp = new_timestamp
        
        # If not watching, just show once and exit
        if duration == 0 and not search_query:
          # Default behavior: show latest logs once
          break
        
        # Wait before next fetch if we're in watch mode
        if duration != 0 or search_query:
          time.sleep(interval)
    
    except KeyboardInterrupt:
      print(f'\n⏹️  Stopped by user')
    
    if duration > 0:
      print(f'\n⏰ Completed after {duration} seconds')
    print(f'📊 Displayed {total_displayed} log messages')


def main():
  """CLI interface for the logz client."""
  import argparse

  parser = argparse.ArgumentParser(
    description='Fetch logs from Databricks App using /logz/batch endpoint',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  # Fetch and display latest logs once (auto-detects app URL)
  python dba_logz.py
  
  # Stream logs for 30 seconds (fetch every 5 seconds)
  python dba_logz.py --duration 30
  
  # Stream logs continuously
  python dba_logz.py --duration -1
  
  # Search for ERROR messages
  python dba_logz.py --search ERROR
  
  # Search for specific text for 60 seconds
  python dba_logz.py --search "database" --duration 60
  
  # Fetch logs every 2 seconds
  python dba_logz.py --duration 30 --interval 2
  
  # Or specify app URL explicitly
  python dba_logz.py --app_url https://app.databricksapps.com
    """
  )
  parser.add_argument('--app_url', help='Base URL of the Databricks app (optional, auto-detected from DATABRICKS_APP_NAME if not provided)')
  parser.add_argument('--search', default='', help='Search query to filter logs')
  parser.add_argument('--duration', type=int, default=0, 
                      help='How long to stream logs in seconds (0=once, -1=forever)')
  parser.add_argument('--interval', type=int, default=5,
                      help='Interval between fetches when streaming (seconds)')

  args = parser.parse_args()

  client = LogzClient(args.app_url)
  
  # Adjust duration for continuous streaming
  duration = 0 if args.duration == -1 else args.duration
  
  client.stream_logs(args.search, duration, args.interval)


if __name__ == '__main__':
  main()