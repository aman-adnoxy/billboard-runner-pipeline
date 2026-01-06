import re
from datetime import datetime
from typing import List, Dict, Optional

class LogParser:
    """
    Parses raw log lines into structured data.
    """
    
    # Regex to match Prefect log format: 16:54:10.152 | INFO    | Flow run '...' - Message
    # Group 1: Timestamp
    # Group 2: Level
    # Group 3: Source (broadly)
    # Group 4: Message
    LOG_PATTERN = re.compile(r"^(\d{2}:\d{2}:\d{2}\.\d{3})\s+\|\s+([A-Z]+)\s+\|\s+(.*?)\s+-\s+(.*)$")

    @staticmethod
    def parse_logs(log_lines: List[str]) -> List[Dict[str, str]]:
        """
        Parses a list of raw log strings into a structured list of dictionaries.
        Handling multi-line logs by attaching them to the previous entry's details.
        """
        structured_logs = []
        current_entry = None

        for line in log_lines:
            line = line.rstrip()
            if not line:
                continue

            match = LogParser.LOG_PATTERN.match(line)
            if match:
                # New Log Entry
                timestamp, level, source, message = match.groups()
                
                # Save previous entry if exists
                if current_entry:
                    structured_logs.append(current_entry)
                
                current_entry = {
                    "timestamp": timestamp,
                    "level": level.strip(),
                    "source": source.strip(),
                    "message": message.strip(),
                    "details": "" # For multi-line stack traces etc.
                }
            else:
                # Continuation of previous entry (e.g. Traceback)
                if current_entry:
                    if current_entry["details"]:
                        current_entry["details"] += "\n" + line
                    else:
                        current_entry["details"] = line
                else:
                    # Orphaned line at start, treat as raw info or separate entry
                    # Ideally shouldn't happen if logs start clean, but safe fallback:
                    structured_logs.append({
                        "timestamp": "",
                        "level": "RAW",
                        "source": "System",
                        "message": line,
                        "details": ""
                    })

        # Append the last entry
        if current_entry:
            structured_logs.append(current_entry)

        return structured_logs

    @staticmethod
    def get_color_for_level(level: str) -> str:
        """Returns a hex color code suitable for the UI based on log level."""
        level = level.upper()
        if level == "INFO":
            return "#3498db" # Blue
        elif level == "WARNING":
            return "#f39c12" # Orange
        elif level == "ERROR" or level == "CRITICAL":
            return "#e74c3c" # Red
        elif level == "SUCCESS":
             return "#2ecc71" # Green
        else:
            return "#95a5a6" # Gray
