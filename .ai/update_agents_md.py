#!/usr/bin/env python3
"""
Auto-update system for agents.md
Scans codebase and updates the documentation with current structure and recent changes.

Usage:
    python .ai/update_agents_md.py          # Run manually
    python .ai/update_agents_md.py --watch  # Watch for changes (dev mode)
"""

import os
import re
import json
import argparse
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Any
import subprocess


class AgentsMdUpdater:
    """Maintains and updates agents.md documentation."""
    
    def __init__(self, root_dir: str = "."):
        self.root = Path(root_dir).resolve()
        self.agents_md_path = self.root / "agents.md"
        self.ai_dir = self.root / ".ai"
        self.state_file = self.ai_dir / "agents_state.json"
        
        # Ensure .ai directory exists
        self.ai_dir.mkdir(exist_ok=True)
    
    def scan_project_structure(self) -> Dict[str, Any]:
        """Scan the project and extract current structure."""
        structure = {
            "backend_modules": [],
            "frontend_components": [],
            "dashboard_pages": [],
            "services": [],
            "recent_files": []
        }
        
        # Scan BackEnd modules
        backend_path = self.root / "BackEnd"
        if backend_path.exists():
            for item in backend_path.iterdir():
                if item.is_dir() and not item.name.startswith(".") and not item.name.startswith("__"):
                    structure["backend_modules"].append(item.name)
        
        # Scan FrontEnd components
        components_path = self.root / "FrontEnd" / "components"
        if components_path.exists():
            for f in components_path.glob("*.py"):
                if not f.name.startswith("__"):
                    structure["frontend_components"].append(f.stem)
        
        # Scan dashboard pages
        dashboard_lib = self.root / "FrontEnd" / "pages" / "dashboard_lib"
        if dashboard_lib.exists():
            for f in dashboard_lib.glob("*.py"):
                if not f.name.startswith("__"):
                    structure["dashboard_pages"].append(f.stem)
        
        # Scan services
        services_path = self.root / "BackEnd" / "services"
        if services_path.exists():
            for f in services_path.glob("*.py"):
                if not f.name.startswith("__"):
                    structure["services"].append(f.stem)
        
        # Get recently modified files (last 7 days)
        try:
            result = subprocess.run(
                ["git", "log", "--name-only", "--since=\"7 days ago\"", "--pretty=format:"],
                cwd=self.root,
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                recent = set(line.strip() for line in result.stdout.split("\n") if line.strip() and line.endswith(".py"))
                structure["recent_files"] = sorted(recent)[:20]  # Top 20
        except Exception:
            pass
        
        return structure
    
    def extract_recent_changes_from_git(self) -> List[Dict[str, str]]:
        """Extract recent changes from git history."""
        changes = []
        
        try:
            # Get recent commits with messages
            result = subprocess.run(
                ["git", "log", "--since=\"30 days ago\"", "--pretty=format:%h|%s|%ad", "--date=short", "--no-merges"],
                cwd=self.root,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                for line in result.stdout.strip().split("\n"):
                    if "|" in line:
                        parts = line.split("|", 2)
                        if len(parts) >= 3:
                            changes.append({
                                "commit": parts[0],
                                "message": parts[1],
                                "date": parts[2]
                            })
        except Exception as e:
            print(f"Warning: Could not extract git history: {e}")
        
        return changes[:10]  # Last 10 commits
    
    def update_agents_md(self) -> bool:
        """Update the agents.md file with current information."""
        if not self.agents_md_path.exists():
            print(f"Error: {self.agents_md_path} not found!")
            return False
        
        # Read current content
        content = self.agents_md_path.read_text(encoding="utf-8")
        
        # Update last updated date
        today = date.today().isoformat()
        content = re.sub(
            r"> \*\*Last Updated\*\*: \d{4}-\d{2}-\d{2}",
            f"> **Last Updated**: {today}",
            content
        )
        
        # Scan current structure
        structure = self.scan_project_structure()
        
        # Update Recent Changes section
        recent_changes = self.extract_recent_changes_from_git()
        if recent_changes:
            changes_section = "### Recent Changes (Auto-Updated)\n\n"
            for change in recent_changes[:5]:
                changes_section += f"- **{change['date']}**: {change['message']} ({change['commit']})\n"
            
            # Find and replace or append to recent changes
            if "### Recent Changes (Auto-Updated)" in content:
                content = re.sub(
                    r"### Recent Changes \(Auto-Updated\).*?(?=\n## |\Z)",
                    changes_section,
                    content,
                    flags=re.DOTALL
                )
        
        # Save updated content
        self.agents_md_path.write_text(content, encoding="utf-8")
        
        # Save state
        state = {
            "last_update": datetime.now().isoformat(),
            "structure": structure,
            "changes_count": len(recent_changes)
        }
        self.state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")
        
        print(f"✅ Updated {self.agents_md_path}")
        print(f"   - Last updated: {today}")
        print(f"   - Backend modules: {len(structure['backend_modules'])}")
        print(f"   - Frontend components: {len(structure['frontend_components'])}")
        print(f"   - Recent changes: {len(recent_changes)}")
        
        return True
    
    def should_update(self) -> bool:
        """Check if update is needed based on file changes."""
        if not self.state_file.exists():
            return True
        
        try:
            state = json.loads(self.state_file.read_text(encoding="utf-8"))
            last_update = datetime.fromisoformat(state["last_update"])
            
            # Update if more than 1 hour passed
            return (datetime.now() - last_update).total_seconds() > 3600
        except Exception:
            return True
    
    def watch_mode(self):
        """Watch for file changes and auto-update."""
        import time
        
        print("👁️  Watch mode active. Press Ctrl+C to stop.")
        print("   Checking every 60 seconds...\n")
        
        try:
            while True:
                if self.should_update():
                    self.update_agents_md()
                else:
                    print(f"⏭️  Skipped (last update < 1 hour ago)")
                
                time.sleep(60)
        except KeyboardInterrupt:
            print("\n👋 Watch mode stopped.")


def main():
    parser = argparse.ArgumentParser(description="Update agents.md documentation")
    parser.add_argument("--watch", action="store_true", help="Watch for changes continuously")
    parser.add_argument("--force", action="store_true", help="Force update even if recently updated")
    args = parser.parse_args()
    
    updater = AgentsMdUpdater()
    
    if args.watch:
        updater.watch_mode()
    else:
        if args.force or updater.should_update():
            updater.update_agents_md()
        else:
            print("⏭️  Update not needed (last update < 1 hour ago)")
            print("   Use --force to update anyway")


if __name__ == "__main__":
    main()
