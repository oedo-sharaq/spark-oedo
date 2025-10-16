#!/usr/bin/env python3
"""
Shared utilities for spark-oedo Python CLI tools
"""

import os
import sys
import glob
from pathlib import Path

def find_jar_path(script_dir=None):
    """
    Automatically find the JAR file for the current Scala version
    
    Args:
        script_dir: Directory where the script is located (defaults to parent of this file)
        
    Returns:
        Path to the JAR file
        
    Raises:
        SystemExit: If JAR is not found
    """
    if script_dir is None:
        script_dir = Path(__file__).parent.parent
    else:
        script_dir = Path(script_dir)
    
    target_dir = script_dir / "scala_package" / "target"
    
    if not target_dir.exists():
        print(f"Error: Target directory not found at {target_dir}")
        print("Please build the Scala package first:")
        print(f"  cd {script_dir}/scala_package && sbt package")
        sys.exit(1)
    
    # Look for scala-* directories
    scala_dirs = list(target_dir.glob("scala-*"))
    
    if not scala_dirs:
        print(f"Error: No Scala build directories found in {target_dir}")
        print("Please build the Scala package first:")
        print(f"  cd {script_dir}/scala_package && sbt package")
        sys.exit(1)
    
    # Try each Scala version directory (in case there are multiple)
    jar_paths = []
    for scala_dir in scala_dirs:
        jar_pattern = scala_dir / "spark-oedo-package_*.jar"
        jars = list(scala_dir.glob("spark-oedo-package_*.jar"))
        jar_paths.extend(jars)
    
    if not jar_paths:
        print(f"Error: No JAR files found in {target_dir}")
        print("Available Scala directories:", [d.name for d in scala_dirs])
        print("Please build the Scala package first:")
        print(f"  cd {script_dir}/scala_package && sbt package")
        sys.exit(1)
    
    # Use the first (or only) JAR found
    jar_path = jar_paths[0]
    
    if len(jar_paths) > 1:
        print(f"Info: Multiple JARs found, using: {jar_path.name}")
        print(f"      Available: {[j.name for j in jar_paths]}")
    
    return jar_path

def get_scala_version_from_jar(jar_path):
    """
    Extract Scala version from JAR filename
    
    Args:
        jar_path: Path to the JAR file
        
    Returns:
        Tuple of (scala_version, scala_major_minor)
        e.g., ("2.12.18", "2.12")
    """
    jar_name = Path(jar_path).name
    
    # Extract version from filename like: spark-oedo-package_2.12-1.0.jar
    if "_" in jar_name and "-" in jar_name:
        # Split by underscore and dash to get version parts
        parts = jar_name.replace(".jar", "").split("_")
        if len(parts) >= 2:
            version_part = parts[1].split("-")[0]  # Get "2.12" from "2.12-1.0"
            return version_part, version_part
    
    # Fallback: try to detect from parent directory
    parent_name = Path(jar_path).parent.name
    if parent_name.startswith("scala-"):
        scala_version = parent_name[6:]  # Remove "scala-" prefix
        return scala_version, scala_version
    
    # Default fallback
    return "2.12", "2.12"

def print_jar_info(jar_path):
    """Print information about the detected JAR"""
    scala_version, scala_major = get_scala_version_from_jar(jar_path)
    print(f"✓ Using JAR: {jar_path.name}")
    print(f"  Scala version: {scala_version}")
    print(f"  Full path: {jar_path}")