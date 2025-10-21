#!/usr/bin/env python3
"""
MIRA to Parquet Converter Runner

This script provides a Python interface to run the MiraToParquet Scala application.
It handles argument parsing, validation, and execution of the Scala decoder.
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path


def run_mira_to_parquet(input_file, output_file, scala_package_dir=None):
    """
    Run the MiraToParquet Scala application
    
    Args:
        input_file (str): Path to the input MIRA data file
        output_file (str): Path to the output Parquet file
        scala_package_dir (str): Path to the Scala package directory (optional)
    
    Returns:
        int: Exit code from the Scala application
    """
    # Default to scala_package directory relative to this script
    if scala_package_dir is None:
        script_dir = Path(__file__).parent
        scala_package_dir = script_dir.parent / "scala_package"
    
    scala_package_dir = Path(scala_package_dir)
    
    # Validate paths
    if not Path(input_file).exists():
        print(f"Error: Input file '{input_file}' does not exist", file=sys.stderr)
        return 1
    
    if not scala_package_dir.exists():
        print(f"Error: Scala package directory '{scala_package_dir}' does not exist", file=sys.stderr)
        return 1
    
    if not (scala_package_dir / "build.sbt").exists():
        print(f"Error: No build.sbt found in '{scala_package_dir}'", file=sys.stderr)
        return 1
    
    # Convert to absolute paths
    input_file = Path(input_file).absolute()
    output_file = Path(output_file).absolute()
    
    # Ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Converting MIRA data: {input_file}")
    print(f"Output Parquet file: {output_file}")
    print(f"Using Scala package: {scala_package_dir}")
    print("-" * 60)
    
    # Change to scala package directory and run sbt
    try:
        cmd = f'sbt "runMain MiraToParquet {input_file} {output_file}"'
        result = subprocess.run(
            cmd,
            cwd=scala_package_dir,
            shell=True,
            capture_output=False,
            text=True
        )
        return result.returncode
    
    except subprocess.CalledProcessError as e:
        print(f"Error running SBT: {e}", file=sys.stderr)
        return e.returncode
    except FileNotFoundError:
        print("Error: SBT not found. Please ensure SBT is installed and in your PATH", file=sys.stderr)
        return 1


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Convert MIRA detector data files to Parquet format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Convert test.dat to output.parquet
  %(prog)s test.dat output.parquet
  
  # Convert with absolute paths
  %(prog)s /path/to/data.dat /path/to/output.parquet
  
  # Specify custom Scala package directory
  %(prog)s test.dat output.parquet --scala-dir /custom/path/scala_package
        """
    )
    
    parser.add_argument(
        "input_file",
        help="Path to the input MIRA data file"
    )
    
    parser.add_argument(
        "output_file", 
        help="Path to the output Parquet file"
    )
    
    parser.add_argument(
        "--scala-dir",
        dest="scala_package_dir",
        help="Path to the Scala package directory (default: ../scala_package relative to this script)"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version="mira_to_parquet 1.0.0"
    )
    
    args = parser.parse_args()
    
    # Run the conversion
    exit_code = run_mira_to_parquet(
        args.input_file,
        args.output_file,
        args.scala_package_dir
    )
    
    if exit_code == 0:
        print("-" * 60)
        print("✓ Conversion completed successfully!")
        if Path(args.output_file).exists():
            file_size = Path(args.output_file).stat().st_size
            print(f"✓ Output file size: {file_size / (1024*1024):.1f} MB")
    else:
        print("-" * 60)
        print("✗ Conversion failed!")
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()