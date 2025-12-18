#!/usr/bin/env python3
"""
Extract Citi Bike zip files, handling:
- Nested zip files (zips inside zips)
- __MACOSX folders
- Multiple CSVs per archive (for months with >1M trips)
- Duplicate filenames across archives

Supports both NYC and Jersey City (JC) systems:
    python src/ingest.py                  # NYC (default)
    python src/ingest.py --system nyc     # NYC explicitly
    python src/ingest.py --system jc      # Jersey City
"""

import argparse
import json
import os
import zipfile
from datetime import datetime
from pathlib import Path
import hashlib

DATA_DIR = Path(__file__).parent.parent / "data"
LOGS_DIR = Path(__file__).parent.parent / "logs"

# System-specific paths
SYSTEM_PATHS = {
    'nyc': {
        'source': DATA_DIR / "raw_zips",
        'dest': DATA_DIR / "raw_csvs",
    },
    'jc': {
        'source': DATA_DIR / "jc" / "raw_zips",
        'dest': DATA_DIR / "jc" / "raw_csvs",
    }
}


def hash_file(path: Path) -> str:
    """Calculate MD5 hash of a file."""
    hasher = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            hasher.update(chunk)
    return hasher.hexdigest()


def extract_zip(zip_path: Path, dest_dir: Path, manifest: list) -> int:
    """
    Extract a zip file, handling nested zips.
    Returns count of CSVs extracted.
    """
    csv_count = 0
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for name in zf.namelist():
                # Skip macOS metadata
                if '__MACOSX' in name or name.startswith('.'):
                    continue
                
                # Skip directories
                if name.endswith('/'):
                    continue
                
                if name.lower().endswith('.csv'):
                    # Extract CSV
                    # Prefix with zip name to avoid collisions
                    base_name = Path(name).name
                    dest_name = f"{zip_path.stem}_{base_name}" if base_name != zip_path.stem.replace('.csv', '') + '.csv' else base_name
                    dest_path = dest_dir / dest_name
                    
                    # Handle duplicate filenames
                    if dest_path.exists():
                        existing_hash = hash_file(dest_path)
                        zf.extract(name, dest_dir / "_temp")
                        new_hash = hash_file(dest_dir / "_temp" / name)
                        
                        if existing_hash == new_hash:
                            print(f"    Duplicate (identical): {dest_name}")
                            (dest_dir / "_temp" / name).unlink()
                        else:
                            # Different content - rename
                            counter = 1
                            while dest_path.exists():
                                dest_name = f"{zip_path.stem}_{counter}_{base_name}"
                                dest_path = dest_dir / dest_name
                                counter += 1
                            (dest_dir / "_temp" / name).rename(dest_path)
                            print(f"    Extracted (renamed): {dest_name}")
                        
                        # Clean up temp dir
                        for p in (dest_dir / "_temp").rglob("*"):
                            if p.is_file():
                                p.unlink()
                        continue
                    
                    # Normal extraction
                    with zf.open(name) as src, open(dest_path, 'wb') as dst:
                        dst.write(src.read())
                    
                    # Count lines (approximate row count)
                    with open(dest_path, 'r') as f:
                        line_count = sum(1 for _ in f) - 1  # Subtract header
                    
                    manifest.append({
                        'source_zip': zip_path.name,
                        'csv_file': dest_name,
                        'size_bytes': dest_path.stat().st_size,
                        'approx_rows': line_count,
                    })
                    
                    print(f"    Extracted: {dest_name} ({line_count:,} rows)")
                    csv_count += 1
                
                elif name.lower().endswith('.zip'):
                    # Nested zip - extract and recurse
                    print(f"    Found nested zip: {name}")
                    nested_path = dest_dir / "_nested" / Path(name).name
                    nested_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    with zf.open(name) as src, open(nested_path, 'wb') as dst:
                        dst.write(src.read())
                    
                    csv_count += extract_zip(nested_path, dest_dir, manifest)
                    nested_path.unlink()
    
    except zipfile.BadZipFile:
        print(f"    ✗ Bad zip file: {zip_path.name}")
    
    return csv_count


def main():
    parser = argparse.ArgumentParser(description="Extract Citi Bike zip files")
    parser.add_argument("--system", choices=['nyc', 'jc'], default='nyc',
                        help="System to process: 'nyc' (default) or 'jc' (Jersey City)")
    parser.add_argument("--source", type=Path, default=None,
                        help="Source directory with zips (auto-detected based on --system)")
    parser.add_argument("--dest", type=Path, default=None,
                        help="Destination directory for CSVs (auto-detected based on --system)")

    args = parser.parse_args()

    # Set default paths based on system
    system_paths = SYSTEM_PATHS[args.system]
    if args.source is None:
        args.source = system_paths['source']
    if args.dest is None:
        args.dest = system_paths['dest']

    print(f"Processing {args.system.upper()} Citi Bike data")
    
    args.dest.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Clean up any temp directories
    temp_dirs = list(args.dest.glob("_*"))
    for td in temp_dirs:
        if td.is_dir():
            for f in td.rglob("*"):
                if f.is_file():
                    f.unlink()
            td.rmdir()
    
    zip_files = sorted(args.source.glob("*.zip"))
    print(f"Found {len(zip_files)} zip files in {args.source}")
    
    manifest = []
    total_csvs = 0
    
    for zf in zip_files:
        print(f"\n{zf.name}:")
        csv_count = extract_zip(zf, args.dest, manifest)
        total_csvs += csv_count
    
    # Save manifest
    manifest_path = args.dest / "manifest.json"
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    # Save extraction log
    log_path = LOGS_DIR / f"ingest_{args.system}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    log_data = {
        'timestamp': datetime.now().isoformat(),
        'system': args.system,
        'source_dir': str(args.source),
        'dest_dir': str(args.dest),
        'zip_files_processed': len(zip_files),
        'csv_files_extracted': total_csvs,
        'total_approx_rows': sum(m['approx_rows'] for m in manifest),
        'files': manifest,
    }
    with open(log_path, 'w') as f:
        json.dump(log_data, f, indent=2)
    
    print(f"\n✓ Extracted {total_csvs} CSV files")
    print(f"✓ Manifest saved to {manifest_path}")
    print(f"✓ Log saved to {log_path}")


if __name__ == "__main__":
    main()
