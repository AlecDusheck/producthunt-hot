#!/usr/bin/env python3
"""
Extract ProductHunt posts from HTML files within a date range.
Outputs posts as NDJSON (newline-delimited JSON).
"""

import asyncio
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import AsyncIterator, Dict, Any, List

import aiofiles
import click
import orjson
from tqdm.asyncio import tqdm


def extract_post_objects(text: str) -> List[Dict[str, Any]]:
    """
    Extract complete JSON Post objects from text by finding the pattern and tracking braces.
    """
    posts = []
    search_pattern = '{"__typename":"Post"'
    
    # Find all occurrences of the pattern
    start_pos = 0
    while True:
        pos = text.find(search_pattern, start_pos)
        if pos == -1:
            break
            
        # Extract the complete JSON object starting from this position
        json_start = pos
        brace_count = 0
        in_string = False
        escape_next = False
        i = json_start
        
        while i < len(text):
            char = text[i]
            
            if escape_next:
                escape_next = False
                i += 1
                continue
                
            if char == '\\':
                escape_next = True
                i += 1
                continue
                
            if char == '"' and not escape_next:
                in_string = not in_string
                i += 1
                continue
                
            if not in_string:
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        # Found the end of the JSON object
                        json_end = i + 1
                        json_text = text[json_start:json_end]
                        
                        try:
                            post_obj = orjson.loads(json_text)
                            if isinstance(post_obj, dict) and post_obj.get("__typename") == "Post":
                                posts.append(post_obj)
                        except Exception:
                            # Invalid JSON, skip
                            pass
                        break
            
            i += 1
        
        # Move search position forward
        start_pos = pos + len(search_pattern)
    
    return posts


async def extract_posts_from_file(file_path: Path) -> List[Dict[str, Any]]:
    """
    Extract all Post objects from a single file.
    """
    try:
        async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
            content = await f.read()
        
        posts = extract_post_objects(content)
        return posts
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return []


async def get_all_files(start_date: datetime, end_date: datetime, base_path: Path) -> List[Path]:
    """
    Get all file paths in the date range.
    """
    files = []
    current_date = start_date
    
    while current_date <= end_date:
        year = current_date.strftime("%Y")
        month = current_date.strftime("%m")
        day = current_date.strftime("%d")
        
        file_path = base_path / "history" / year / month / day
        
        if file_path.exists():
            files.append(file_path)
        
        current_date += timedelta(days=1)
    
    return files


async def process_files_with_progress(files: List[Path]) -> AsyncIterator[Dict[str, Any]]:
    """
    Process all files with a progress bar and yield posts.
    """
    pbar = tqdm(total=len(files), desc="Processing files", unit="file")
    
    for file_path in files:
        posts = await extract_posts_from_file(file_path)
        
        for post in posts:
            yield post
        
        pbar.set_postfix({"File": file_path.name, "Posts": len(posts)})
        pbar.update(1)
    
    pbar.close()


async def write_ndjson_with_progress(output_path: Path, posts_iterator: AsyncIterator[Dict[str, Any]]) -> int:
    """
    Write posts to NDJSON file with progress tracking.
    """
    count = 0
    
    async with aiofiles.open(output_path, 'wb') as f:
        async for post in posts_iterator:
            json_bytes = orjson.dumps(post, option=orjson.OPT_APPEND_NEWLINE)
            await f.write(json_bytes)
            count += 1
            
            # Update progress every 100 posts
            if count % 100 == 0:
                print(f"Written {count} posts...", end='\r')
    
    return count


@click.command()
@click.option('--start-date', '-s', required=True, help='Start date (YYYY-MM-DD)')
@click.option('--end-date', '-e', required=True, help='End date (YYYY-MM-DD)')
@click.option('--output', '-o', default='posts.ndjson', help='Output NDJSON file')
@click.option('--base-path', '-b', default='.', help='Base path to the history directory')
def main(start_date: str, end_date: str, output: str, base_path: str):
    """
    Extract ProductHunt posts from files within a date range.
    """
    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        click.echo("Error: Invalid date format. Use YYYY-MM-DD", err=True)
        return
    
    if start_dt > end_dt:
        click.echo("Error: Start date must be before or equal to end date", err=True)
        return
    
    base_path_obj = Path(base_path).resolve()
    output_path = Path(output).resolve()
    
    click.echo(f"Extracting posts from {start_date} to {end_date}...")
    click.echo(f"Base path: {base_path_obj}")
    click.echo(f"Output file: {output_path}")
    
    async def run():
        # Get all files first
        files = await get_all_files(start_dt, end_dt, base_path_obj)
        click.echo(f"Found {len(files)} files to process")
        
        if not files:
            click.echo("No files found in the specified date range")
            return 0
        
        # Process files with progress
        posts_iterator = process_files_with_progress(files)
        count = await write_ndjson_with_progress(output_path, posts_iterator)
        return count
    
    count = asyncio.run(run())
    click.echo(f"\nExtraction complete! {count} posts written to {output_path}")


if __name__ == '__main__':
    main() 