#!/usr/bin/env python3
"""
TikTok End-to-End Crawler — CLI Entry Point
============================================

Follows the template-crawler pattern: argparse CLI -> Controllers -> Input/Output drivers.

Usage:
    # Scrape-only (prints JSON to stdout)
    python main.py crawler --mode scrape --keyword "persib"

    # Scrape + save to file
    python main.py crawler --mode scrape --keyword "persib" --count 20 --max-pages 3 --output-file results.json

    # Full pipeline: scrape + publish to Kafka
    python main.py crawler --mode full --keyword "persib" -d kafka -o tiktok.posts.raw --bootstrap-servers localhost:9092

    # Scrape + index to Elasticsearch
    python main.py crawler --mode full --keyword "persib" -d elasticsearch -o tiktok_posts --elasticsearch-hosts http://localhost:9200
"""

import argparse
import asyncio
import logging
import sys

if __name__ == "__main__":
    argp = argparse.ArgumentParser(
        description="TikTok End-to-End Crawler",
    )

    argp.add_argument("-c", "--config", dest="config", type=str, default="config.yaml")
    argp.add_argument("-s", "--source", dest="source", type=str, default=None)
    argp.add_argument("-d", "--destination", dest="destination", type=str, default=None)
    argp.add_argument("-i", "--input", dest="input", type=str, default=None)
    argp.add_argument("-o", "--output", dest="output", type=str, default=None)

    # Kafka
    argp.add_argument("--bootstrap-servers", dest="bootstrap_servers", type=str, default="localhost:9092")
    # Elasticsearch
    argp.add_argument("--elasticsearch-hosts", dest="elasticsearch_hosts", type=str, default="http://localhost:9200")

    # --- Subcommands ---
    argp_sub = argp.add_subparsers(title="action", dest="which", help="-h / --help to see usage")

    argp_crawler = argp_sub.add_parser("crawler", help="Run the TikTok crawler")
    argp_crawler.add_argument("--mode", dest="mode", type=str, default="scrape",
                              choices=["scrape", "full"],
                              help="scrape: JSON only | full: crawl + output driver")
    argp_crawler.add_argument("--type", dest="type", type=str, default="search",
                              choices=["search", "user-posts", "user-story"],
                              help="search: keyword search | user-posts: posts by username | user-story: stories by username")
    argp_crawler.add_argument("--keyword", dest="keyword", type=str, default=None,
                              help="Search keyword (for --type search)")
    argp_crawler.add_argument("--unique-id", dest="unique_id", type=str, default=None,
                              help="TikTok username / unique_id (for --type user-posts, e.g. @persib)")
    argp_crawler.add_argument("--count", dest="count", type=int, default=12)
    argp_crawler.add_argument("--max-pages", dest="max_pages", type=int, default=1)
    argp_crawler.add_argument("--hd", dest="hd", type=int, default=1, choices=[0, 1])
    argp_crawler.add_argument("--cookies", dest="cookies", type=str, default=None,
                              help="Cookie string for authenticated endpoints (e.g. cf_clearance=...; key=val)")
    argp_crawler.add_argument("-o", "--output", dest="output_file", type=str, default=None,
                             help="Save JSON to file (scrape mode) or output destination name (full mode)")
    argp_crawler.add_argument("--output-file", dest="output_file_legacy", type=str, default=None,
                              help=argparse.SUPPRESS)  # legacy alias, hidden
    argp_crawler.add_argument("--pretty", action="store_true", default=False,
                              help="Pretty-print JSON output")
    argp_crawler.add_argument("--log-level", dest="log_level", type=str, default="INFO")
    # Output-driver flags (duplicated from parent so they work after 'crawler' too)
    argp_crawler.add_argument("-d", "--destination", dest="destination_crawler", type=str, default=None,
                              help="Output driver: kafka | elasticsearch | file | std")
    argp_crawler.add_argument("--bootstrap-servers", dest="bootstrap_servers_crawler", type=str, default=None,
                              help="Kafka broker list")
    argp_crawler.add_argument("--elasticsearch-hosts", dest="elasticsearch_hosts_crawler", type=str, default=None,
                              help="ES host URL")

    args = argp.parse_args()

    # Merge: crawler subparser values take precedence over parent parser defaults
    if getattr(args, "bootstrap_servers_crawler", None):
        args.bootstrap_servers = args.bootstrap_servers_crawler
    if getattr(args, "elasticsearch_hosts_crawler", None):
        args.elasticsearch_hosts = args.elasticsearch_hosts_crawler
    if getattr(args, "destination_crawler", None):
        args.destination = args.destination_crawler

    # --- Setup logging ---
    log_level = getattr(args, "log_level", "INFO")
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger("main")

    if args.which != "crawler":
        argp.print_help()
        sys.exit(1)

    # ================================================================
    # Mode: scrape (no output driver — just JSON to stdout/file)
    # ================================================================
    if args.mode == "scrape":
        import json as _json
        import os as _os

        # Resolve output path: --output-file takes precedence, then -o, then legacy
        output_path = (getattr(args, "output_file_legacy", None)  # legacy --output-file
                       or args.output_file                         # -o / --output
                       or args.output)                             # parent -o

        indent = 2 if args.pretty else None

        # --- type: user-posts ---
        if args.type == "user-posts":
            if not args.unique_id:
                log.error("--unique-id is required for --type user-posts (e.g. --unique-id @persib)")
                sys.exit(1)

            from controllers.tiktok.user_posts import TikTokUserPosts

            kwargs = dict(
                unique_id=args.unique_id,
                count=args.count,
                max_pages=args.max_pages,
                hd=args.hd,
                cookies=args.cookies,
                output_file=output_path,
            )
            ctl = TikTokUserPosts(**kwargs)
            job = {"unique_id": args.unique_id}
            posts = asyncio.run(ctl.scrape_to_json(job))

        # --- type: user-story ---
        elif args.type == "user-story":
            if not args.unique_id:
                log.error("--unique-id is required for --type user-story (e.g. --unique-id @zavann_d)")
                sys.exit(1)

            from controllers.tiktok.user_story import TikTokUserStory

            kwargs = dict(
                unique_id=args.unique_id,
                count=args.count,
                max_pages=args.max_pages,
                hd=args.hd,
                cookies=args.cookies,
                output_file=output_path,
            )
            ctl = TikTokUserStory(**kwargs)
            job = {"unique_id": args.unique_id}
            posts = asyncio.run(ctl.scrape_to_json(job))

        # --- type: search (default) ---
        else:
            from controllers.tiktok.search_post import TikTokSearchPost

            kwargs = dict(
                keyword=args.keyword,
                count=args.count,
                max_pages=args.max_pages,
                hd=args.hd,
                cookies=args.cookies,
                output_file=output_path,
            )
            ctl = TikTokSearchPost(**kwargs)
            job = {"keyword": args.keyword or ""}
            posts = asyncio.run(ctl.scrape_to_json(job))

        if output_path:
            _os.makedirs(_os.path.dirname(output_path) or ".", exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                _json.dump(posts, f, ensure_ascii=False, indent=indent, default=str)
            log.info("Saved %d posts -> %s", len(posts), output_path)
            print(f"Saved {len(posts)} posts to {output_path}")
        else:
            text = _json.dumps(posts, ensure_ascii=False, indent=indent, default=str)
            try:
                print(text)
            except UnicodeEncodeError:
                print(_json.dumps(posts, ensure_ascii=True, indent=indent, default=str))

        log.info("Scraped %d posts (type=%s)", len(posts), args.type)

    # ================================================================
    # Mode: full (crawl + output driver: Kafka / ES / file / std)
    # ================================================================
    elif args.mode == "full":
        if not args.destination:
            log.error("--destination / -d is required for full mode")
            sys.exit(1)

        # Merge: crawler subparser -o takes precedence over parent -o
        if args.output_file and not args.output:
            args.output = args.output_file

        kwargs = vars(args)

        # --- type: user-posts ---
        if args.type == "user-posts":
            if not args.unique_id:
                log.error("--unique-id is required for --type user-posts")
                sys.exit(1)
            from controllers.tiktok.user_posts import TikTokUserPosts
            ctl = TikTokUserPosts(**kwargs)
            job = {"unique_id": args.unique_id}

        # --- type: user-story ---
        elif args.type == "user-story":
            if not args.unique_id:
                log.error("--unique-id is required for --type user-story")
                sys.exit(1)
            from controllers.tiktok.user_story import TikTokUserStory
            ctl = TikTokUserStory(**kwargs)
            job = {"unique_id": args.unique_id}

        # --- type: search (default) ---
        else:
            from controllers.tiktok.search_post import TikTokSearchPost
            ctl = TikTokSearchPost(**kwargs)
            job = {"keyword": args.keyword or ""}

        asyncio.run(ctl.main())
