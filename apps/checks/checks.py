#!/usr/bin/env python3
"""
"Checks": Post-migration validation script.

Reads a CSV of migration requests (locator and status), fetches mappings for
finished migrations (with pagination), loads expected source accounts from disk, 
and confirms that all expected source accounts are present in the migrated results.

Usage:
    python checks.py requests.csv --tenant-locator TENANT --auth-token YOUR_TOKEN \
        --source-data PATH_TO_SOURCE_DATA [--base-url URL] [--page-size N] [-o output.txt]
"""
import os
import csv
import argparse
import sys
import requests


def load_requests(csv_path):
    """
    Load migration locators and statuses from a CSV file.
    Expects each row to have at least three columns: [*, locator, status].
    Returns a list of (locator, status) tuples.
    """
    requests_list = []
    with open(csv_path, newline='') as fh:
        reader = csv.reader(fh)
        for row in reader:
            if len(row) < 3:
                continue
            locator = row[1]
            status = row[2]
            requests_list.append((locator, status))
    return requests_list


def fetch_mappings(tenant, migration_locator, base_url, headers, page_size):
    """
    Fetch mapping results for a given migration locator, handling pagination.
    Expects JSON response with:
      - items: list of mapping objects containing 'originalAccountId'
      - listCompleted: boolean flag when all items have been returned
    Sends 'offset' and 'count' query params; defaults to page_size.
    Returns a list of mapping objects.
    """
    url = f"{base_url}/migration/{tenant}/migrations/{migration_locator}/mappings/list"
    all_items = []
    offset = 0
    while True:
        params = {'offset': offset, 'count': page_size}
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        items = data.get('items', [])
        all_items.extend(items)
        if data.get('listCompleted', False):
            break
        offset += page_size
    return all_items


def load_source_accounts(migration_locator, source_data_path):
    """
    Load expected source account IDs from the 'accounts' subdirectory of source_data_path.
    Files are named 'accounts/account-{id}.json'. Returns a list of id strings.
    """
    account_dir = os.path.join(source_data_path, 'accounts')
    if not os.path.isdir(account_dir):
        print(f"Error: accounts directory not found at {account_dir}")
        sys.exit(1)
    ids = []
    for filename in os.listdir(account_dir):
        if filename.startswith('account-') and filename.endswith('.json'):
            id_part = filename[len('account-'):-len('.json')]
            ids.append(id_part)
    return ids


def main():
    parser = argparse.ArgumentParser(description="Post-migration checks")
    parser.add_argument('csv_file', help='Path to the migration requests CSV')
    parser.add_argument('--tenant-locator', required=True, help='Tenant locator for API endpoints')
    parser.add_argument('--auth-token', required=True, help='API authentication token')
    parser.add_argument('--source-data', required=True,
                        help='Path to the original source data directory (must contain accounts/ subdir)')
    parser.add_argument('--base-url', default='https://api.example.com',
                        help='Base URL for the platform API')
    parser.add_argument('--page-size', type=int, default=100,
                        help='Number of mappings to fetch per page')
    parser.add_argument('-o', '--output', help='Optional path to write error results')
    args = parser.parse_args()

    headers = {
        'Authorization': f"Bearer {args.auth_token}",
        'Accept': 'application/json',
    }

    # Load requests from CSV
    requests_list = load_requests(args.csv_file)
    if not requests_list:
        print("No valid migration requests (locator + status) found in CSV.")
        sys.exit(1)

    # Validate source data path
    if not os.path.isdir(args.source_data):
        print(f"Error: source data directory not found at {args.source_data}")
        sys.exit(1)

    errors = []
    for locator, status in requests_list:
        if status.lower() != 'finished':
            errors.append(f"[Status] Locator {locator} has status '{status}', skipping mappings check.")
            continue

        # Fetch mappings for finished migrations
        try:
            mappings = fetch_mappings(
                args.tenant_locator,
                locator,
                args.base_url,
                headers,
                args.page_size
            )
        except Exception as e:
            errors.append(f"[Mappings] Failed to fetch mappings for {locator}: {e}")
            continue

        migrated_ids = {m.get('originalAccountId') for m in mappings}
        expected_ids = load_source_accounts(locator, args.source_data)
        missing = set(expected_ids) - migrated_ids
        if missing:
            errors.append(f"[Mappings] Locator {locator} missing accounts: {missing}")

    # Summary
    if errors:
        print("\nErrors detected during post-migration checks:")
        for e in errors:
            print(f" - {e}")
    else:
        print("All migrations passed checks successfully.")

    # Optionally write to file
    if args.output:
        with open(args.output, 'w') as outfh:
            for e in errors:
                outfh.write(e + "\n")
        print(f"Results written to {args.output}")


if __name__ == '__main__':
    main()
