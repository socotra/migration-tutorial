import json
import argparse
import uuid
from pathlib import Path

def transform_policy(policy_dir: Path) -> dict:
    """
    Read a policy directory and convert it into the target migration format,
    including nested terms and transactions.
    """
    # Load top-level policy.json
    policy_json_path = policy_dir / 'policy.json'
    if not policy_json_path.exists():
        raise FileNotFoundError(f"Missing policy.json in {policy_dir}")
    with policy_json_path.open('r', encoding='utf-8') as f:
        raw = json.load(f)

    # Policy identifiers and metadata
    policy_id = f"policy_{raw.get('id')}"
    product_name = raw.get('productName')
    timezone = raw.get('timezone')
    currency = raw.get('currency')

    # Generate a single root element UUID for this policy
    root_uuid = str(uuid.uuid4())

    terms_base = policy_dir / 'terms'
    if not terms_base.is_dir():
        raise FileNotFoundError(f"Missing terms directory in {policy_dir}")

    terms = []
    created_dates = []

    # Iterate through each term subdirectory in order
    for term_dir in sorted(terms_base.iterdir(), key=lambda p: int(p.name)):
        if not term_dir.is_dir():
            continue

        # Load term.json for start/end
        term_json_path = term_dir / 'term.json'
        if not term_json_path.exists():
            raise FileNotFoundError(f"Missing term.json in {term_dir}")
        with term_json_path.open('r', encoding='utf-8') as f:
            term_raw = json.load(f)
        term_start = term_raw.get('start')
        term_end = term_raw.get('end')

        # Transactions directory
        tx_base = term_dir / 'transactions'
        if not tx_base.is_dir():
            raise FileNotFoundError(f"Missing transactions directory in {term_dir}")

        transactions = []
        # Read each transaction file in order
        for tx_file in sorted(tx_base.glob('tx_*.json'), key=lambda p: int(p.stem.split('_')[1])):
            with tx_file.open('r', encoding='utf-8') as f:
                tx_raw = json.load(f)

            tx_created = tx_raw.get('created')
            tx_issued = tx_raw.get('issued')
            tx_start = tx_raw.get('start')
            tx_type = tx_raw.get('type')

            # Collect for policy-level createdAt
            if tx_created:
                created_dates.append(tx_created)

            # Determine segment type: cancellations become gaps, others are coverage
            seg_type = 'gap' if tx_type == 'cancellation' else 'coverage'

            # Build segment (reuse root_uuid across transactions)
            segment = {
                'rootElement': {
                    'id': root_uuid,
                    'elementType': product_name
                },
                'segmentType': seg_type,
                'startTime': tx_start
            }

            transactions.append({
                'transactionType': tx_type,
                'createdAt': tx_created,
                'issuedTime': tx_issued,
                'segment': segment
            })

        terms.append({
            'startTime': term_start,
            'endTime': term_end,
            'transactions': transactions
        })

    # Determine policy-level createdAt as earliest transaction created date
    policy_created_at = min(created_dates) if created_dates else None

    return {
        'id': policy_id,
        'productName': product_name,
        'timezone': timezone,
        'currency': currency,
        'billingLevel': 'inherit',
        'durationBasis': 'months',
        'createdAt': policy_created_at,
        'terms': terms
    }


def transform_account(account_path: Path, policies_dir: Path, default_created_by: str) -> dict:
    """
    Read an account JSON and convert into migration format,
    embedding policies.
    """
    with account_path.open('r', encoding='utf-8') as f:
        raw = json.load(f)

    # Extract account ID
    stem = account_path.stem  # account-1234
    try:
        _, account_id = stem.split('-', 1)
    except ValueError:
        raise ValueError(f"Invalid account filename: {account_path.name}")

    account_data = {
        'id': account_id,
        'accountType': raw.get('type'),
        'data': raw.get('fields', {}),
        'billingLevel': raw.get('billing'),
        'createdAt': raw.get('created')
    }

    # Transform policies
    policies = []
    for pref in raw.get('policies', []):
        # pref like 'policy-1000'
        parts = pref.split('-', 1)
        if len(parts) != 2:
            print(f"Warning: unrecognized policy ref '{pref}' in {account_path}")
            continue
        policy_subdir = policies_dir / f"policy-{parts[1]}"
        if not policy_subdir.is_dir():
            print(f"Warning: missing policy directory {policy_subdir}")
            continue
        policies.append(transform_policy(policy_subdir))

    return {
        'accountData': account_data,
        'defaultCreatedBy': default_created_by,
        'policies': policies
    }


def main(input_dir: str, output_file: str, default_created_by: str) -> None:
    """
    Traverse accounts and policies in input_dir to produce a single migration JSON.
    """
    base = Path(input_dir)
    accounts_dir = base / 'accounts'
    policies_dir = base / 'policies'

    if not accounts_dir.is_dir():
        print(f"Error: accounts directory '{accounts_dir}' not found.")
        return
    if not policies_dir.is_dir():
        print(f"Error: policies directory '{policies_dir}' not found.")
        return

    output = []
    for acct_file in sorted(accounts_dir.glob('account-*.json')):
        try:
            rec = transform_account(acct_file, policies_dir, default_created_by)
            output.append(rec)
        except Exception as e:
            print(f"Warning: skipping account {acct_file.name}: {e}")

    # Write output
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=4)
    print(f"Wrote {len(output)} account records to {output_file}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Transform insurance data into migration format'
    )
    parser.add_argument('input_dir', help='Root directory containing accounts/ and policies/')
    parser.add_argument('output_file', help='Path to write migration JSON')
    parser.add_argument('--defaultCreatedBy', required=True,
                        help='UUID for defaultCreatedBy in migration request')
    args = parser.parse_args()

    main(args.input_dir, args.output_file, args.defaultCreatedBy)
