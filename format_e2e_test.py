import json
from pathlib import Path


def format_jsonl_file(filename: Path) -> None:
    lines = filename.read_text().splitlines()

    with filename.open('w') as f:
        # TODO validate schema
        for line in lines:
            if not line.strip():
                continue

            obj = json.loads(line)
            formatted_line = json.dumps(obj, sort_keys=True, separators=(',', ':'))
            f.write(formatted_line + '\n')


def format_all_jsonl() -> None:
    for file_path in (Path(__file__).parent / 'test' / 'e2e').rglob('*.jsonl'):
        format_jsonl_file(file_path)


if __name__ == '__main__':
    format_all_jsonl()
