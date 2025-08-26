import argparse

def main():
    parser = argparse.ArgumentParser(prog="argo-migration", description="Argo Migration CLI")
    parser.add_argument("--version", action="version", version="argo-migration 0.1.0")

    subparsers = parser.add_subparsers(dest="command")

    # Example command: hello
    hello_parser = subparsers.add_parser("hello", help="Say hello")
    hello_parser.add_argument("name", help="Name to greet")

    args = parser.parse_args()

    if args.command == "hello":
        print(f"Hello, {args.name} 👋")
    else:
        parser.print_help()